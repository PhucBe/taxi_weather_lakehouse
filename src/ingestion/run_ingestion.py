from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

from src.common.config import load_app_config
from src.common.logger import get_logger
from src.common.utils import build_s3_key, ensure_directory, today_str

from src.ingestion.fetch_tlc_parquet import fetch_tlc_parquets
from src.ingestion.fetch_weather_api import fetch_weather_raw
from src.ingestion.fetch_zone_lookup import fetch_zone_lookup
from src.ingestion.prepare_taxi_flat_file import prepare_taxi_flat_files
from src.ingestion.prepare_weather_flat_file import prepare_weather_flat_file
from src.ingestion.upload_to_s3 import upload_file_to_s3
from src.ingestion.load_raw_redshift import (
    reset_truncate_state,
    load_taxi_csv_to_redshift_raw,
    load_weather_csv_to_redshift_raw,
    load_zone_csv_to_redshift_raw,
)


YEAR_MONTH_IN_FILENAME = re.compile(r"(\d{4}-\d{2})")


def resolve_load_date() -> str:
    """Ưu tiên LOAD_DATE từ env, nếu không có thì dùng ngày hôm nay."""
    return os.getenv("LOAD_DATE", today_str())


def _require_existing_file(path: str | Path, label: str) -> Path:
    """Đảm bảo file local tồn tại trước khi upload/load."""
    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(f"{label} not found: {file_path}")
    return file_path


def _extract_year_month_from_filename(filename: str) -> str:
    """Lấy YYYY-MM từ tên file, ví dụ yellow_tripdata_2023-01.parquet -> 2023-01."""
    match = YEAR_MONTH_IN_FILENAME.search(filename)
    if not match:
        raise ValueError(f"Cannot extract year_month from filename: {filename}")
    return match.group(1)


def _normalize_taxi_files(taxi_files: list[Any]) -> list[dict[str, str]]:
    """
    Chuẩn hóa đầu ra của fetch_tlc_parquets() về dạng:
    [
        {
            "year_month": "2023-01",
            "local_path": "/abs/or/relative/path/file.parquet",
            "filename": "yellow_tripdata_2023-01.parquet",
        },
        ...
    ]

    Hỗ trợ 2 kiểu đầu vào:
    1) list[dict]
    2) list[str | Path]
    """
    normalized: list[dict[str, str]] = []

    for item in taxi_files:
        if isinstance(item, dict):
            if "local_path" not in item:
                raise ValueError("Each taxi file item must contain 'local_path'")

            local_path = _require_existing_file(item["local_path"], "Taxi parquet file")
            filename = str(item.get("filename") or local_path.name)
            year_month = str(item.get("year_month") or _extract_year_month_from_filename(filename))
        else:
            local_path = _require_existing_file(item, "Taxi parquet file")
            filename = local_path.name
            year_month = _extract_year_month_from_filename(filename)

        normalized.append(
            {
                "year_month": year_month,
                "local_path": str(local_path),
                "filename": filename,
            }
        )

    if not normalized:
        raise ValueError("No taxi parquet files returned from fetch_tlc_parquets()")

    return normalized


def main() -> None:
    config = load_app_config()
    load_date = resolve_load_date()
    year_months = config["prototype"]["year_months"]

    reset_truncate_state()

    logger = get_logger(
        name="taxi_weather_ingestion",
        log_dir=config["paths"]["log_dir"],
    )

    logger.info("=" * 50)
    logger.info("=== START INGESTION ===")
    logger.info("=" * 50)

    logger.info(
        "Starting raw ingestion | project=%s | env=%s | load_date=%s | year_months=%s",
        config["project"]["name"],
        config["project"]["env"],
        load_date,
        ",".join(year_months),
    )

    # 1) Ensure local directories exist
    ensure_directory(config["paths"]["local_data_dir"])
    ensure_directory(config["paths"]["taxi_dir"])
    ensure_directory(config["paths"]["weather_dir"])
    ensure_directory(config["paths"]["zone_dir"])

    # 2) Fetch taxi parquet files from web -> local
    logger.info("Fetching NYC TLC taxi parquet files...")
    taxi_parquet_files = fetch_tlc_parquets(
        config=config,
        year_months=year_months,
        logger=logger,
    )
    taxi_parquet_files = _normalize_taxi_files(taxi_parquet_files)
    logger.info("Fetched %s taxi parquet file(s).", len(taxi_parquet_files))

    # 3) Prepare taxi flat CSV files from local parquet
    logger.info("Preparing taxi flat CSV files...")
    taxi_csv_files = prepare_taxi_flat_files(
        config=config,
        taxi_files=taxi_parquet_files,
        logger=logger,
    )
    logger.info("Prepared %s taxi flat CSV file(s).", len(taxi_csv_files))

    # 4) Fetch weather raw from API -> local json / in-memory payload
    logger.info("Fetching weather raw data from API...")
    weather_raw = fetch_weather_raw(
        config=config,
        year_months=year_months,
        logger=logger,
    )

    # 5) Flatten weather raw -> local CSV
    logger.info("Preparing flattened weather CSV...")
    weather_csv_path = _require_existing_file(
        prepare_weather_flat_file(
            config=config,
            weather_raw=weather_raw,
            logger=logger,
        ),
        "Flattened weather CSV",
    )

    # 6) Fetch zone lookup CSV -> local
    logger.info("Fetching taxi zone lookup CSV...")
    zone_csv_path = _require_existing_file(
        fetch_zone_lookup(
            config=config,
            logger=logger,
        ),
        "Zone lookup CSV",
    )

    logger.info("=" * 50)
    logger.info("=== START RAW VALIDATION ===")
    logger.info("=" * 50)

    bucket_name = config["aws"]["bucket_name"]
    region = config["aws"]["region"]

    # 7) Upload taxi CSV files -> S3
    logger.info("Uploading taxi flat CSV files to S3...")
    taxi_s3_objects: list[dict[str, str]] = []

    for item in taxi_csv_files:
        s3_key = build_s3_key(
            prefix=config["aws"]["s3_prefix_taxi"],
            filename=item["filename"],
            load_date=load_date,
        )
        s3_uri = upload_file_to_s3(
            local_path=item["local_path"],
            bucket_name=bucket_name,
            s3_key=s3_key,
            region=region,
            logger=logger,
        )

        taxi_s3_objects.append(
            {
                "year_month": item["year_month"],
                "local_path": item["local_path"],
                "filename": item["filename"],
                "s3_key": s3_key,
                "s3_uri": s3_uri,
            }
        )

    # 8) Upload weather CSV -> S3
    logger.info("Uploading weather CSV to S3...")
    weather_s3_key = build_s3_key(
        prefix=config["aws"]["s3_prefix_weather"],
        filename=weather_csv_path.name,
        load_date=load_date,
    )
    weather_s3_uri = upload_file_to_s3(
        local_path=weather_csv_path,
        bucket_name=bucket_name,
        s3_key=weather_s3_key,
        region=region,
        logger=logger,
    )

    # 9) Upload zone CSV -> S3
    logger.info("Uploading zone lookup CSV to S3...")
    zone_s3_key = build_s3_key(
        prefix=config["aws"]["s3_prefix_zone"],
        filename=zone_csv_path.name,
        load_date=load_date,
    )
    zone_s3_uri = upload_file_to_s3(
        local_path=zone_csv_path,
        bucket_name=bucket_name,
        s3_key=zone_s3_key,
        region=region,
        logger=logger,
    )

    # 10) Load taxi CSV -> Redshift raw
    logger.info("Loading taxi CSV files from S3 into Redshift raw table...")
    for item in taxi_s3_objects:
        load_taxi_csv_to_redshift_raw(
            config=config,
            local_csv_path=item["local_path"],
            raw_table=config["redshift"]["table_raw_taxi"],
            bucket_name=bucket_name,
            s3_key=item["s3_key"],
            logger=logger,
        )

    # 11) Load weather CSV -> Redshift raw
    logger.info("Loading weather CSV from S3 into Redshift raw table...")
    load_weather_csv_to_redshift_raw(
        config=config,
        local_csv_path=weather_csv_path,
        raw_table=config["redshift"]["table_raw_weather"],
        bucket_name=bucket_name,
        s3_key=weather_s3_key,
        logger=logger,
    )

    # 12) Load zone CSV -> Redshift raw
    logger.info("Loading zone CSV from S3 into Redshift raw table...")
    load_zone_csv_to_redshift_raw(
        config=config,
        local_csv_path=zone_csv_path,
        raw_table=config["redshift"]["table_raw_zone"],
        bucket_name=bucket_name,
        s3_key=zone_s3_key,
        logger=logger,
    )

    logger.info(
        "Raw ingestion finished successfully | taxi_csv_files=%s | weather_s3_uri=%s | zone_s3_uri=%s",
        len(taxi_s3_objects),
        weather_s3_uri,
        zone_s3_uri,
    )

    logger.info("=" * 50)
    logger.info("=== INGESTION FINISHED SUCCESSFULLY ===")
    logger.info("=" * 50)


if __name__ == "__main__":
    main()