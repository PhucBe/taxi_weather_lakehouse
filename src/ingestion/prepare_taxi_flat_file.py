from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


# Thứ tự cột đầu ra phải khớp với schema taxi raw chuẩn hóa
OUTPUT_COLUMNS = [
    "vendorid",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "ratecodeid",
    "store_and_fwd_flag",
    "pulocationid",
    "dolocationid",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
]

# Mapping từ tên cột source TLC parquet -> tên cột output chuẩn hóa
SOURCE_TO_OUTPUT_COLUMN_MAP = {
    "VendorID": "vendorid",
    "tpep_pickup_datetime": "tpep_pickup_datetime",
    "tpep_dropoff_datetime": "tpep_dropoff_datetime",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "RatecodeID": "ratecodeid",
    "store_and_fwd_flag": "store_and_fwd_flag",
    "PULocationID": "pulocationid",
    "DOLocationID": "dolocationid",
    "payment_type": "payment_type",
    "fare_amount": "fare_amount",
    "extra": "extra",
    "mta_tax": "mta_tax",
    "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount",
    "improvement_surcharge": "improvement_surcharge",
    "total_amount": "total_amount",
    "congestion_surcharge": "congestion_surcharge",
    "Airport_fee": "airport_fee",
}

# Cột bắt buộc tối thiểu để coi file taxi parquet là hợp lệ
REQUIRED_SOURCE_COLUMNS = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
]


def _require_existing_file(path: str | Path, label: str) -> Path:
    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(f"{label} not found: {file_path}")
    return file_path


def _build_output_path(config: dict[str, Any], year_month: str) -> Path:
    """
    Ví dụ:
    data/raw/taxi/flat/year=2023/month=01/yellow_tripdata_2023-01.csv
    """
    year, month = year_month.split("-")
    taxi_dir = Path(config["paths"]["taxi_dir"])

    return (
        taxi_dir
        / "flat"
        / f"year={year}"
        / f"month={month}"
        / f"yellow_tripdata_{year_month}.csv"
    )


def _load_parquet_to_dataframe(parquet_path: str | Path) -> pd.DataFrame:
    parquet_path = Path(parquet_path)
    try:
        return pd.read_parquet(parquet_path)
    except Exception as exc:
        raise RuntimeError(f"Failed to read taxi parquet: {parquet_path}") from exc


def _validate_required_source_columns(df: pd.DataFrame, parquet_path: str | Path) -> None:
    missing = [col for col in REQUIRED_SOURCE_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(
            f"Taxi parquet missing required columns: {missing} | file={parquet_path}"
        )


def _normalize_taxi_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    - chỉ giữ bộ cột chuẩn
    - rename về output schema cố định
    - thêm cột optional bị thiếu dưới dạng null
    - cast về kiểu dễ COPY CSV vào Redshift
    """
    _validate_required_source_columns(df, "<in-memory-dataframe>")

    # Chỉ lấy những cột source có trong parquet
    available_source_cols = [col for col in SOURCE_TO_OUTPUT_COLUMN_MAP if col in df.columns]
    df = df[available_source_cols].copy()

    # Rename sang output schema chuẩn hóa
    df = df.rename(columns=SOURCE_TO_OUTPUT_COLUMN_MAP)

    # Bổ sung cột optional còn thiếu
    for col in OUTPUT_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA

    # Reorder đúng thứ tự
    df = df[OUTPUT_COLUMNS]

    # Chuẩn hóa datetime
    for col in ["tpep_pickup_datetime", "tpep_dropoff_datetime"]:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")

    # Chuẩn hóa numeric -> float/int-friendly cho CSV
    numeric_cols = [
        "vendorid",
        "passenger_count",
        "trip_distance",
        "ratecodeid",
        "pulocationid",
        "dolocationid",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "airport_fee",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Chuẩn hóa string
    df["store_and_fwd_flag"] = df["store_and_fwd_flag"].astype("string")

    return df


def prepare_taxi_flat_file(
    config: dict[str, Any],
    taxi_file: dict[str, Any],
    logger,
    overwrite: bool = False,
) -> Path:
    """
    Chuyển 1 taxi parquet local -> 1 taxi flat CSV local.

    taxi_file mong đợi:
    {
        "year_month": "2023-01",
        "local_path": ".../yellow_tripdata_2023-01.parquet",
        "filename": "yellow_tripdata_2023-01.parquet",
        ...
    }
    """
    if not isinstance(taxi_file, dict):
        raise ValueError("taxi_file must be a dictionary")

    year_month = taxi_file.get("year_month")
    local_parquet_path = taxi_file.get("local_path")

    if not year_month:
        raise ValueError("taxi_file must contain 'year_month'")
    if not local_parquet_path:
        raise ValueError("taxi_file must contain 'local_path'")

    parquet_path = _require_existing_file(local_parquet_path, "Taxi parquet file")
    output_path = _build_output_path(config=config, year_month=year_month)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_path.exists() and not overwrite:
        logger.info("Taxi flat CSV already exists, skip rebuild: %s", output_path)
        return output_path

    logger.info(
        "Preparing taxi flat CSV | year_month=%s | input=%s | output=%s",
        year_month,
        parquet_path,
        output_path,
    )

    df = _load_parquet_to_dataframe(parquet_path)
    _validate_required_source_columns(df, parquet_path)
    df = _normalize_taxi_dataframe(df)

    df.to_csv(output_path, index=False, encoding="utf-8")

    logger.info(
        "Taxi flat CSV created successfully | path=%s | row_count=%s",
        output_path,
        len(df),
    )

    return output_path


def prepare_taxi_flat_files(
    config: dict[str, Any],
    taxi_files: list[dict[str, Any]],
    logger,
    overwrite: bool = False,
) -> list[dict[str, str]]:
    """
    Chuyển nhiều taxi parquet local -> nhiều taxi flat CSV local.

    Trả về:
    [
        {
            "dataset": "taxi",
            "year_month": "2023-01",
            "local_path": ".../yellow_tripdata_2023-01.csv",
            "filename": "yellow_tripdata_2023-01.csv",
            "file_format": "csv",
            "source_parquet_path": ".../yellow_tripdata_2023-01.parquet",
        },
        ...
    ]
    """
    if not isinstance(taxi_files, list) or not taxi_files:
        raise ValueError("taxi_files must be a non-empty list[dict]")

    results: list[dict[str, str]] = []

    for item in taxi_files:
        csv_path = prepare_taxi_flat_file(
            config=config,
            taxi_file=item,
            logger=logger,
            overwrite=overwrite,
        )

        results.append(
            {
                "dataset": "taxi",
                "year_month": str(item["year_month"]),
                "local_path": str(csv_path),
                "filename": csv_path.name,
                "file_format": "csv",
                "source_parquet_path": str(item["local_path"]),
            }
        )

    logger.info(
        "Finished preparing taxi flat CSV files | file_count=%s",
        len(results),
    )

    return results