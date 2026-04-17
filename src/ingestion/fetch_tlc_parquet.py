from __future__ import annotations

from pathlib import Path
import re
from typing import Any

import requests


YEAR_MONTH_PATTERN = re.compile(r"^\d{4}-\d{2}$")


def _validate_year_month(year_month: str) -> tuple[str, str]:
    """
    Validate định dạng YYYY-MM và trả về (year, month).
    """
    if not isinstance(year_month, str) or not YEAR_MONTH_PATTERN.match(year_month):
        raise ValueError(f"Invalid year_month format: {year_month}. Expected YYYY-MM")

    year, month = year_month.split("-")
    return year, month


def build_tlc_parquet_url(config: dict[str, Any], year_month: str) -> str:
    """
    Tạo URL tải file TLC parquet theo template trong config.

    Ví dụ:
    https://.../yellow_tripdata_2023-01.parquet
    """
    _validate_year_month(year_month)

    url_template = config["source_urls"]["taxi_url_template"]
    return url_template.format(year_month=year_month)


def build_output_path(config: dict[str, Any], year_month: str) -> Path:
    """
    Tạo đường dẫn local để lưu taxi parquet theo partition:
    data/raw/taxi/year=2023/month=01/yellow_tripdata_2023-01.parquet
    """
    year, month = _validate_year_month(year_month)

    taxi_dir = Path(config["paths"]["taxi_dir"])
    filename = f"yellow_tripdata_{year_month}.parquet"

    return taxi_dir / f"year={year}" / f"month={month}" / filename


def download_file(
    url: str,
    output_path: str | Path,
    logger,
    overwrite: bool = False,
    chunk_size: int = 1024 * 1024,
    timeout: int = 120,
) -> Path:
    """
    Tải một file từ URL về local.

    - Tạo thư mục cha nếu chưa có
    - Nếu file đã tồn tại và overwrite=False thì bỏ qua
    - Tải theo stream để phù hợp file parquet lớn
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_path.exists() and not overwrite:
        logger.info("Taxi parquet already exists, skip download: %s", output_path)
        return output_path

    logger.info("Downloading taxi parquet | url=%s | output=%s", url, output_path)

    try:
        with requests.get(url, stream=True, timeout=timeout) as response:
            response.raise_for_status()

            with output_path.open("wb") as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
    except requests.RequestException as exc:
        raise RuntimeError(f"Failed to download taxi parquet from {url}") from exc

    file_size_mb = output_path.stat().st_size / (1024 * 1024)
    logger.info(
        "Downloaded taxi parquet successfully | path=%s | size_mb=%.2f",
        output_path,
        file_size_mb,
    )

    return output_path


def fetch_tlc_parquets(
    config: dict[str, Any],
    year_months: list[str],
    logger,
    overwrite: bool = False,
) -> list[dict[str, str]]:
    """
    Tải nhiều file taxi parquet theo danh sách year_months.

    Trả về list metadata có dạng:
    [
        {
            "dataset": "taxi",
            "year_month": "2023-01",
            "local_path": "/.../yellow_tripdata_2023-01.parquet",
            "filename": "yellow_tripdata_2023-01.parquet",
            "file_format": "parquet",
        },
        ...
    ]
    """
    if not isinstance(year_months, list) or not year_months:
        raise ValueError("year_months must be a non-empty list[str]")

    results: list[dict[str, str]] = []

    for year_month in year_months:
        _validate_year_month(year_month)

        url = build_tlc_parquet_url(config=config, year_month=year_month)
        output_path = build_output_path(config=config, year_month=year_month)

        downloaded_path = download_file(
            url=url,
            output_path=output_path,
            logger=logger,
            overwrite=overwrite,
        )

        results.append(
            {
                "dataset": "taxi",
                "year_month": year_month,
                "local_path": str(downloaded_path),
                "filename": downloaded_path.name,
                "file_format": "parquet",
            }
        )

    logger.info(
        "Finished fetching taxi parquet files | file_count=%s | year_months=%s",
        len(results),
        ",".join(year_months),
    )

    return results