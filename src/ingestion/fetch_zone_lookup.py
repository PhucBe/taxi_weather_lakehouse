from __future__ import annotations

from pathlib import Path
import csv

import requests

from src.common.utils import sanitize_identifier


EXPECTED_ZONE_HEADERS = [
    "locationid",
    "borough",
    "zone",
    "service_zone",
]


def build_output_path(config: dict) -> Path:
    """
    Tạo đường dẫn local để lưu zone lookup CSV.
    """
    zone_dir = Path(config["paths"]["zone_dir"])
    return zone_dir / "taxi_zone_lookup.csv"


def _read_csv_headers(csv_path: str | Path, encoding: str = "utf-8") -> list[str]:
    """
    Đọc header của file CSV và chuẩn hóa về dạng an toàn để validate.
    """
    csv_path = Path(csv_path)

    with csv_path.open("r", encoding=encoding, newline="") as f:
        reader = csv.reader(f)
        headers = next(reader)

    return [sanitize_identifier(col) for col in headers]


def _validate_zone_lookup_csv(csv_path: str | Path, encoding: str = "utf-8") -> None:
    """
    Validate file zone lookup CSV có đúng 4 cột source đã chốt hay không.
    """
    headers = _read_csv_headers(csv_path=csv_path, encoding=encoding)

    if headers != EXPECTED_ZONE_HEADERS:
        raise ValueError(
            "Invalid zone lookup header. "
            f"Expected {EXPECTED_ZONE_HEADERS}, got {headers}"
        )


def download_zone_lookup_csv(
    url: str,
    output_path: str | Path,
    logger,
    overwrite: bool = False,
    timeout: int = 120,
) -> Path:
    """
    Tải zone lookup CSV từ web về local.

    - Nếu file đã tồn tại và overwrite=False thì bỏ qua
    - Sau khi tải xong sẽ validate header
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_path.exists() and not overwrite:
        logger.info("Zone lookup CSV already exists, skip download: %s", output_path)
        return output_path

    logger.info("Downloading zone lookup CSV | url=%s | output=%s", url, output_path)

    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(f"Failed to download zone lookup CSV from {url}") from exc

    with output_path.open("wb") as f:
        f.write(response.content)

    logger.info("Downloaded zone lookup CSV successfully | path=%s", output_path)
    return output_path


def fetch_zone_lookup(
    config: dict,
    logger,
    overwrite: bool = False,
) -> Path:
    """
    Fetch zone lookup CSV từ source URL về local.

    Trả về:
        Path tới file CSV local, ví dụ:
        data/raw/zone_lookup/taxi_zone_lookup.csv
    """
    url = config["source_urls"]["zone_lookup_url"]
    output_path = build_output_path(config=config)

    downloaded_path = download_zone_lookup_csv(
        url=url,
        output_path=output_path,
        logger=logger,
        overwrite=overwrite,
    )

    encoding = config["ingestion"]["encoding"]
    _validate_zone_lookup_csv(
        csv_path=downloaded_path,
        encoding=encoding,
    )

    logger.info("Zone lookup CSV validated successfully | path=%s", downloaded_path)
    return downloaded_path