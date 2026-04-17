from __future__ import annotations

from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError, ClientError


def _normalize_s3_key(s3_key: str) -> str:
    """
    Chuẩn hóa S3 key:
    - bỏ khoảng trắng đầu/cuối
    - bỏ dấu / thừa ở đầu
    """
    if not isinstance(s3_key, str):
        raise ValueError("s3_key must be a string")

    cleaned = s3_key.strip().lstrip("/")
    if not cleaned:
        raise ValueError("s3_key must not be empty")

    return cleaned


def upload_file_to_s3(
    local_path: str | Path,
    bucket_name: str,
    s3_key: str,
    region: str,
    logger,
) -> str:
    """
    Upload 1 file local lên S3 và trả về S3 URI.

    Ví dụ:
    - local_path = data/raw/taxi/year=2023/month=01/yellow_tripdata_2023-01.parquet
    - bucket_name = my-bucket
    - s3_key = raw/taxi_weather/taxi/load_date=2026-04-17/yellow_tripdata_2023-01.parquet

    Kết quả:
    - s3://my-bucket/raw/taxi_weather/taxi/load_date=2026-04-17/yellow_tripdata_2023-01.parquet
    """
    local_path = Path(local_path)
    s3_key = _normalize_s3_key(s3_key)

    if not local_path.exists():
        raise FileNotFoundError(f"Local file not found: {local_path}")

    if not local_path.is_file():
        raise ValueError(f"Local path is not a file: {local_path}")

    logger.info(
        "Uploading local file to S3 | local_path=%s | bucket=%s | s3_key=%s",
        local_path,
        bucket_name,
        s3_key,
    )

    try:
        s3_client = boto3.client("s3", region_name=region)
        s3_client.upload_file(str(local_path), bucket_name, s3_key)
    except (BotoCoreError, ClientError) as exc:
        raise RuntimeError(
            f"Failed to upload file to s3://{bucket_name}/{s3_key}"
        ) from exc

    s3_uri = f"s3://{bucket_name}/{s3_key}"
    logger.info("Upload completed successfully | s3_uri=%s", s3_uri)

    return s3_uri