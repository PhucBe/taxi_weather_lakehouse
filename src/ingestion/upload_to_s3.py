from __future__ import annotations
from pathlib import Path
import boto3
from botocore.exceptions import BotoCoreError, ClientError


# Chuẩn hóa S3 key
def _normalize_s3_key(s3_key: str) -> str:
    if not isinstance(s3_key, str):
        raise ValueError("s3_key must be a string")

    cleaned = s3_key.strip().lstrip("/")

    if not cleaned:
        raise ValueError("s3_key must not be empty")

    return cleaned


# Upload 1 file local lên S3 và trả về S3 URI.
def upload_file_to_s3(
    local_path: str | Path,
    bucket_name: str,
    s3_key: str,
    region: str,
    logger,
) -> str:
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