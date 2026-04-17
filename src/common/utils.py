from datetime import date
from pathlib import Path
import re


def ensure_directory(path: str | Path) -> None:
    """Đảm bảo thư mục tồn tại."""
    Path(path).mkdir(parents=True, exist_ok=True)


def today_str() -> str:
    """Trả về ngày hôm nay theo định dạng YYYY-MM-DD."""
    return date.today().isoformat()


def build_s3_key(prefix: str, filename: str, load_date: str | None = None) -> str:
    """
    Tạo S3 key từ prefix đã đầy đủ sẵn.

    Ví dụ:
    - prefix = raw/taxi_weather/taxi
    - filename = yellow_tripdata_2023-01.parquet
    - load_date = 2026-04-17

    Kết quả:
    raw/taxi_weather/taxi/load_date=2026-04-17/yellow_tripdata_2023-01.parquet
    """
    clean_prefix = prefix.strip("/")

    if load_date:
        return f"{clean_prefix}/load_date={load_date}/{filename}"

    return f"{clean_prefix}/{filename}"


def sanitize_identifier(value: str) -> str:
    """Chuẩn hóa tên cột/tên bảng để an toàn hơn khi dùng trong SQL."""
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9_]", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")

    if value and value[0].isdigit():
        value = f"col_{value}"

    return value