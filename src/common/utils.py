from datetime import date
from pathlib import Path
import re


# Đảm bảo thư mục tồn tại.
def ensure_directory(path: str | Path) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)


# Trả về ngày hôm nay theo định dạng YYYY-MM-DD.
def today_str() -> str:
    return date.today().isoformat()


# Tạo S3 key từ prefix đã đầy đủ sẵn.
def build_s3_key(prefix: str, filename: str, load_date: str | None = None) -> str:
    """
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


# Chuẩn hóa tên cột/tên bảng để an toàn hơn khi dùng trong SQL. 
def sanitize_identifier(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9_]", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")

    if value and value[0].isdigit():
        value = f"col_{value}"

    return value