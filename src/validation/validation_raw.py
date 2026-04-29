from __future__ import annotations
import csv
from pathlib import Path
import redshift_connector

from src.common.config import load_app_config
from src.common.logger import get_logger
from src.common.utils import sanitize_identifier


EXPECTED_TAXI_COLUMNS = [
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

EXPECTED_WEATHER_COLUMNS = [
    "date",
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "snowfall_sum",
]

EXPECTED_ZONE_COLUMNS_SANITIZED = [
    "locationid",
    "borough",
    "zone",
    "service_zone",
]


# Hàm kiểm tra file có tồn tại không.
def _require_existing_file(path: str | Path, label: str) -> Path:
    file_path = Path(path)

    if not file_path.exists():
        raise FileNotFoundError(f"{label} not found: {file_path}")
    
    if not file_path.is_file():
        raise ValueError(f"{label} is not a file: {file_path}")
    
    return file_path


# Hàm đọc dòng header của file CSV
def _read_csv_header(csv_path: str | Path, encoding: str = "utf-8") -> list[str]:
    csv_path = Path(csv_path)

    with csv_path.open("r", encoding=encoding, newline="") as f:
        reader = csv.reader(f)

        return next(reader)


# Hàm đếm số dòng dữ liệu trong CSV (Không tính header).
def _count_csv_rows(csv_path: str | Path, encoding: str = "utf-8") -> int:
    csv_path = Path(csv_path)

    with csv_path.open("r", encoding=encoding, newline="") as f:
        reader = csv.reader(f)
        next(reader, None)  # skip header

        return sum(1 for _ in reader)


# Hàm tạo danh sách đường dẫn taxi CSV cần validate
def _build_taxi_csv_paths(config: dict, year_months: list[str]) -> list[Path]:
    taxi_dir = Path(config["paths"]["taxi_dir"])
    paths: list[Path] = []

    for year_month in year_months:
        year, month = year_month.split("-")
        paths.append(
            taxi_dir / "flat" / f"year={year}" / f"month={month}" / f"yellow_tripdata_{year_month}.csv"
        )

    return paths


# Hàm tạo đường dẫn weather CSV cần Validate
def _build_weather_csv_path(config: dict, year_months: list[str]) -> Path:
    start_date = f"{year_months[0]}-01"
    last_year, last_month = year_months[-1].split("-")

    from calendar import monthrange

    last_day = monthrange(int(last_year), int(last_month))[1]
    end_date = f"{last_year}-{last_month}-{last_day:02d}"
    weather_dir = Path(config["paths"]["weather_dir"])

    return (
        weather_dir
        / "flat"
        / f"start_date={start_date}"
        / f"end_date={end_date}"
        / f"weather_daily_{start_date}_{end_date}.csv"
    )


# Hàm tạo đường dẫn zone lookup CSV.
def _build_zone_csv_path(config: dict) -> Path:
    zone_dir = Path(config["paths"]["zone_dir"])

    return zone_dir / "taxi_zone_lookup.csv"


# Hàm validate các file CSV raw ở local
def validate_local_raw_files(config: dict, logger) -> None:
    logger.info("=" * 50)
    logger.info("=== START LOCAL RAW VALIDATION ===")
    logger.info("=" * 50)

    year_months = config["prototype"]["year_months"]
    encoding = config["ingestion"]["encoding"]

    taxi_paths = _build_taxi_csv_paths(config, year_months)
    weather_path = _build_weather_csv_path(config, year_months)
    zone_path = _build_zone_csv_path(config)

    # Taxi
    for path in taxi_paths:
        path = _require_existing_file(path, "Taxi CSV")
        header = _read_csv_header(path, encoding=encoding)
        row_count = _count_csv_rows(path, encoding=encoding)

        if header != EXPECTED_TAXI_COLUMNS:
            raise ValueError(
                f"Invalid taxi CSV header for {path}. "
                f"Expected {EXPECTED_TAXI_COLUMNS}, got {header}"
            )

        if row_count <= 0:
            raise ValueError(f"Taxi CSV has no data rows: {path}")

        logger.info("Validated taxi CSV | path=%s | row_count=%s", path, row_count)

    # Weather
    weather_path = _require_existing_file(weather_path, "Weather CSV")
    weather_header = _read_csv_header(weather_path, encoding=encoding)
    weather_row_count = _count_csv_rows(weather_path, encoding=encoding)

    if weather_header != EXPECTED_WEATHER_COLUMNS:
        raise ValueError(
            f"Invalid weather CSV header. "
            f"Expected {EXPECTED_WEATHER_COLUMNS}, got {weather_header}"
        )

    if weather_row_count <= 0:
        raise ValueError(f"Weather CSV has no data rows: {weather_path}")

    logger.info("Validated weather CSV | path=%s | row_count=%s", weather_path, weather_row_count)

    # Zone
    zone_path = _require_existing_file(zone_path, "Zone CSV")
    zone_header = [sanitize_identifier(col) for col in _read_csv_header(zone_path, encoding=encoding)]
    zone_row_count = _count_csv_rows(zone_path, encoding=encoding)

    if zone_header != EXPECTED_ZONE_COLUMNS_SANITIZED:
        raise ValueError(
            f"Invalid zone CSV header. "
            f"Expected {EXPECTED_ZONE_COLUMNS_SANITIZED}, got {zone_header}"
        )

    if zone_row_count <= 0:
        raise ValueError(f"Zone CSV has no data rows: {zone_path}")

    logger.info("Validated zone CSV | path=%s | row_count=%s", zone_path, zone_row_count)


# Hàm tạo kết nối tới Redshift.
def _get_redshift_connection(config: dict):
    return redshift_connector.connect(
        host=config["redshift"]["host"],
        port=config["redshift"]["port"],
        database=config["redshift"]["database"],
        user=config["redshift"]["user"],
        password=config["redshift"]["password"],
    )


# Hàm chạy một câu SQL và lấy 1 dòng kết quả đầu tiên.
def _fetch_one(cursor, sql: str):
    cursor.execute(sql)

    return cursor.fetchone()


# Hàm validate các bảng raw trên Redshift.
def validate_redshift_raw_tables(config: dict, logger) -> None:
    logger.info("=" * 50)
    logger.info("=== START REDSHIFT RAW VALIDATION ===")
    logger.info("=" * 50)

    schema_name = config["redshift"]["schema_raw"]
    taxi_table = config["redshift"]["table_raw_taxi"]
    weather_table = config["redshift"]["table_raw_weather"]
    zone_table = config["redshift"]["table_raw_zone"]

    conn = _get_redshift_connection(config)

    try:
        with conn.cursor() as cur:
            taxi_count = _fetch_one(cur, f"select count(*) from {schema_name}.{taxi_table};")[0]
            weather_count = _fetch_one(cur, f"select count(*) from {schema_name}.{weather_table};")[0]
            zone_count = _fetch_one(cur, f"select count(*) from {schema_name}.{zone_table};")[0]

            if taxi_count <= 0:
                raise ValueError("Redshift raw taxi table is empty")
            
            if weather_count <= 0:
                raise ValueError("Redshift raw weather table is empty")
            
            if zone_count <= 0:
                raise ValueError("Redshift raw zone table is empty")

            logger.info("Validated raw row counts | taxi=%s | weather=%s | zone=%s", taxi_count, weather_count, zone_count)

            taxi_min_max = _fetch_one(
                cur,
                f"""
                select
                    min(cast(tpep_pickup_datetime as date)),
                    max(cast(tpep_pickup_datetime as date))
                from {schema_name}.{taxi_table};
                """
            )

            logger.info(
                "Taxi pickup date range | min=%s | max=%s",
                taxi_min_max[0],
                taxi_min_max[1],
            )

            weather_min_max = _fetch_one(
                cur,
                f"""
                select
                    min(date),
                    max(date)
                from {schema_name}.{weather_table};
                """
            )

            logger.info(
                "Weather date range | min=%s | max=%s",
                weather_min_max[0],
                weather_min_max[1],
            )

            zone_dupes = _fetch_one(
                cur,
                f"""
                select count(*)
                from (
                    select location_id
                    from {schema_name}.{zone_table}
                    group by location_id
                    having count(*) > 1
                ) t;
                """
            )[0]

            logger.info("Zone duplicate location_id groups | count=%s", zone_dupes)

    finally:
        conn.close()


def main() -> None:
    config = load_app_config()

    logger = get_logger(
        name="taxi_weather_raw_validation",
        log_dir=config["paths"]["log_dir"],
    )

    logger.info("=" * 50)
    logger.info("=== START RAW VALIDATION ===")
    logger.info("=" * 50)

    validate_local_raw_files(config, logger)
    validate_redshift_raw_tables(config, logger)

    logger.info("=" * 50)
    logger.info("=== RAW VALIDATION FINISHED SUCCESSFULLY ===")
    logger.info("=" * 50)


if __name__ == "__main__":
    main()