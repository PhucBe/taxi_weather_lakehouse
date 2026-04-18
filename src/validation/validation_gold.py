from __future__ import annotations

# =========================================================
# validation_gold.py
# ---------------------------------------------------------
# Mục tiêu:
# - Kiểm tra layer gold sau khi job Spark silver_to_gold chạy xong
# - Gold hiện gồm 4 bảng semantic:
#     + dim_date
#     + dim_zone
#     + dim_weather
#     + fact_taxi_trips
#
# Validation bao gồm:
# - kiểm tra path output có tồn tại không
# - kiểm tra có file parquet thật hay không
# - kiểm tra bộ cột có đúng như thiết kế không
# - kiểm tra dataset không rỗng
# - kiểm tra uniqueness theo grain
# - kiểm tra not-null ở các cột cốt lõi
# - kiểm tra logic business / derived columns
# - kiểm tra foreign key coverage của fact sang dimensions
# - kiểm tra reconciliation giữa silver_taxi_trips và fact_taxi_trips
# - kiểm tra partition của fact_taxi_trips
#
# Triết lý:
# - chạy thành công != dữ liệu gold đã sạch
# - validation_gold phải chứng minh fact đủ tin cậy để làm đầu vào
#   cho serving / marts / dashboard
# =========================================================

from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# =========================================================
# IMPORT CONFIG + LOGGER
# ---------------------------------------------------------
# Project của bạn từng có 2 kiểu:
# - src.common.config / src.common.logger
# - src.utils.config / src.utils.logger
# Nên để fallback cho linh hoạt.
# =========================================================
try:
    from src.common.config import load_app_config
    from src.common.logger import get_logger
except ImportError:  # pragma: no cover
    from src.utils.config import load_app_config  # type: ignore
    from src.utils.logger import get_logger  # type: ignore


# =========================================================
# 1) CONSTANTS - BỘ CỘT CHUẨN CỦA GOLD
# =========================================================
EXPECTED_DIM_DATE_COLUMNS = [
    "date_day",
    "year_num",
    "quarter_num",
    "month_num",
    "month_name",
    "day_num_in_month",
    "day_of_week_num",
    "day_of_week_name",
    "is_weekend",
    "gold_loaded_at",
]

EXPECTED_DIM_ZONE_COLUMNS = [
    "location_id",
    "borough",
    "zone",
    "service_zone",
    "gold_loaded_at",
]

EXPECTED_DIM_WEATHER_COLUMNS = [
    "weather_date",
    "temperature_max",
    "temperature_min",
    "temperature_mean",
    "precipitation_sum",
    "snowfall_sum",
    "is_rainy_day",
    "is_snowy_day",
    "gold_loaded_at",
]

EXPECTED_FACT_TAXI_COLUMNS = [
    "trip_id",
    "pickup_datetime",
    "dropoff_datetime",
    "pickup_date",
    "pickup_year",
    "pickup_month",
    "pickup_hour",
    "weather_date",
    "pickup_location_id",
    "dropoff_location_id",
    "vendor_id",
    "rate_code_id",
    "payment_type_code",
    "store_and_fwd_flag",
    "passenger_count",
    "trip_distance",
    "trip_duration_minutes",
    "fare_amount",
    "extra_amount",
    "mta_tax_amount",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge_amount",
    "congestion_surcharge_amount",
    "airport_fee_amount",
    "total_amount",
    "gold_loaded_at",
]


# =========================================================
# 2) HELPER FUNCTIONS
# =========================================================
def _as_path(value: Any) -> Path:
    """
    Chuyển mọi giá trị path thành Path object.
    """
    if isinstance(value, Path):
        return value
    return Path(str(value))


def _resolve_paths(config: dict[str, Any]) -> dict[str, Path]:
    """
    Resolve toàn bộ path cần dùng cho validation gold.

    Bao gồm:
    - input silver (để reconciliation)
    - output gold
    """
    paths_cfg = config["paths"]

    local_data_dir = _as_path(paths_cfg.get("local_data_dir", "data"))

    silver_dir = _as_path(paths_cfg.get("silver_dir", local_data_dir / "silver"))
    silver_taxi_dir = _as_path(paths_cfg.get("silver_taxi_dir", silver_dir / "taxi_trips"))
    silver_weather_dir = _as_path(paths_cfg.get("silver_weather_dir", silver_dir / "weather_daily"))
    silver_zone_dir = _as_path(paths_cfg.get("silver_zone_dir", silver_dir / "zone_lookup"))

    gold_dir = _as_path(paths_cfg.get("gold_dir", local_data_dir / "gold"))
    gold_dim_date_dir = _as_path(paths_cfg.get("gold_dim_date_dir", gold_dir / "dim_date"))
    gold_dim_zone_dir = _as_path(paths_cfg.get("gold_dim_zone_dir", gold_dir / "dim_zone"))
    gold_dim_weather_dir = _as_path(paths_cfg.get("gold_dim_weather_dir", gold_dir / "dim_weather"))
    gold_fact_taxi_dir = _as_path(
        paths_cfg.get(
            "gold_fact_taxi_dir",
            paths_cfg.get("gold_fact_taxi_trips_dir", gold_dir / "fact_taxi_trips"),
        )
    )

    return {
        "silver_taxi_dir": silver_taxi_dir,
        "silver_weather_dir": silver_weather_dir,
        "silver_zone_dir": silver_zone_dir,
        "gold_dim_date_dir": gold_dim_date_dir,
        "gold_dim_zone_dir": gold_dim_zone_dir,
        "gold_dim_weather_dir": gold_dim_weather_dir,
        "gold_fact_taxi_dir": gold_fact_taxi_dir,
    }


def _require_existing_dir(path: Path, label: str) -> None:
    """
    Đảm bảo folder tồn tại và đúng là directory.
    """
    if not path.exists():
        raise FileNotFoundError(f"{label} directory not found: {path}")
    if not path.is_dir():
        raise ValueError(f"{label} exists but is not a directory: {path}")


def _list_parquet_files(path: Path) -> list[Path]:
    """
    Liệt kê toàn bộ file *.parquet bên trong 1 folder.
    """
    return sorted(path.rglob("*.parquet"))


def _assert_has_parquet_files(path: Path, label: str) -> list[Path]:
    """
    Đảm bảo folder có file parquet thật.
    """
    parquet_files = _list_parquet_files(path)
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {label}: {path}")
    return parquet_files


def _assert_columns_match_by_name(
    df: DataFrame,
    expected_columns: list[str],
    dataset_name: str,
) -> None:
    """
    Kiểm tra DataFrame có đủ đúng các cột mong đợi hay không.

    Lưu ý:
    - KHÔNG kiểm tra thứ tự cột tuyệt đối
    - vì parquet partitioned thường khiến Spark đưa cột partition ra cuối schema
    """
    actual_columns = df.columns

    expected_set = set(expected_columns)
    actual_set = set(actual_columns)

    missing_columns = sorted(expected_set - actual_set)
    unexpected_columns = sorted(actual_set - expected_set)

    if missing_columns or unexpected_columns:
        raise ValueError(
            f"[{dataset_name}] Column mismatch.\n"
            f"Missing: {missing_columns}\n"
            f"Unexpected: {unexpected_columns}\n"
            f"Expected: {expected_columns}\n"
            f"Actual:   {actual_columns}"
        )


def _log_schema(logger, dataset_name: str, df: DataFrame) -> None:
    """
    Log schema ngắn gọn của DataFrame.
    """
    logger.info("[%s] columns=%s", dataset_name, ", ".join(df.columns))
    logger.info("[%s] schema=%s", dataset_name, df.schema.simpleString())


def _count_rows(df: DataFrame) -> int:
    """
    Đếm số dòng của DataFrame.
    """
    return df.count()


def build_spark(app_name: str = "validation_gold") -> SparkSession:
    """
    Tạo SparkSession cho job validation gold.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# =========================================================
# 3) READ SILVER / GOLD PARQUET
# =========================================================
def read_parquet_dataset(
    spark: SparkSession,
    dataset_path: Path,
    logger,
    label: str,
) -> DataFrame:
    """
    Đọc parquet dataset từ folder.
    """
    _require_existing_dir(dataset_path, label)
    parquet_files = _assert_has_parquet_files(dataset_path, label)

    logger.info("[%s] path=%s", label, dataset_path)
    logger.info("[%s] parquet file count=%s", label, len(parquet_files))

    return spark.read.parquet(str(dataset_path))


# =========================================================
# 4) VALIDATION CHUNG
# =========================================================
def validate_non_empty(df: DataFrame, dataset_name: str) -> None:
    """
    Dataset gold không được rỗng.
    """
    row_count = _count_rows(df)
    if row_count == 0:
        raise ValueError(f"[{dataset_name}] gold dataset is empty")


def validate_unique_key(df: DataFrame, key_columns: list[str], dataset_name: str) -> None:
    """
    Kiểm tra key phải unique.
    """
    duplicate_count = (
        df.groupBy(*key_columns)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicate_count != 0:
        raise ValueError(
            f"[{dataset_name}] duplicate keys found for {key_columns}: {duplicate_count}"
        )


def validate_exact_row_count(
    left_df: DataFrame,
    right_df: DataFrame,
    left_name: str,
    right_name: str,
    logger,
) -> None:
    """
    Kiểm tra 2 dataset phải có row count bằng nhau.
    """
    left_count = _count_rows(left_df)
    right_count = _count_rows(right_df)

    logger.info("[%s] row_count=%s", left_name, left_count)
    logger.info("[%s] row_count=%s", right_name, right_count)

    if left_count != right_count:
        raise ValueError(
            f"Row count mismatch: {left_name}={left_count}, {right_name}={right_count}"
        )


# =========================================================
# 5) DIM_DATE VALIDATION
# =========================================================
def validate_dim_date_schema(df: DataFrame, logger) -> None:
    """
    Kiểm tra bộ cột dim_date đúng theo tên cột mong đợi.
    """
    _assert_columns_match_by_name(df, EXPECTED_DIM_DATE_COLUMNS, "dim_date")
    _log_schema(logger, "dim_date", df)


def validate_dim_date_not_null(df: DataFrame) -> None:
    """
    Kiểm tra các cột cốt lõi của dim_date không được null.
    """
    null_counts = (
        df.select(
            F.sum(F.col("date_day").isNull().cast("int")).alias("date_day_nulls"),
            F.sum(F.col("year_num").isNull().cast("int")).alias("year_num_nulls"),
            F.sum(F.col("quarter_num").isNull().cast("int")).alias("quarter_num_nulls"),
            F.sum(F.col("month_num").isNull().cast("int")).alias("month_num_nulls"),
            F.sum(F.col("month_name").isNull().cast("int")).alias("month_name_nulls"),
            F.sum(F.col("day_num_in_month").isNull().cast("int")).alias("day_num_in_month_nulls"),
            F.sum(F.col("day_of_week_num").isNull().cast("int")).alias("day_of_week_num_nulls"),
            F.sum(F.col("day_of_week_name").isNull().cast("int")).alias("day_of_week_name_nulls"),
            F.sum(F.col("is_weekend").isNull().cast("int")).alias("is_weekend_nulls"),
            F.sum(F.col("gold_loaded_at").isNull().cast("int")).alias("gold_loaded_at_nulls"),
        ).collect()[0]
    )

    for field_name, value in null_counts.asDict().items():
        if value != 0:
            raise ValueError(f"[dim_date] {field_name} must be 0 but got {value}")


def validate_dim_date_uniqueness(df: DataFrame) -> None:
    """
    dim_date phải unique theo date_day.
    """
    validate_unique_key(df, ["date_day"], "dim_date")


def validate_dim_date_logic(df: DataFrame) -> None:
    """
    Kiểm tra derived columns của dim_date.
    """
    invalid_year_count = (
        df.filter(F.col("year_num") != F.year(F.col("date_day"))).count()
    )
    if invalid_year_count != 0:
        raise ValueError(f"[dim_date] year_num mismatch found: {invalid_year_count}")

    invalid_quarter_count = (
        df.filter(F.col("quarter_num") != F.quarter(F.col("date_day"))).count()
    )
    if invalid_quarter_count != 0:
        raise ValueError(f"[dim_date] quarter_num mismatch found: {invalid_quarter_count}")

    invalid_month_count = (
        df.filter(F.col("month_num") != F.month(F.col("date_day"))).count()
    )
    if invalid_month_count != 0:
        raise ValueError(f"[dim_date] month_num mismatch found: {invalid_month_count}")

    invalid_day_count = (
        df.filter(F.col("day_num_in_month") != F.dayofmonth(F.col("date_day"))).count()
    )
    if invalid_day_count != 0:
        raise ValueError(f"[dim_date] day_num_in_month mismatch found: {invalid_day_count}")

    invalid_dow_count = (
        df.filter(F.col("day_of_week_num") != F.dayofweek(F.col("date_day"))).count()
    )
    if invalid_dow_count != 0:
        raise ValueError(f"[dim_date] day_of_week_num mismatch found: {invalid_dow_count}")

    invalid_weekend_count = (
        df.filter(
            F.col("is_weekend") != F.col("day_of_week_num").isin([1, 7])
        ).count()
    )
    if invalid_weekend_count != 0:
        raise ValueError(f"[dim_date] is_weekend mismatch found: {invalid_weekend_count}")


def validate_dim_date_matches_silver_taxi(
    dim_date_df: DataFrame,
    silver_taxi_df: DataFrame,
    logger,
) -> None:
    """
    Kiểm tra dim_date có số ngày bằng số pickup_date distinct từ silver_taxi_trips.
    """
    expected_count = (
        silver_taxi_df
        .select("pickup_date")
        .filter(F.col("pickup_date").isNotNull())
        .distinct()
        .count()
    )
    actual_count = dim_date_df.count()

    logger.info("[dim_date] expected distinct pickup_date count=%s", expected_count)
    logger.info("[dim_date] actual row_count=%s", actual_count)

    if actual_count != expected_count:
        raise ValueError(
            f"[dim_date] row_count != distinct pickup_date count from silver_taxi_trips | "
            f"expected={expected_count}, actual={actual_count}"
        )


# =========================================================
# 6) DIM_ZONE VALIDATION
# =========================================================
def validate_dim_zone_schema(df: DataFrame, logger) -> None:
    """
    Kiểm tra bộ cột dim_zone đúng theo tên cột mong đợi.
    """
    _assert_columns_match_by_name(df, EXPECTED_DIM_ZONE_COLUMNS, "dim_zone")
    _log_schema(logger, "dim_zone", df)


def validate_dim_zone_not_null(df: DataFrame) -> None:
    """
    Kiểm tra các cột cốt lõi của dim_zone không được null.
    """
    null_counts = (
        df.select(
            F.sum(F.col("location_id").isNull().cast("int")).alias("location_id_nulls"),
            F.sum(F.col("borough").isNull().cast("int")).alias("borough_nulls"),
            F.sum(F.col("zone").isNull().cast("int")).alias("zone_nulls"),
            F.sum(F.col("service_zone").isNull().cast("int")).alias("service_zone_nulls"),
            F.sum(F.col("gold_loaded_at").isNull().cast("int")).alias("gold_loaded_at_nulls"),
        ).collect()[0]
    )

    for field_name, value in null_counts.asDict().items():
        if value != 0:
            raise ValueError(f"[dim_zone] {field_name} must be 0 but got {value}")


def validate_dim_zone_string_quality(df: DataFrame) -> None:
    """
    Kiểm tra các cột string quan trọng không rỗng sau khi trim.
    """
    blank_counts = (
        df.select(
            F.sum((F.trim(F.col("borough")) == "").cast("int")).alias("blank_borough_rows"),
            F.sum((F.trim(F.col("zone")) == "").cast("int")).alias("blank_zone_rows"),
            F.sum((F.trim(F.col("service_zone")) == "").cast("int")).alias("blank_service_zone_rows"),
        ).collect()[0]
    )

    for field_name, value in blank_counts.asDict().items():
        if value != 0:
            raise ValueError(f"[dim_zone] {field_name} must be 0 but got {value}")


def validate_dim_zone_uniqueness(df: DataFrame) -> None:
    """
    dim_zone phải unique theo location_id.
    """
    validate_unique_key(df, ["location_id"], "dim_zone")


def validate_dim_zone_matches_silver_zone(
    dim_zone_df: DataFrame,
    silver_zone_df: DataFrame,
    logger,
) -> None:
    """
    Kiểm tra dim_zone giữ nguyên row count so với silver_zone_lookup.
    """
    validate_exact_row_count(
        left_df=silver_zone_df,
        right_df=dim_zone_df,
        left_name="silver_zone_lookup",
        right_name="dim_zone",
        logger=logger,
    )


# =========================================================
# 7) DIM_WEATHER VALIDATION
# =========================================================
def validate_dim_weather_schema(df: DataFrame, logger) -> None:
    """
    Kiểm tra bộ cột dim_weather đúng theo tên cột mong đợi.
    """
    _assert_columns_match_by_name(df, EXPECTED_DIM_WEATHER_COLUMNS, "dim_weather")
    _log_schema(logger, "dim_weather", df)


def validate_dim_weather_not_null(df: DataFrame) -> None:
    """
    Kiểm tra các cột cốt lõi của dim_weather không được null.
    """
    null_counts = (
        df.select(
            F.sum(F.col("weather_date").isNull().cast("int")).alias("weather_date_nulls"),
            F.sum(F.col("temperature_max").isNull().cast("int")).alias("temperature_max_nulls"),
            F.sum(F.col("temperature_min").isNull().cast("int")).alias("temperature_min_nulls"),
            F.sum(F.col("temperature_mean").isNull().cast("int")).alias("temperature_mean_nulls"),
            F.sum(F.col("precipitation_sum").isNull().cast("int")).alias("precipitation_sum_nulls"),
            F.sum(F.col("snowfall_sum").isNull().cast("int")).alias("snowfall_sum_nulls"),
            F.sum(F.col("is_rainy_day").isNull().cast("int")).alias("is_rainy_day_nulls"),
            F.sum(F.col("is_snowy_day").isNull().cast("int")).alias("is_snowy_day_nulls"),
            F.sum(F.col("gold_loaded_at").isNull().cast("int")).alias("gold_loaded_at_nulls"),
        ).collect()[0]
    )

    for field_name, value in null_counts.asDict().items():
        if value != 0:
            raise ValueError(f"[dim_weather] {field_name} must be 0 but got {value}")


def validate_dim_weather_uniqueness(df: DataFrame) -> None:
    """
    dim_weather phải unique theo weather_date.
    """
    validate_unique_key(df, ["weather_date"], "dim_weather")


def validate_dim_weather_logic(df: DataFrame) -> None:
    """
    Kiểm tra cờ weather logic:
    - precipitation_sum > 0  <=> is_rainy_day = True
    - snowfall_sum > 0       <=> is_snowy_day = True
    """
    bad_rain_flag_count = (
        df.filter(
            ((F.col("precipitation_sum") > 0) & (F.col("is_rainy_day") == F.lit(False))) |
            ((F.col("precipitation_sum") <= 0) & (F.col("is_rainy_day") == F.lit(True)))
        ).count()
    )
    if bad_rain_flag_count != 0:
        raise ValueError(f"[dim_weather] is_rainy_day mismatch found: {bad_rain_flag_count}")

    bad_snow_flag_count = (
        df.filter(
            ((F.col("snowfall_sum") > 0) & (F.col("is_snowy_day") == F.lit(False))) |
            ((F.col("snowfall_sum") <= 0) & (F.col("is_snowy_day") == F.lit(True)))
        ).count()
    )
    if bad_snow_flag_count != 0:
        raise ValueError(f"[dim_weather] is_snowy_day mismatch found: {bad_snow_flag_count}")


def validate_dim_weather_matches_silver_weather(
    dim_weather_df: DataFrame,
    silver_weather_df: DataFrame,
    logger,
) -> None:
    """
    Kiểm tra dim_weather giữ nguyên row count so với silver_weather_daily.
    """
    validate_exact_row_count(
        left_df=silver_weather_df,
        right_df=dim_weather_df,
        left_name="silver_weather_daily",
        right_name="dim_weather",
        logger=logger,
    )


# =========================================================
# 8) FACT_TAXI_TRIPS VALIDATION
# =========================================================
def validate_fact_taxi_schema(df: DataFrame, logger) -> None:
    """
    Kiểm tra bộ cột fact_taxi_trips đúng theo tên cột mong đợi.
    """
    _assert_columns_match_by_name(df, EXPECTED_FACT_TAXI_COLUMNS, "fact_taxi_trips")
    _log_schema(logger, "fact_taxi_trips", df)


def validate_fact_taxi_not_null(df: DataFrame) -> None:
    """
    Kiểm tra các cột cốt lõi của fact_taxi_trips không được null.

    Lưu ý:
    - Không ép tất cả degenerate dimensions phải non-null
      (ví dụ vendor_id / rate_code_id / payment_type_code có thể phụ thuộc upstream)
    - Nhưng các key cốt lõi, time columns và measures chính phải sạch
    """
    null_counts = (
        df.select(
            F.sum(F.col("trip_id").isNull().cast("int")).alias("trip_id_nulls"),
            F.sum(F.col("pickup_datetime").isNull().cast("int")).alias("pickup_datetime_nulls"),
            F.sum(F.col("dropoff_datetime").isNull().cast("int")).alias("dropoff_datetime_nulls"),
            F.sum(F.col("pickup_date").isNull().cast("int")).alias("pickup_date_nulls"),
            F.sum(F.col("pickup_year").isNull().cast("int")).alias("pickup_year_nulls"),
            F.sum(F.col("pickup_month").isNull().cast("int")).alias("pickup_month_nulls"),
            F.sum(F.col("pickup_hour").isNull().cast("int")).alias("pickup_hour_nulls"),
            F.sum(F.col("weather_date").isNull().cast("int")).alias("weather_date_nulls"),
            F.sum(F.col("pickup_location_id").isNull().cast("int")).alias("pickup_location_id_nulls"),
            F.sum(F.col("dropoff_location_id").isNull().cast("int")).alias("dropoff_location_id_nulls"),
            F.sum(F.col("trip_distance").isNull().cast("int")).alias("trip_distance_nulls"),
            F.sum(F.col("trip_duration_minutes").isNull().cast("int")).alias("trip_duration_minutes_nulls"),
            F.sum(F.col("fare_amount").isNull().cast("int")).alias("fare_amount_nulls"),
            F.sum(F.col("extra_amount").isNull().cast("int")).alias("extra_amount_nulls"),
            F.sum(F.col("mta_tax_amount").isNull().cast("int")).alias("mta_tax_amount_nulls"),
            F.sum(F.col("tip_amount").isNull().cast("int")).alias("tip_amount_nulls"),
            F.sum(F.col("tolls_amount").isNull().cast("int")).alias("tolls_amount_nulls"),
            F.sum(F.col("improvement_surcharge_amount").isNull().cast("int")).alias("improvement_surcharge_amount_nulls"),
            F.sum(F.col("congestion_surcharge_amount").isNull().cast("int")).alias("congestion_surcharge_amount_nulls"),
            F.sum(F.col("airport_fee_amount").isNull().cast("int")).alias("airport_fee_amount_nulls"),
            F.sum(F.col("total_amount").isNull().cast("int")).alias("total_amount_nulls"),
            F.sum(F.col("gold_loaded_at").isNull().cast("int")).alias("gold_loaded_at_nulls"),
        ).collect()[0]
    )

    for field_name, value in null_counts.asDict().items():
        if value != 0:
            raise ValueError(f"[fact_taxi_trips] {field_name} must be 0 but got {value}")


def validate_fact_taxi_uniqueness(df: DataFrame) -> None:
    """
    fact_taxi_trips ở grain 1 trip nên trip_id phải unique.
    """
    validate_unique_key(df, ["trip_id"], "fact_taxi_trips")


def validate_fact_taxi_time_logic(df: DataFrame) -> None:
    """
    Kiểm tra logic thời gian của fact:
    - pickup < dropoff
    - trip_duration_minutes > 0
    - weather_date phải khớp pickup_date theo design hiện tại
    """
    invalid_pickup_dropoff_count = (
        df.filter(F.col("pickup_datetime") >= F.col("dropoff_datetime")).count()
    )
    if invalid_pickup_dropoff_count != 0:
        raise ValueError(
            f"[fact_taxi_trips] pickup_datetime >= dropoff_datetime found: "
            f"{invalid_pickup_dropoff_count}"
        )

    invalid_duration_count = (
        df.filter(F.col("trip_duration_minutes") <= 0).count()
    )
    if invalid_duration_count != 0:
        raise ValueError(
            f"[fact_taxi_trips] trip_duration_minutes <= 0 found: {invalid_duration_count}"
        )

    mismatched_weather_date_count = (
        df.filter(F.col("pickup_date") != F.col("weather_date")).count()
    )
    if mismatched_weather_date_count != 0:
        raise ValueError(
            f"[fact_taxi_trips] pickup_date != weather_date found: {mismatched_weather_date_count}"
        )


def validate_fact_taxi_derived_columns(df: DataFrame) -> None:
    """
    Kiểm tra các cột dẫn xuất có khớp với pickup_date / pickup_datetime.
    """
    invalid_year_count = (
        df.filter(F.col("pickup_year") != F.year(F.col("pickup_date"))).count()
    )
    if invalid_year_count != 0:
        raise ValueError(
            f"[fact_taxi_trips] pickup_year mismatch found: {invalid_year_count}"
        )

    invalid_month_count = (
        df.filter(F.col("pickup_month") != F.month(F.col("pickup_date"))).count()
    )
    if invalid_month_count != 0:
        raise ValueError(
            f"[fact_taxi_trips] pickup_month mismatch found: {invalid_month_count}"
        )

    invalid_hour_count = (
        df.filter(F.col("pickup_hour") != F.hour(F.col("pickup_datetime"))).count()
    )
    if invalid_hour_count != 0:
        raise ValueError(
            f"[fact_taxi_trips] pickup_hour mismatch found: {invalid_hour_count}"
        )


def validate_fact_taxi_measure_sanity(df: DataFrame, logger) -> None:
    """
    Kiểm tra sanity của các measure trong fact.

    Hard fail:
    - trip_distance < 0
    - trip_duration_minutes <= 0

    Warning:
    - một số money columns âm (nếu có) để người dùng review thêm
    """
    negative_distance_count = (
        df.filter(F.col("trip_distance") < 0).count()
    )
    if negative_distance_count != 0:
        raise ValueError(
            f"[fact_taxi_trips] trip_distance < 0 found: {negative_distance_count}"
        )

    money_warning_counts = (
        df.select(
            F.sum((F.col("fare_amount") < 0).cast("int")).alias("negative_fare_amount_rows"),
            F.sum((F.col("extra_amount") < 0).cast("int")).alias("negative_extra_amount_rows"),
            F.sum((F.col("mta_tax_amount") < 0).cast("int")).alias("negative_mta_tax_amount_rows"),
            F.sum((F.col("tip_amount") < 0).cast("int")).alias("negative_tip_amount_rows"),
            F.sum((F.col("tolls_amount") < 0).cast("int")).alias("negative_tolls_amount_rows"),
            F.sum((F.col("improvement_surcharge_amount") < 0).cast("int")).alias("negative_improvement_surcharge_rows"),
            F.sum((F.col("congestion_surcharge_amount") < 0).cast("int")).alias("negative_congestion_surcharge_rows"),
            F.sum((F.col("airport_fee_amount") < 0).cast("int")).alias("negative_airport_fee_rows"),
            F.sum((F.col("total_amount") < 0).cast("int")).alias("negative_total_amount_rows"),
        ).collect()[0]
    )

    for field_name, value in money_warning_counts.asDict().items():
        if value != 0:
            logger.warning("[fact_taxi_trips] %s=%s", field_name, value)


def validate_fact_taxi_matches_silver_taxi(
    fact_df: DataFrame,
    silver_taxi_df: DataFrame,
    logger,
) -> None:
    """
    Kiểm tra fact giữ nguyên grain và số dòng so với silver_taxi_trips.
    """
    validate_exact_row_count(
        left_df=silver_taxi_df,
        right_df=fact_df,
        left_name="silver_taxi_trips",
        right_name="fact_taxi_trips",
        logger=logger,
    )


def validate_fact_taxi_trip_id_set_matches_silver(
    fact_df: DataFrame,
    silver_taxi_df: DataFrame,
) -> None:
    """
    Kiểm tra tập trip_id của fact và silver phải giống nhau.

    Điều này mạnh hơn chỉ so row count, vì row count bằng nhau
    vẫn chưa chắc cùng đúng tập record.
    """
    fact_trip_ids = fact_df.select("trip_id").distinct()
    silver_trip_ids = silver_taxi_df.select("trip_id").distinct()

    missing_in_fact_count = (
        silver_trip_ids
        .join(fact_trip_ids, on="trip_id", how="left_anti")
        .count()
    )
    if missing_in_fact_count != 0:
        raise ValueError(
            f"[fact_taxi_trips] trip_ids missing in fact compared with silver_taxi_trips: "
            f"{missing_in_fact_count}"
        )

    unexpected_in_fact_count = (
        fact_trip_ids
        .join(silver_trip_ids, on="trip_id", how="left_anti")
        .count()
    )
    if unexpected_in_fact_count != 0:
        raise ValueError(
            f"[fact_taxi_trips] unexpected trip_ids found in fact compared with silver_taxi_trips: "
            f"{unexpected_in_fact_count}"
        )


def validate_fact_taxi_fk_coverage(
    fact_df: DataFrame,
    dim_date_df: DataFrame,
    dim_zone_df: DataFrame,
    dim_weather_df: DataFrame,
) -> None:
    """
    Kiểm tra fact có join đầy đủ được sang các dimension hay không.

    Rule:
    - pickup_date phải resolve được vào dim_date.date_day
    - pickup_location_id phải resolve được vào dim_zone.location_id
    - dropoff_location_id phải resolve được vào dim_zone.location_id
    - weather_date phải resolve được vào dim_weather.weather_date
    """
    missing_date_fk_count = (
        fact_df.alias("f")
        .join(
            dim_date_df.select(F.col("date_day")).alias("d"),
            F.col("f.pickup_date") == F.col("d.date_day"),
            how="left",
        )
        .filter(F.col("d.date_day").isNull())
        .count()
    )
    if missing_date_fk_count != 0:
        raise ValueError(
            f"[fact_taxi_trips] missing dim_date foreign key rows: {missing_date_fk_count}"
        )

    missing_pickup_zone_fk_count = (
        fact_df.alias("f")
        .join(
            dim_zone_df.select(F.col("location_id")).alias("z"),
            F.col("f.pickup_location_id") == F.col("z.location_id"),
            how="left",
        )
        .filter(F.col("z.location_id").isNull())
        .count()
    )
    if missing_pickup_zone_fk_count != 0:
        raise ValueError(
            f"[fact_taxi_trips] missing pickup dim_zone foreign key rows: {missing_pickup_zone_fk_count}"
        )

    missing_dropoff_zone_fk_count = (
        fact_df.alias("f")
        .join(
            dim_zone_df.select(F.col("location_id")).alias("z"),
            F.col("f.dropoff_location_id") == F.col("z.location_id"),
            how="left",
        )
        .filter(F.col("z.location_id").isNull())
        .count()
    )
    if missing_dropoff_zone_fk_count != 0:
        raise ValueError(
            f"[fact_taxi_trips] missing dropoff dim_zone foreign key rows: {missing_dropoff_zone_fk_count}"
        )

    missing_weather_fk_count = (
        fact_df.alias("f")
        .join(
            dim_weather_df.select(F.col("weather_date")).alias("w"),
            F.col("f.weather_date") == F.col("w.weather_date"),
            how="left",
        )
        .filter(F.col("w.weather_date").isNull())
        .count()
    )
    if missing_weather_fk_count != 0:
        raise ValueError(
            f"[fact_taxi_trips] missing dim_weather foreign key rows: {missing_weather_fk_count}"
        )


def validate_fact_taxi_partitions(df: DataFrame, gold_path: Path, logger) -> None:
    """
    Kiểm tra partition fact_taxi_trips:
    - có partition trong dữ liệu
    - có partition directory trên disk
    """
    partition_rows = (
        df.select("pickup_year", "pickup_month")
        .distinct()
        .orderBy("pickup_year", "pickup_month")
        .collect()
    )

    partitions = [(row["pickup_year"], row["pickup_month"]) for row in partition_rows]
    logger.info("[fact_taxi_trips] distinct partitions=%s", partitions)

    if not partitions:
        raise ValueError("[fact_taxi_trips] no partitions found in dataframe")

    partition_dirs = sorted(gold_path.rglob("pickup_month=*"))
    if not partition_dirs:
        raise ValueError(f"[fact_taxi_trips] no partition directories found in {gold_path}")

    logger.info("[fact_taxi_trips] partition directory count=%s", len(partition_dirs))


# =========================================================
# 9) MAIN
# =========================================================
def main() -> None:
    """
    Chạy toàn bộ validation gold theo thứ tự:
    - đọc silver sources để reconciliation
    - đọc gold outputs
    - validate dim_date
    - validate dim_zone
    - validate dim_weather
    - validate fact_taxi_trips
    """
    config = load_app_config()
    paths = _resolve_paths(config)

    logger = get_logger(
        name="validation_gold",
        log_dir=config["paths"]["log_dir"],
    )

    logger.info("=" * 60)
    logger.info("START GOLD VALIDATION")
    logger.info("=" * 60)
    logger.info("Resolved validation paths: %s", {k: str(v) for k, v in paths.items()})

    spark = build_spark(app_name="validation_gold")

    try:
        # -------------------------------------------------
        # READ SILVER SOURCES FOR RECONCILIATION
        # -------------------------------------------------
        logger.info("Reading silver sources for reconciliation...")

        silver_taxi_df = read_parquet_dataset(
            spark=spark,
            dataset_path=paths["silver_taxi_dir"],
            logger=logger,
            label="silver_taxi_trips",
        )

        silver_weather_df = read_parquet_dataset(
            spark=spark,
            dataset_path=paths["silver_weather_dir"],
            logger=logger,
            label="silver_weather_daily",
        )

        silver_zone_df = read_parquet_dataset(
            spark=spark,
            dataset_path=paths["silver_zone_dir"],
            logger=logger,
            label="silver_zone_lookup",
        )

        # -------------------------------------------------
        # READ GOLD OUTPUTS
        # -------------------------------------------------
        logger.info("Reading gold outputs...")

        dim_date_df = read_parquet_dataset(
            spark=spark,
            dataset_path=paths["gold_dim_date_dir"],
            logger=logger,
            label="dim_date",
        )

        dim_zone_df = read_parquet_dataset(
            spark=spark,
            dataset_path=paths["gold_dim_zone_dir"],
            logger=logger,
            label="dim_zone",
        )

        dim_weather_df = read_parquet_dataset(
            spark=spark,
            dataset_path=paths["gold_dim_weather_dir"],
            logger=logger,
            label="dim_weather",
        )

        fact_taxi_df = read_parquet_dataset(
            spark=spark,
            dataset_path=paths["gold_fact_taxi_dir"],
            logger=logger,
            label="fact_taxi_trips",
        )

        # -------------------------------------------------
        # DIM_DATE VALIDATION
        # -------------------------------------------------
        logger.info("Validating dim_date...")

        validate_non_empty(dim_date_df, "dim_date")
        validate_dim_date_schema(dim_date_df, logger)
        validate_dim_date_not_null(dim_date_df)
        validate_dim_date_uniqueness(dim_date_df)
        validate_dim_date_logic(dim_date_df)
        validate_dim_date_matches_silver_taxi(
            dim_date_df=dim_date_df,
            silver_taxi_df=silver_taxi_df,
            logger=logger,
        )

        logger.info("dim_date validation passed.")

        # -------------------------------------------------
        # DIM_ZONE VALIDATION
        # -------------------------------------------------
        logger.info("Validating dim_zone...")

        validate_non_empty(dim_zone_df, "dim_zone")
        validate_dim_zone_schema(dim_zone_df, logger)
        validate_dim_zone_not_null(dim_zone_df)
        validate_dim_zone_string_quality(dim_zone_df)
        validate_dim_zone_uniqueness(dim_zone_df)
        validate_dim_zone_matches_silver_zone(
            dim_zone_df=dim_zone_df,
            silver_zone_df=silver_zone_df,
            logger=logger,
        )

        logger.info("dim_zone validation passed.")

        # -------------------------------------------------
        # DIM_WEATHER VALIDATION
        # -------------------------------------------------
        logger.info("Validating dim_weather...")

        validate_non_empty(dim_weather_df, "dim_weather")
        validate_dim_weather_schema(dim_weather_df, logger)
        validate_dim_weather_not_null(dim_weather_df)
        validate_dim_weather_uniqueness(dim_weather_df)
        validate_dim_weather_logic(dim_weather_df)
        validate_dim_weather_matches_silver_weather(
            dim_weather_df=dim_weather_df,
            silver_weather_df=silver_weather_df,
            logger=logger,
        )

        logger.info("dim_weather validation passed.")

        # -------------------------------------------------
        # FACT_TAXI_TRIPS VALIDATION
        # -------------------------------------------------
        logger.info("Validating fact_taxi_trips...")

        validate_non_empty(fact_taxi_df, "fact_taxi_trips")
        validate_fact_taxi_schema(fact_taxi_df, logger)
        validate_fact_taxi_not_null(fact_taxi_df)
        validate_fact_taxi_uniqueness(fact_taxi_df)
        validate_fact_taxi_time_logic(fact_taxi_df)
        validate_fact_taxi_derived_columns(fact_taxi_df)
        validate_fact_taxi_measure_sanity(fact_taxi_df, logger)
        validate_fact_taxi_matches_silver_taxi(
            fact_df=fact_taxi_df,
            silver_taxi_df=silver_taxi_df,
            logger=logger,
        )
        validate_fact_taxi_trip_id_set_matches_silver(
            fact_df=fact_taxi_df,
            silver_taxi_df=silver_taxi_df,
        )
        validate_fact_taxi_fk_coverage(
            fact_df=fact_taxi_df,
            dim_date_df=dim_date_df,
            dim_zone_df=dim_zone_df,
            dim_weather_df=dim_weather_df,
        )
        validate_fact_taxi_partitions(
            df=fact_taxi_df,
            gold_path=paths["gold_fact_taxi_dir"],
            logger=logger,
        )

        logger.info("fact_taxi_trips validation passed.")

        logger.info("=" * 60)
        logger.info("GOLD VALIDATION FINISHED SUCCESSFULLY")
        logger.info("=" * 60)

    finally:
        spark.stop()


# =========================================================
# 10) ENTRYPOINT
# =========================================================
if __name__ == "__main__":
    main()