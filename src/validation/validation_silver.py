from __future__ import annotations

# =========================================================
# validation_silver.py
# ---------------------------------------------------------
# Mục tiêu:
# - Kiểm tra layer silver sau khi job Spark bronze_to_silver chạy xong
# - Kiểm tra path output có tồn tại không
# - Kiểm tra có file parquet thật hay không
# - Kiểm tra bộ cột có đúng như thiết kế không
# - So sánh row count giữa bronze và silver base
# - Kiểm tra partition taxi / weather / enriched
# - Kiểm tra null / duplicate / key uniqueness
# - Kiểm tra logic thời gian của taxi silver
# - Kiểm tra join completeness của bảng enriched
#
# Lưu ý:
# - Đây là validation kỹ thuật + business rules cơ bản của silver
# - Chưa phải data quality framework đầy đủ kiểu quarantine / anomaly detection
# =========================================================

import glob
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
# 1) CONSTANTS - BỘ CỘT CHUẨN CỦA SILVER
# =========================================================
EXPECTED_SILVER_TAXI_COLUMNS = [
    "trip_id",
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "pickup_date",
    "pickup_year",
    "pickup_month",
    "pickup_hour",
    "passenger_count",
    "trip_distance",
    "trip_duration_minutes",
    "rate_code_id",
    "store_and_fwd_flag",
    "pickup_location_id",
    "dropoff_location_id",
    "payment_type_code",
    "fare_amount",
    "extra_amount",
    "mta_tax_amount",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge_amount",
    "congestion_surcharge_amount",
    "airport_fee_amount",
    "total_amount",
    "source_file",
    "bronze_loaded_at",
    "silver_loaded_at",
]

EXPECTED_SILVER_WEATHER_COLUMNS = [
    "weather_date",
    "temperature_max",
    "temperature_min",
    "temperature_mean",
    "precipitation_sum",
    "snowfall_sum",
    "is_rainy_day",
    "is_snowy_day",
    "weather_year",
    "weather_month",
    "source_file",
    "bronze_loaded_at",
    "silver_loaded_at",
]

EXPECTED_SILVER_ZONE_COLUMNS = [
    "location_id",
    "borough",
    "zone",
    "service_zone",
    "source_file",
    "bronze_loaded_at",
    "silver_loaded_at",
]

EXPECTED_SILVER_TAXI_ENRICHED_COLUMNS = [
    "trip_id",
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "pickup_date",
    "pickup_year",
    "pickup_month",
    "pickup_hour",
    "passenger_count",
    "trip_distance",
    "trip_duration_minutes",
    "rate_code_id",
    "store_and_fwd_flag",
    "pickup_location_id",
    "pickup_borough",
    "pickup_zone",
    "pickup_service_zone",
    "dropoff_location_id",
    "dropoff_borough",
    "dropoff_zone",
    "dropoff_service_zone",
    "payment_type_code",
    "fare_amount",
    "extra_amount",
    "mta_tax_amount",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge_amount",
    "congestion_surcharge_amount",
    "airport_fee_amount",
    "total_amount",
    "weather_date",
    "temperature_max",
    "temperature_min",
    "temperature_mean",
    "precipitation_sum",
    "snowfall_sum",
    "is_rainy_day",
    "is_snowy_day",
    "source_file",
    "bronze_loaded_at",
    "silver_loaded_at",
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
    Resolve toàn bộ path cần dùng cho validation silver.

    Bao gồm:
    - input bronze
    - output silver
    """
    paths_cfg = config["paths"]

    local_data_dir = _as_path(paths_cfg.get("local_data_dir", "data"))

    bronze_dir = _as_path(paths_cfg.get("bronze_dir", local_data_dir / "bronze"))
    bronze_taxi_dir = _as_path(paths_cfg.get("bronze_taxi_dir", bronze_dir / "taxi_trips_raw"))
    bronze_weather_dir = _as_path(paths_cfg.get("bronze_weather_dir", bronze_dir / "weather_raw"))
    bronze_zone_dir = _as_path(paths_cfg.get("bronze_zone_dir", bronze_dir / "zone_lookup"))

    silver_dir = _as_path(paths_cfg.get("silver_dir", local_data_dir / "silver"))
    silver_taxi_dir = _as_path(paths_cfg.get("silver_taxi_dir", silver_dir / "taxi_trips"))
    silver_weather_dir = _as_path(paths_cfg.get("silver_weather_dir", silver_dir / "weather_daily"))
    silver_zone_dir = _as_path(paths_cfg.get("silver_zone_dir", silver_dir / "zone_lookup"))
    silver_taxi_enriched_dir = _as_path(
        paths_cfg.get("silver_taxi_enriched_dir", silver_dir / "taxi_trips_enriched")
    )

    return {
        "bronze_taxi_dir": bronze_taxi_dir,
        "bronze_weather_dir": bronze_weather_dir,
        "bronze_zone_dir": bronze_zone_dir,
        "silver_taxi_dir": silver_taxi_dir,
        "silver_weather_dir": silver_weather_dir,
        "silver_zone_dir": silver_zone_dir,
        "silver_taxi_enriched_dir": silver_taxi_enriched_dir,
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
    - Vì parquet partitioned thường khiến Spark đưa cột partition ra cuối schema

    Rule:
    - không thiếu cột
    - không thừa cột
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


def build_spark(app_name: str = "validation_silver") -> SparkSession:
    """
    Tạo SparkSession cho job validation silver.
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
# 3) READ BRONZE / SILVER PARQUET
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
    Dataset silver không được rỗng.
    """
    row_count = _count_rows(df)
    if row_count == 0:
        raise ValueError(f"[{dataset_name}] silver dataset is empty")


def validate_row_count_relation(
    source_df: DataFrame,
    target_df: DataFrame,
    source_name: str,
    target_name: str,
    logger,
    allow_target_less_than_source: bool = True,
) -> None:
    """
    So sánh row count giữa nguồn và đích.

    Dùng cho:
    - bronze -> silver base

    Rule:
    - target > source => fail
    - target < source:
        + nếu không được phép => fail
        + nếu được phép => warning
    """
    source_count = _count_rows(source_df)
    target_count = _count_rows(target_df)

    logger.info("[%s -> %s] source_count=%s", source_name, target_name, source_count)
    logger.info("[%s -> %s] target_count=%s", source_name, target_name, target_count)

    if target_count > source_count:
        raise ValueError(
            f"[{target_name}] target_count > source_count | "
            f"source={source_count}, target={target_count}"
        )

    if target_count < source_count:
        if allow_target_less_than_source:
            logger.warning(
                "[%s -> %s] target_count < source_count | source=%s, target=%s, diff=%s",
                source_name,
                target_name,
                source_count,
                target_count,
                source_count - target_count,
            )
        else:
            raise ValueError(
                f"[{target_name}] target_count < source_count | "
                f"source={source_count}, target={target_count}"
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

    Dùng cho:
    - silver_taxi_trips vs silver_taxi_trips_enriched
    """
    left_count = _count_rows(left_df)
    right_count = _count_rows(right_df)

    logger.info("[%s] row_count=%s", left_name, left_count)
    logger.info("[%s] row_count=%s", right_name, right_count)

    if left_count != right_count:
        raise ValueError(
            f"Row count mismatch: {left_name}={left_count}, {right_name}={right_count}"
        )


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


# =========================================================
# 5) TAXI SILVER VALIDATION
# =========================================================
def validate_taxi_silver_schema(df: DataFrame, logger) -> None:
    """
    Kiểm tra bộ cột taxi silver đúng theo tên cột mong đợi.
    """
    _assert_columns_match_by_name(df, EXPECTED_SILVER_TAXI_COLUMNS, "silver_taxi_trips")
    _log_schema(logger, "silver_taxi_trips", df)


def validate_taxi_silver_not_null(df: DataFrame) -> None:
    """
    Kiểm tra các cột kỹ thuật và business quan trọng của taxi silver không được null.
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
            F.sum(F.col("trip_duration_minutes").isNull().cast("int")).alias("trip_duration_minutes_nulls"),
            F.sum(F.col("pickup_location_id").isNull().cast("int")).alias("pickup_location_id_nulls"),
            F.sum(F.col("dropoff_location_id").isNull().cast("int")).alias("dropoff_location_id_nulls"),
            F.sum(F.col("source_file").isNull().cast("int")).alias("source_file_nulls"),
            F.sum(F.col("bronze_loaded_at").isNull().cast("int")).alias("bronze_loaded_at_nulls"),
            F.sum(F.col("silver_loaded_at").isNull().cast("int")).alias("silver_loaded_at_nulls"),
        )
        .collect()[0]
    )

    for field_name, value in null_counts.asDict().items():
        if value != 0:
            raise ValueError(f"[silver_taxi_trips] {field_name} must be 0 but got {value}")


def validate_taxi_silver_uniqueness(df: DataFrame) -> None:
    """
    trip_id là surrogate key ở grain 1 trip nên phải unique.
    """
    validate_unique_key(df, ["trip_id"], "silver_taxi_trips")


def validate_taxi_silver_time_logic(df: DataFrame) -> None:
    """
    Kiểm tra logic thời gian của taxi silver:
    - pickup < dropoff
    - trip_duration_minutes > 0
    """
    invalid_pickup_dropoff_count = (
        df.filter(F.col("pickup_datetime") >= F.col("dropoff_datetime")).count()
    )
    if invalid_pickup_dropoff_count != 0:
        raise ValueError(
            f"[silver_taxi_trips] pickup_datetime >= dropoff_datetime found: "
            f"{invalid_pickup_dropoff_count}"
        )

    invalid_duration_count = (
        df.filter(F.col("trip_duration_minutes") <= 0).count()
    )
    if invalid_duration_count != 0:
        raise ValueError(
            f"[silver_taxi_trips] trip_duration_minutes <= 0 found: {invalid_duration_count}"
        )


def validate_taxi_silver_derived_columns(df: DataFrame) -> None:
    """
    Kiểm tra các cột dẫn xuất có khớp với pickup_date hay không:
    - pickup_year == year(pickup_date)
    - pickup_month == month(pickup_date)
    - pickup_hour == hour(pickup_datetime)
    """
    invalid_year_count = (
        df.filter(F.col("pickup_year") != F.year(F.col("pickup_date"))).count()
    )
    if invalid_year_count != 0:
        raise ValueError(
            f"[silver_taxi_trips] pickup_year mismatch found: {invalid_year_count}"
        )

    invalid_month_count = (
        df.filter(F.col("pickup_month") != F.month(F.col("pickup_date"))).count()
    )
    if invalid_month_count != 0:
        raise ValueError(
            f"[silver_taxi_trips] pickup_month mismatch found: {invalid_month_count}"
        )

    invalid_hour_count = (
        df.filter(F.col("pickup_hour") != F.hour(F.col("pickup_datetime"))).count()
    )
    if invalid_hour_count != 0:
        raise ValueError(
            f"[silver_taxi_trips] pickup_hour mismatch found: {invalid_hour_count}"
        )


def validate_taxi_silver_partitions(df: DataFrame, silver_path: Path, logger) -> None:
    """
    Kiểm tra partition taxi silver:
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
    logger.info("[silver_taxi_trips] distinct partitions=%s", partitions)

    if not partitions:
        raise ValueError("[silver_taxi_trips] no partitions found in dataframe")

    partition_dirs = sorted(silver_path.rglob("pickup_month=*"))
    if not partition_dirs:
        raise ValueError(f"[silver_taxi_trips] no partition directories found in {silver_path}")

    logger.info("[silver_taxi_trips] partition directory count=%s", len(partition_dirs))


# =========================================================
# 6) WEATHER SILVER VALIDATION
# =========================================================
def validate_weather_silver_schema(df: DataFrame, logger) -> None:
    """
    Kiểm tra bộ cột weather silver đúng theo tên cột mong đợi.
    """
    _assert_columns_match_by_name(df, EXPECTED_SILVER_WEATHER_COLUMNS, "silver_weather_daily")
    _log_schema(logger, "silver_weather_daily", df)


def validate_weather_silver_not_null(df: DataFrame) -> None:
    """
    Kiểm tra các cột chính của weather silver không được null.
    """
    null_counts = (
        df.select(
            F.sum(F.col("weather_date").isNull().cast("int")).alias("weather_date_nulls"),
            F.sum(F.col("weather_year").isNull().cast("int")).alias("weather_year_nulls"),
            F.sum(F.col("weather_month").isNull().cast("int")).alias("weather_month_nulls"),
            F.sum(F.col("source_file").isNull().cast("int")).alias("source_file_nulls"),
            F.sum(F.col("bronze_loaded_at").isNull().cast("int")).alias("bronze_loaded_at_nulls"),
            F.sum(F.col("silver_loaded_at").isNull().cast("int")).alias("silver_loaded_at_nulls"),
        )
        .collect()[0]
    )

    for field_name, value in null_counts.asDict().items():
        if value != 0:
            raise ValueError(f"[silver_weather_daily] {field_name} must be 0 but got {value}")


def validate_weather_silver_uniqueness(df: DataFrame) -> None:
    """
    Weather daily phải unique theo ngày.
    """
    validate_unique_key(df, ["weather_date"], "silver_weather_daily")


def validate_weather_silver_partitions(df: DataFrame, silver_path: Path, logger) -> None:
    """
    Kiểm tra partition weather silver.
    """
    partition_rows = (
        df.select("weather_year", "weather_month")
        .distinct()
        .orderBy("weather_year", "weather_month")
        .collect()
    )

    partitions = [(row["weather_year"], row["weather_month"]) for row in partition_rows]
    logger.info("[silver_weather_daily] distinct partitions=%s", partitions)

    if not partitions:
        raise ValueError("[silver_weather_daily] no partitions found in dataframe")

    partition_dirs = sorted(silver_path.rglob("weather_month=*"))
    if not partition_dirs:
        raise ValueError(f"[silver_weather_daily] no partition directories found in {silver_path}")

    logger.info("[silver_weather_daily] partition directory count=%s", len(partition_dirs))


# =========================================================
# 7) ZONE SILVER VALIDATION
# =========================================================
def validate_zone_silver_schema(df: DataFrame, logger) -> None:
    """
    Kiểm tra bộ cột zone silver đúng theo tên cột mong đợi.
    """
    _assert_columns_match_by_name(df, EXPECTED_SILVER_ZONE_COLUMNS, "silver_zone_lookup")
    _log_schema(logger, "silver_zone_lookup", df)


def validate_zone_silver_not_null(df: DataFrame) -> None:
    """
    location_id là khóa lookup chính nên không được null.
    """
    null_counts = (
        df.select(
            F.sum(F.col("location_id").isNull().cast("int")).alias("location_id_nulls"),
            F.sum(F.col("source_file").isNull().cast("int")).alias("source_file_nulls"),
            F.sum(F.col("bronze_loaded_at").isNull().cast("int")).alias("bronze_loaded_at_nulls"),
            F.sum(F.col("silver_loaded_at").isNull().cast("int")).alias("silver_loaded_at_nulls"),
        )
        .collect()[0]
    )

    for field_name, value in null_counts.asDict().items():
        if value != 0:
            raise ValueError(f"[silver_zone_lookup] {field_name} must be 0 but got {value}")


def validate_zone_silver_uniqueness(df: DataFrame) -> None:
    """
    Zone lookup phải unique theo location_id.
    """
    validate_unique_key(df, ["location_id"], "silver_zone_lookup")


# =========================================================
# 8) ENRICHED SILVER VALIDATION
# =========================================================
def validate_enriched_silver_schema(df: DataFrame, logger) -> None:
    """
    Kiểm tra bộ cột taxi enriched đúng theo tên cột mong đợi.
    """
    _assert_columns_match_by_name(
        df,
        EXPECTED_SILVER_TAXI_ENRICHED_COLUMNS,
        "silver_taxi_trips_enriched",
    )
    _log_schema(logger, "silver_taxi_trips_enriched", df)


def validate_enriched_silver_not_null(df: DataFrame) -> None:
    """
    Kiểm tra các cột cốt lõi của enriched không được null.

    Lưu ý:
    - Không ép weather columns phải non-null ở đây,
      vì phần đó sẽ được kiểm bằng join completeness riêng.
    """
    null_counts = (
        df.select(
            F.sum(F.col("trip_id").isNull().cast("int")).alias("trip_id_nulls"),
            F.sum(F.col("pickup_datetime").isNull().cast("int")).alias("pickup_datetime_nulls"),
            F.sum(F.col("dropoff_datetime").isNull().cast("int")).alias("dropoff_datetime_nulls"),
            F.sum(F.col("pickup_date").isNull().cast("int")).alias("pickup_date_nulls"),
            F.sum(F.col("pickup_year").isNull().cast("int")).alias("pickup_year_nulls"),
            F.sum(F.col("pickup_month").isNull().cast("int")).alias("pickup_month_nulls"),
            F.sum(F.col("pickup_location_id").isNull().cast("int")).alias("pickup_location_id_nulls"),
            F.sum(F.col("dropoff_location_id").isNull().cast("int")).alias("dropoff_location_id_nulls"),
            F.sum(F.col("source_file").isNull().cast("int")).alias("source_file_nulls"),
            F.sum(F.col("bronze_loaded_at").isNull().cast("int")).alias("bronze_loaded_at_nulls"),
            F.sum(F.col("silver_loaded_at").isNull().cast("int")).alias("silver_loaded_at_nulls"),
        )
        .collect()[0]
    )

    for field_name, value in null_counts.asDict().items():
        if value != 0:
            raise ValueError(
                f"[silver_taxi_trips_enriched] {field_name} must be 0 but got {value}"
            )


def validate_enriched_silver_uniqueness(df: DataFrame) -> None:
    """
    Bảng enriched vẫn ở grain 1 trip nên trip_id phải unique.
    """
    validate_unique_key(df, ["trip_id"], "silver_taxi_trips_enriched")


def validate_enriched_row_count_matches_taxi(
    taxi_df: DataFrame,
    enriched_df: DataFrame,
    logger,
) -> None:
    """
    Enriched phải giữ nguyên grain trip-level nên row count phải bằng taxi silver.
    """
    validate_exact_row_count(
        left_df=taxi_df,
        right_df=enriched_df,
        left_name="silver_taxi_trips",
        right_name="silver_taxi_trips_enriched",
        logger=logger,
    )


def validate_enriched_join_completeness(df: DataFrame) -> None:
    """
    Kiểm tra độ đầy đủ của join zone + weather trong bảng enriched.

    Rule hiện tại:
    - pickup_zone không được null nếu pickup_location_id có giá trị
    - dropoff_zone không được null nếu dropoff_location_id có giá trị
    - weather_date không được null nếu pickup_date có giá trị

    Lưu ý:
    - Rule này phù hợp với phạm vi dữ liệu hiện tại của project
    - Nếu sau này bạn nạp taxi rộng hơn weather date range,
      có thể đổi check weather thành warning thay vì fail
    """
    missing_pickup_zone_count = (
        df.filter(
            F.col("pickup_location_id").isNotNull() & F.col("pickup_zone").isNull()
        ).count()
    )
    if missing_pickup_zone_count != 0:
        raise ValueError(
            f"[silver_taxi_trips_enriched] missing pickup zone rows: "
            f"{missing_pickup_zone_count}"
        )

    missing_dropoff_zone_count = (
        df.filter(
            F.col("dropoff_location_id").isNotNull() & F.col("dropoff_zone").isNull()
        ).count()
    )
    if missing_dropoff_zone_count != 0:
        raise ValueError(
            f"[silver_taxi_trips_enriched] missing dropoff zone rows: "
            f"{missing_dropoff_zone_count}"
        )

    missing_weather_count = (
        df.filter(
            F.col("pickup_date").isNotNull() & F.col("weather_date").isNull()
        ).count()
    )
    if missing_weather_count != 0:
        raise ValueError(
            f"[silver_taxi_trips_enriched] missing weather rows: {missing_weather_count}"
        )


def validate_enriched_silver_partitions(df: DataFrame, silver_path: Path, logger) -> None:
    """
    Kiểm tra partition enriched silver:
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
    logger.info("[silver_taxi_trips_enriched] distinct partitions=%s", partitions)

    if not partitions:
        raise ValueError("[silver_taxi_trips_enriched] no partitions found in dataframe")

    partition_dirs = sorted(silver_path.rglob("pickup_month=*"))
    if not partition_dirs:
        raise ValueError(
            f"[silver_taxi_trips_enriched] no partition directories found in {silver_path}"
        )

    logger.info(
        "[silver_taxi_trips_enriched] partition directory count=%s",
        len(partition_dirs),
    )


# =========================================================
# 9) MAIN
# =========================================================
def main() -> None:
    """
    Chạy toàn bộ validation silver theo thứ tự:
    - taxi silver
    - weather silver
    - zone silver
    - enriched silver
    """
    config = load_app_config()
    paths = _resolve_paths(config)

    logger = get_logger(
        name="validation_silver",
        log_dir=config["paths"]["log_dir"],
    )

    logger.info("=" * 60)
    logger.info("START SILVER VALIDATION")
    logger.info("=" * 60)
    logger.info("Resolved validation paths: %s", {k: str(v) for k, v in paths.items()})

    spark = build_spark(app_name="validation_silver")

    try:
        # -------------------------------------------------
        # READ BRONZE SOURCES
        # -------------------------------------------------
        logger.info("Reading bronze sources for comparison...")

        taxi_bronze_df = read_parquet_dataset(
            spark=spark,
            dataset_path=paths["bronze_taxi_dir"],
            logger=logger,
            label="taxi_bronze",
        )

        weather_bronze_df = read_parquet_dataset(
            spark=spark,
            dataset_path=paths["bronze_weather_dir"],
            logger=logger,
            label="weather_bronze",
        )

        zone_bronze_df = read_parquet_dataset(
            spark=spark,
            dataset_path=paths["bronze_zone_dir"],
            logger=logger,
            label="zone_bronze",
        )

        # -------------------------------------------------
        # READ SILVER OUTPUTS
        # -------------------------------------------------
        logger.info("Reading silver outputs...")

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

        silver_taxi_enriched_df = read_parquet_dataset(
            spark=spark,
            dataset_path=paths["silver_taxi_enriched_dir"],
            logger=logger,
            label="silver_taxi_trips_enriched",
        )

        # -------------------------------------------------
        # TAXI SILVER VALIDATION
        # -------------------------------------------------
        logger.info("Validating silver_taxi_trips...")

        validate_non_empty(silver_taxi_df, "silver_taxi_trips")
        validate_taxi_silver_schema(silver_taxi_df, logger)
        validate_row_count_relation(
            source_df=taxi_bronze_df,
            target_df=silver_taxi_df,
            source_name="taxi_bronze",
            target_name="silver_taxi_trips",
            logger=logger,
            allow_target_less_than_source=True,
        )
        validate_taxi_silver_not_null(silver_taxi_df)
        validate_taxi_silver_uniqueness(silver_taxi_df)
        validate_taxi_silver_time_logic(silver_taxi_df)
        validate_taxi_silver_derived_columns(silver_taxi_df)
        validate_taxi_silver_partitions(
            df=silver_taxi_df,
            silver_path=paths["silver_taxi_dir"],
            logger=logger,
        )

        logger.info("silver_taxi_trips validation passed.")

        # -------------------------------------------------
        # WEATHER SILVER VALIDATION
        # -------------------------------------------------
        logger.info("Validating silver_weather_daily...")

        validate_non_empty(silver_weather_df, "silver_weather_daily")
        validate_weather_silver_schema(silver_weather_df, logger)
        validate_row_count_relation(
            source_df=weather_bronze_df,
            target_df=silver_weather_df,
            source_name="weather_bronze",
            target_name="silver_weather_daily",
            logger=logger,
            allow_target_less_than_source=True,
        )
        validate_weather_silver_not_null(silver_weather_df)
        validate_weather_silver_uniqueness(silver_weather_df)
        validate_weather_silver_partitions(
            df=silver_weather_df,
            silver_path=paths["silver_weather_dir"],
            logger=logger,
        )

        logger.info("silver_weather_daily validation passed.")

        # -------------------------------------------------
        # ZONE SILVER VALIDATION
        # -------------------------------------------------
        logger.info("Validating silver_zone_lookup...")

        validate_non_empty(silver_zone_df, "silver_zone_lookup")
        validate_zone_silver_schema(silver_zone_df, logger)
        validate_row_count_relation(
            source_df=zone_bronze_df,
            target_df=silver_zone_df,
            source_name="zone_bronze",
            target_name="silver_zone_lookup",
            logger=logger,
            allow_target_less_than_source=True,
        )
        validate_zone_silver_not_null(silver_zone_df)
        validate_zone_silver_uniqueness(silver_zone_df)

        logger.info("silver_zone_lookup validation passed.")

        # -------------------------------------------------
        # ENRICHED SILVER VALIDATION
        # -------------------------------------------------
        logger.info("Validating silver_taxi_trips_enriched...")

        validate_non_empty(silver_taxi_enriched_df, "silver_taxi_trips_enriched")
        validate_enriched_silver_schema(silver_taxi_enriched_df, logger)
        validate_enriched_silver_not_null(silver_taxi_enriched_df)
        validate_enriched_silver_uniqueness(silver_taxi_enriched_df)
        validate_enriched_row_count_matches_taxi(
            taxi_df=silver_taxi_df,
            enriched_df=silver_taxi_enriched_df,
            logger=logger,
        )
        validate_enriched_join_completeness(silver_taxi_enriched_df)
        validate_enriched_silver_partitions(
            df=silver_taxi_enriched_df,
            silver_path=paths["silver_taxi_enriched_dir"],
            logger=logger,
        )

        logger.info("silver_taxi_trips_enriched validation passed.")

        logger.info("=" * 60)
        logger.info("SILVER VALIDATION FINISHED SUCCESSFULLY")
        logger.info("=" * 60)

    finally:
        spark.stop()


# =========================================================
# 10) ENTRYPOINT
# =========================================================
if __name__ == "__main__":
    main()