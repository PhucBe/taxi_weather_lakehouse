from __future__ import annotations
import glob
from pathlib import Path
from typing import Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import load_app_config
from src.common.logger import get_logger


# CONSTANTS - BỘ CỘT CHUẨN CỦA BRONZE
EXPECTED_TAXI_BRONZE_COLUMNS = [
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
    "pickup_date",
    "pickup_year",
    "pickup_month",
    "source_file",
    "bronze_loaded_at",
]

EXPECTED_WEATHER_BRONZE_COLUMNS = [
    "date",
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "snowfall_sum",
    "weather_year",
    "weather_month",
    "source_file",
    "bronze_loaded_at",
]

EXPECTED_ZONE_BRONZE_COLUMNS = [
    "location_id",
    "borough",
    "zone",
    "service_zone",
    "source_file",
    "bronze_loaded_at",
]


# Chuyển mọi giá trị path thành Path object.
def _as_path(value: Any) -> Path:
    if isinstance(value, Path):
        return value
    
    return Path(str(value))


# Resolve toàn bộ path cần dùng cho validation.
def _resolve_paths(config: dict[str, Any]) -> dict[str, Path]:
    paths_cfg = config["paths"]
    taxi_dir = _as_path(paths_cfg["taxi_dir"])
    weather_dir = _as_path(paths_cfg["weather_dir"])
    zone_dir = _as_path(paths_cfg["zone_dir"])
    local_data_dir = _as_path(paths_cfg.get("local_data_dir", "data"))
    bronze_dir = _as_path(paths_cfg.get("bronze_dir", local_data_dir / "bronze"))
    bronze_taxi_dir = _as_path(paths_cfg.get("bronze_taxi_dir", bronze_dir / "taxi_trips_raw"))
    bronze_weather_dir = _as_path(paths_cfg.get("bronze_weather_dir", bronze_dir / "weather_raw"))
    bronze_zone_dir = _as_path(paths_cfg.get("bronze_zone_dir", bronze_dir / "zone_lookup"))

    return {
        "taxi_raw_glob": taxi_dir / "flat" / "**" / "*.csv",
        "weather_raw_glob": weather_dir / "flat" / "**" / "*.csv",
        "zone_raw_glob": zone_dir / "**" / "*.csv",
        "bronze_taxi_dir": bronze_taxi_dir,
        "bronze_weather_dir": bronze_weather_dir,
        "bronze_zone_dir": bronze_zone_dir,
    }


# Đảm bảo folder tồn tại và đúng là directory.
def _require_existing_dir(path: Path, label: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"{label} directory not found: {path}")
    
    if not path.is_dir():
        raise ValueError(f"{label} exists but is not a directory: {path}")


# Kiểm tra glob pattern có match ra file hay không.
def _require_glob_matches(pattern: str, label: str) -> list[str]:
    matches = sorted(glob.glob(pattern, recursive=True))

    if not matches:
        raise FileNotFoundError(f"No {label} files found for pattern: {pattern}")
    
    return matches


# Liệt kê toàn bộ file *.parquet bên trong 1 folder.
def _list_parquet_files(path: Path) -> list[Path]:
    return sorted(path.rglob("*.parquet"))


# Đảm bảo folder bronze có file parquet thật.
def _assert_has_parquet_files(path: Path, label: str) -> list[Path]:
    parquet_files = _list_parquet_files(path)

    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {label}: {path}")
    
    return parquet_files


# Kiểm tra DataFrame có đủ đúng các cột mong đợi hay không.
def _assert_columns_match_by_name(
    df: DataFrame,
    expected_columns: list[str],
    dataset_name: str,
) -> None:
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


# Log schema ngắn gọn của DataFrame.
def _log_schema(logger, dataset_name: str, df: DataFrame) -> None:
    logger.info("[%s] columns=%s", dataset_name, ", ".join(df.columns))
    logger.info("[%s] schema=%s", dataset_name, df.schema.simpleString())


# Đếm số dòng của DataFrame.
def _count_rows(df: DataFrame) -> int:
    return df.count()


# Tạo SparkSession cho job validation bronze.
def build_spark(app_name: str = "validation_bronze") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    return spark


# Đọc raw CSV chỉ để phục vụ count đối chiếu.
def read_raw_csv_files(spark: SparkSession, input_glob: str, logger, label: str) -> DataFrame:
    matched_files = _require_glob_matches(input_glob, label)

    logger.info("[%s] matched raw files=%s", label, matched_files)

    return (
        spark.read
        .option("header", True)
        .csv(matched_files)
    )


# Đọc bronze parquet.
def read_bronze_parquet(spark: SparkSession, bronze_path: Path, logger, label: str) -> DataFrame:
    _require_existing_dir(bronze_path, label)
    parquet_files = _assert_has_parquet_files(bronze_path, label)

    logger.info("[%s] bronze path=%s", label, bronze_path)
    logger.info("[%s] parquet file count=%s", label, len(parquet_files))

    return spark.read.parquet(str(bronze_path))


# Dataset bronze không được rỗng.
def validate_non_empty(df: DataFrame, dataset_name: str) -> None:
    row_count = _count_rows(df)

    if row_count == 0:
        raise ValueError(f"[{dataset_name}] bronze dataset is empty")


# So sánh row count raw và bronze.
def validate_row_count_relation(
    raw_df: DataFrame,
    bronze_df: DataFrame,
    dataset_name: str,
    logger,
    allow_bronze_less_than_raw: bool = False,
) -> None:
    raw_count = _count_rows(raw_df)
    bronze_count = _count_rows(bronze_df)

    logger.info("[%s] raw_count=%s", dataset_name, raw_count)
    logger.info("[%s] bronze_count=%s", dataset_name, bronze_count)

    if bronze_count > raw_count:
        raise ValueError(
            f"[{dataset_name}] bronze_count > raw_count | raw={raw_count}, bronze={bronze_count}"
        )

    if bronze_count < raw_count:
        if allow_bronze_less_than_raw:
            logger.warning(
                "[%s] bronze_count < raw_count | raw=%s, bronze=%s, diff=%s",
                dataset_name,
                raw_count,
                bronze_count,
                raw_count - bronze_count,
            )
        else:
            raise ValueError(
                f"[{dataset_name}] bronze_count < raw_count | raw={raw_count}, bronze={bronze_count}"
            )


# Kiểm tra bộ cột taxi bronze đúng theo tên cột mong đợi. Không ép đúng thứ tự cột.
def validate_taxi_bronze_schema(df: DataFrame, logger) -> None:
    _assert_columns_match_by_name(df, EXPECTED_TAXI_BRONZE_COLUMNS, "taxi_bronze")
    _log_schema(logger, "taxi_bronze", df)


# Kiểm tra các cột kỹ thuật quan trọng của taxi bronze không được null.
def validate_taxi_bronze_not_null(df: DataFrame) -> None:
    null_counts = (
        df.select(
            F.sum(F.col("pickup_date").isNull().cast("int")).alias("pickup_date_nulls"),
            F.sum(F.col("pickup_year").isNull().cast("int")).alias("pickup_year_nulls"),
            F.sum(F.col("pickup_month").isNull().cast("int")).alias("pickup_month_nulls"),
            F.sum(F.col("source_file").isNull().cast("int")).alias("source_file_nulls"),
            F.sum(F.col("bronze_loaded_at").isNull().cast("int")).alias("bronze_loaded_at_nulls"),
        )
        .collect()[0]
    )

    for field_name, value in null_counts.asDict().items():
        if value != 0:
            raise ValueError(f"[taxi_bronze] {field_name} must be 0 but got {value}")


# Kiểm tra partition taxi bronze:
# - có partition trong dữ liệu
# - có partition directory trên disk
def validate_taxi_bronze_partitions(df: DataFrame, bronze_path: Path, logger) -> None:
    partition_rows = (
        df.select("pickup_year", "pickup_month")
        .distinct()
        .orderBy("pickup_year", "pickup_month")
        .collect()
    )

    partitions = [(row["pickup_year"], row["pickup_month"]) for row in partition_rows]
    
    logger.info("[taxi_bronze] distinct partitions=%s", partitions)

    if not partitions:
        raise ValueError("[taxi_bronze] no partitions found in dataframe")

    partition_dirs = sorted(bronze_path.rglob("pickup_month=*"))

    if not partition_dirs:
        raise ValueError(f"[taxi_bronze] no partition directories found in {bronze_path}")

    logger.info("[taxi_bronze] partition directory count=%s", len(partition_dirs))


# Kiểm tra bộ cột weather bronze đúng theo tên cột mong đợi.
def validate_weather_bronze_schema(df: DataFrame, logger) -> None:
    _assert_columns_match_by_name(df, EXPECTED_WEATHER_BRONZE_COLUMNS, "weather_bronze")
    _log_schema(logger, "weather_bronze", df)


# Kiểm tra các cột chính của weather bronze không được null.
def validate_weather_bronze_not_null(df: DataFrame) -> None:
    null_counts = (
        df.select(
            F.sum(F.col("date").isNull().cast("int")).alias("date_nulls"),
            F.sum(F.col("weather_year").isNull().cast("int")).alias("weather_year_nulls"),
            F.sum(F.col("weather_month").isNull().cast("int")).alias("weather_month_nulls"),
            F.sum(F.col("source_file").isNull().cast("int")).alias("source_file_nulls"),
            F.sum(F.col("bronze_loaded_at").isNull().cast("int")).alias("bronze_loaded_at_nulls"),
        )
        .collect()[0]
    )

    for field_name, value in null_counts.asDict().items():
        if value != 0:
            raise ValueError(f"[weather_bronze] {field_name} must be 0 but got {value}")


# Weather daily không nên duplicate theo date.
def validate_weather_bronze_date_uniqueness(df: DataFrame) -> None:
    duplicate_count = (
        df.groupBy("date")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicate_count != 0:
        raise ValueError(f"[weather_bronze] duplicated dates found: {duplicate_count}")


# Kiểm tra partition weather bronze.
def validate_weather_bronze_partitions(df: DataFrame, bronze_path: Path, logger) -> None:
    partition_rows = (
        df.select("weather_year", "weather_month")
        .distinct()
        .orderBy("weather_year", "weather_month")
        .collect()
    )

    partitions = [(row["weather_year"], row["weather_month"]) for row in partition_rows]
    
    logger.info("[weather_bronze] distinct partitions=%s", partitions)

    if not partitions:
        raise ValueError("[weather_bronze] no partitions found in dataframe")

    partition_dirs = sorted(bronze_path.rglob("weather_month=*"))

    if not partition_dirs:
        raise ValueError(f"[weather_bronze] no partition directories found in {bronze_path}")

    logger.info("[weather_bronze] partition directory count=%s", len(partition_dirs))


# Kiểm tra bộ cột zone bronze đúng theo tên cột mong đợi.
def validate_zone_bronze_schema(df: DataFrame, logger) -> None:
    _assert_columns_match_by_name(df, EXPECTED_ZONE_BRONZE_COLUMNS, "zone_bronze")
    _log_schema(logger, "zone_bronze", df)


# location_id là khóa lookup chính nên không được null.
def validate_zone_bronze_not_null(df: DataFrame) -> None:
    null_counts = (
        df.select(
            F.sum(F.col("location_id").isNull().cast("int")).alias("location_id_nulls"),
            F.sum(F.col("source_file").isNull().cast("int")).alias("source_file_nulls"),
            F.sum(F.col("bronze_loaded_at").isNull().cast("int")).alias("bronze_loaded_at_nulls"),
        )
        .collect()[0]
    )

    for field_name, value in null_counts.asDict().items():
        if value != 0:
            raise ValueError(f"[zone_bronze] {field_name} must be 0 but got {value}")


# Kiểm tra duplicate của zone bronze theo natural key.
def validate_zone_bronze_duplicates(df: DataFrame) -> None:
    duplicate_count = (
        df.groupBy("location_id", "borough", "zone", "service_zone")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicate_count != 0:
        raise ValueError(f"[zone_bronze] duplicates found: {duplicate_count}")


# Chạy toàn bộ validation bronze theo thứ tự.
def main() -> None:
    config = load_app_config()
    paths = _resolve_paths(config)

    logger = get_logger(
        name="validation_bronze",
        log_dir=config["paths"]["log_dir"],
    )

    logger.info("=" * 60)
    logger.info("START BRONZE VALIDATION")
    logger.info("=" * 60)

    logger.info("Resolved validation paths: %s", {k: str(v) for k, v in paths.items()})

    spark = build_spark(app_name="validation_bronze")

    try:
        # Taxi
        logger.info("Validating taxi bronze...")

        taxi_raw_df = read_raw_csv_files(
            spark=spark,
            input_glob=str(paths["taxi_raw_glob"]),
            logger=logger,
            label="taxi_raw",
        )

        taxi_bronze_df = read_bronze_parquet(
            spark=spark,
            bronze_path=paths["bronze_taxi_dir"],
            logger=logger,
            label="taxi_bronze",
        )

        validate_non_empty(taxi_bronze_df, "taxi_bronze")
        validate_taxi_bronze_schema(taxi_bronze_df, logger)
        validate_row_count_relation(
            raw_df=taxi_raw_df,
            bronze_df=taxi_bronze_df,
            dataset_name="taxi_bronze",
            logger=logger,
            allow_bronze_less_than_raw=True,
        )
        validate_taxi_bronze_not_null(taxi_bronze_df)
        validate_taxi_bronze_partitions(
            df=taxi_bronze_df,
            bronze_path=paths["bronze_taxi_dir"],
            logger=logger,
        )

        logger.info("Taxi bronze validation passed.")

        # Weather
        logger.info("Validating weather bronze...")

        weather_raw_df = read_raw_csv_files(
            spark=spark,
            input_glob=str(paths["weather_raw_glob"]),
            logger=logger,
            label="weather_raw",
        )

        weather_bronze_df = read_bronze_parquet(
            spark=spark,
            bronze_path=paths["bronze_weather_dir"],
            logger=logger,
            label="weather_bronze",
        )

        validate_non_empty(weather_bronze_df, "weather_bronze")
        validate_weather_bronze_schema(weather_bronze_df, logger)
        validate_row_count_relation(
            raw_df=weather_raw_df,
            bronze_df=weather_bronze_df,
            dataset_name="weather_bronze",
            logger=logger,
            allow_bronze_less_than_raw=False,
        )
        validate_weather_bronze_not_null(weather_bronze_df)
        validate_weather_bronze_date_uniqueness(weather_bronze_df)
        validate_weather_bronze_partitions(
            df=weather_bronze_df,
            bronze_path=paths["bronze_weather_dir"],
            logger=logger,
        )

        logger.info("Weather bronze validation passed.")

        # Zone
        logger.info("Validating zone bronze...")

        zone_raw_df = read_raw_csv_files(
            spark=spark,
            input_glob=str(paths["zone_raw_glob"]),
            logger=logger,
            label="zone_raw",
        )

        zone_bronze_df = read_bronze_parquet(
            spark=spark,
            bronze_path=paths["bronze_zone_dir"],
            logger=logger,
            label="zone_bronze",
        )

        validate_non_empty(zone_bronze_df, "zone_bronze")
        validate_zone_bronze_schema(zone_bronze_df, logger)
        validate_row_count_relation(
            raw_df=zone_raw_df,
            bronze_df=zone_bronze_df,
            dataset_name="zone_bronze",
            logger=logger,
            allow_bronze_less_than_raw=False,
        )
        validate_zone_bronze_not_null(zone_bronze_df)
        validate_zone_bronze_duplicates(zone_bronze_df)

        logger.info("Zone bronze validation passed.")

        logger.info("=" * 60)
        logger.info("BRONZE VALIDATION FINISHED SUCCESSFULLY")
        logger.info("=" * 60)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()