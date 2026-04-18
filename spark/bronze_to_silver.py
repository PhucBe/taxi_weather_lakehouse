from __future__ import annotations

from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

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
# bronze_to_silver.py
# ---------------------------------------------------------
# Mục tiêu phiên bản mới:
# - Đọc 3 bảng bronze parquet:
#     + taxi bronze
#     + weather bronze
#     + zone bronze
# - Transform thành 3 bảng silver base:
#     + silver_taxi_trips
#     + silver_weather_daily
#     + silver_zone_lookup
# - KHÔNG build silver_taxi_trips_enriched nữa
#
# Lý do:
# - Silver chỉ giữ vai trò cleaned base layer
# - Join taxi + zone + weather sẽ được dời sang Gold
#   để build:
#     + dim_date
#     + dim_zone
#     + dim_weather
#     + fact_taxi_trips
# =========================================================


# =========================================================
# 1) CONSTANTS - BỘ CỘT CHUẨN CHO SILVER
# =========================================================
SILVER_TAXI_COLUMNS = [
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

SILVER_WEATHER_COLUMNS = [
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

SILVER_ZONE_COLUMNS = [
    "location_id",
    "borough",
    "zone",
    "service_zone",
    "source_file",
    "bronze_loaded_at",
    "silver_loaded_at",
]


# =========================================================
# 2) HELPER FUNCTIONS
# =========================================================
def _as_path(value: Any) -> Path:
    """
    Ép mọi giá trị path trong config thành pathlib.Path.
    """
    if isinstance(value, Path):
        return value
    return Path(str(value))


def _ensure_directory(path: Path) -> None:
    """
    Tạo folder nếu chưa tồn tại.
    """
    path.mkdir(parents=True, exist_ok=True)


def _resolve_paths(config: dict[str, Any]) -> dict[str, Path]:
    """
    Đọc path từ config và trả về dict các đường dẫn đã normalize.

    Bản mới chỉ resolve:
    - bronze taxi / weather / zone
    - silver taxi / weather / zone
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

    return {
        "bronze_dir": bronze_dir,
        "bronze_taxi_dir": bronze_taxi_dir,
        "bronze_weather_dir": bronze_weather_dir,
        "bronze_zone_dir": bronze_zone_dir,
        "silver_dir": silver_dir,
        "silver_taxi_dir": silver_taxi_dir,
        "silver_weather_dir": silver_weather_dir,
        "silver_zone_dir": silver_zone_dir,
    }


def build_spark(app_name: str = "bronze_to_silver") -> SparkSession:
    """
    Tạo SparkSession cho job bronze -> silver.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def log_dataframe_overview(logger, dataset_name: str, df: DataFrame) -> None:
    """
    Log thông tin cơ bản của DataFrame sau transform.
    """
    row_count = df.count()
    logger.info("[%s] row_count=%s", dataset_name, row_count)
    logger.info("[%s] columns=%s", dataset_name, ", ".join(df.columns))
    logger.info("[%s] schema=%s", dataset_name, df.schema.simpleString())


# =========================================================
# 3) READ BRONZE PARQUET
# =========================================================
def read_bronze_parquet(
    spark: SparkSession,
    input_path: Path,
    label: str,
    logger,
) -> DataFrame:
    """
    Đọc 1 dataset bronze parquet.
    """
    if not input_path.exists():
        raise FileNotFoundError(f"[{label}] input path not found: {input_path}")

    if not input_path.is_dir():
        raise ValueError(f"[{label}] input path exists but is not a directory: {input_path}")

    logger.info("[%s] reading bronze parquet from: %s", label, input_path)
    return spark.read.parquet(str(input_path))


def read_all_bronze_inputs(
    spark: SparkSession,
    paths: dict[str, Path],
    logger,
) -> dict[str, DataFrame]:
    """
    Đọc toàn bộ input bronze và trả về dict DataFrame.
    """
    taxi_bronze_df = read_bronze_parquet(
        spark=spark,
        input_path=paths["bronze_taxi_dir"],
        label="taxi_bronze",
        logger=logger,
    )

    weather_bronze_df = read_bronze_parquet(
        spark=spark,
        input_path=paths["bronze_weather_dir"],
        label="weather_bronze",
        logger=logger,
    )

    zone_bronze_df = read_bronze_parquet(
        spark=spark,
        input_path=paths["bronze_zone_dir"],
        label="zone_bronze",
        logger=logger,
    )

    return {
        "taxi_bronze_df": taxi_bronze_df,
        "weather_bronze_df": weather_bronze_df,
        "zone_bronze_df": zone_bronze_df,
    }


# =========================================================
# 4) TAXI SILVER - HELPER LOGIC
# =========================================================
def add_taxi_derived_columns(df: DataFrame) -> DataFrame:
    """
    Tạo các cột phát sinh cho taxi silver.
    """
    return (
        df
        .withColumn("pickup_hour", F.hour(F.col("pickup_datetime")))
        .withColumn(
            "trip_duration_minutes",
            (
                F.unix_timestamp(F.col("dropoff_datetime"))
                - F.unix_timestamp(F.col("pickup_datetime"))
            ) / F.lit(60.0),
        )
        .withColumn("pickup_year", F.year(F.col("pickup_date")))
        .withColumn("pickup_month", F.month(F.col("pickup_date")))
    )


def apply_taxi_quality_filters(df: DataFrame) -> DataFrame:
    """
    Áp business rules tối thiểu để taxi từ bronze đi vào silver.
    """
    return (
        df
        .filter(F.col("pickup_datetime").isNotNull())
        .filter(F.col("dropoff_datetime").isNotNull())
        .filter(F.col("pickup_date").isNotNull())
        .filter(F.col("pickup_location_id").isNotNull())
        .filter(F.col("dropoff_location_id").isNotNull())
        .filter(F.col("pickup_datetime") < F.col("dropoff_datetime"))
        .filter(F.col("trip_duration_minutes") > 0)
    )


def apply_project_date_scope(df: DataFrame) -> DataFrame:
    """
    Giới hạn taxi theo phạm vi project hiện tại:
    weather cover 2023-01-01 đến 2023-03-31.
    """
    return df.filter(
        (F.col("pickup_date") >= F.to_date(F.lit("2023-01-01"))) &
        (F.col("pickup_date") <= F.to_date(F.lit("2023-03-31")))
    )


def build_trip_id(df: DataFrame) -> DataFrame:
    """
    Sinh surrogate key trip_id cho taxi silver.
    """
    trip_id_expr = F.concat_ws(
        "||",
        F.coalesce(F.col("vendor_id").cast("string"), F.lit("")),
        F.coalesce(F.col("pickup_datetime").cast("string"), F.lit("")),
        F.coalesce(F.col("dropoff_datetime").cast("string"), F.lit("")),
        F.coalesce(F.col("pickup_location_id").cast("string"), F.lit("")),
        F.coalesce(F.col("dropoff_location_id").cast("string"), F.lit("")),
        F.coalesce(F.col("trip_distance").cast("string"), F.lit("")),
        F.coalesce(F.col("total_amount").cast("string"), F.lit("")),
    )
    return df.withColumn("trip_id", F.sha2(trip_id_expr, 256))


# =========================================================
# 5) TRANSFORM BRONZE -> SILVER BASE
# =========================================================
def transform_taxi_to_silver(df: DataFrame) -> DataFrame:
    """
    Transform taxi bronze -> silver_taxi_trips.
    """
    df = (
        df
        .withColumnRenamed("vendorid", "vendor_id")
        .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
        .withColumnRenamed("ratecodeid", "rate_code_id")
        .withColumnRenamed("pulocationid", "pickup_location_id")
        .withColumnRenamed("dolocationid", "dropoff_location_id")
        .withColumnRenamed("payment_type", "payment_type_code")
        .withColumnRenamed("extra", "extra_amount")
        .withColumnRenamed("mta_tax", "mta_tax_amount")
        .withColumnRenamed("improvement_surcharge", "improvement_surcharge_amount")
        .withColumnRenamed("congestion_surcharge", "congestion_surcharge_amount")
        .withColumnRenamed("airport_fee", "airport_fee_amount")
    )

    df = (
        df
        .withColumn("vendor_id", F.col("vendor_id").cast(T.IntegerType()))
        .withColumn("passenger_count", F.col("passenger_count").cast(T.IntegerType()))
        .withColumn("rate_code_id", F.col("rate_code_id").cast(T.IntegerType()))
        .withColumn("pickup_location_id", F.col("pickup_location_id").cast(T.IntegerType()))
        .withColumn("dropoff_location_id", F.col("dropoff_location_id").cast(T.IntegerType()))
        .withColumn("payment_type_code", F.col("payment_type_code").cast(T.IntegerType()))
        .withColumn("trip_distance", F.col("trip_distance").cast(T.DoubleType()))
        .withColumn("fare_amount", F.col("fare_amount").cast(T.DoubleType()))
        .withColumn("extra_amount", F.col("extra_amount").cast(T.DoubleType()))
        .withColumn("mta_tax_amount", F.col("mta_tax_amount").cast(T.DoubleType()))
        .withColumn("tip_amount", F.col("tip_amount").cast(T.DoubleType()))
        .withColumn("tolls_amount", F.col("tolls_amount").cast(T.DoubleType()))
        .withColumn(
            "improvement_surcharge_amount",
            F.col("improvement_surcharge_amount").cast(T.DoubleType()),
        )
        .withColumn(
            "congestion_surcharge_amount",
            F.col("congestion_surcharge_amount").cast(T.DoubleType()),
        )
        .withColumn("airport_fee_amount", F.col("airport_fee_amount").cast(T.DoubleType()))
        .withColumn("total_amount", F.col("total_amount").cast(T.DoubleType()))
        .withColumn("store_and_fwd_flag", F.trim(F.col("store_and_fwd_flag")))
    )

    df = add_taxi_derived_columns(df)
    df = apply_taxi_quality_filters(df)
    df = apply_project_date_scope(df)
    df = build_trip_id(df)
    df = df.dropDuplicates(["trip_id"])
    df = df.withColumn("silver_loaded_at", F.current_timestamp())

    return df.select(*SILVER_TAXI_COLUMNS)


def transform_weather_to_silver(df: DataFrame) -> DataFrame:
    """
    Transform weather bronze -> silver_weather_daily.
    """
    df = (
        df
        .withColumnRenamed("date", "weather_date")
        .withColumnRenamed("temperature_2m_max", "temperature_max")
        .withColumnRenamed("temperature_2m_min", "temperature_min")
        .withColumnRenamed("temperature_2m_mean", "temperature_mean")
    )

    df = (
        df
        .withColumn("is_rainy_day", F.col("precipitation_sum") > F.lit(0))
        .withColumn("is_snowy_day", F.col("snowfall_sum") > F.lit(0))
    )

    df = df.filter(F.col("weather_date").isNotNull())
    df = df.dropDuplicates(["weather_date"])
    df = df.withColumn("silver_loaded_at", F.current_timestamp())

    return df.select(*SILVER_WEATHER_COLUMNS)


def transform_zone_to_silver(df: DataFrame) -> DataFrame:
    """
    Transform zone bronze -> silver_zone_lookup.
    """
    df = (
        df
        .withColumn("borough", F.trim(F.col("borough")))
        .withColumn("zone", F.trim(F.col("zone")))
        .withColumn("service_zone", F.trim(F.col("service_zone")))
    )

    df = df.filter(F.col("location_id").isNotNull())
    df = df.dropDuplicates(["location_id"])
    df = df.withColumn("silver_loaded_at", F.current_timestamp())

    return df.select(*SILVER_ZONE_COLUMNS)


# =========================================================
# 6) WRITE SILVER PARQUET
# =========================================================
def write_parquet(
    df: DataFrame,
    output_path: Path,
    mode: str = "overwrite",
    partition_cols: list[str] | None = None,
) -> None:
    """
    Ghi DataFrame ra parquet.
    """
    _ensure_directory(output_path)

    writer = df.write.mode(mode)

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.parquet(str(output_path))


def write_all_silver_outputs(
    outputs: dict[str, DataFrame],
    paths: dict[str, Path],
    logger,
) -> None:
    """
    Ghi toàn bộ output silver base ra parquet.
    """
    logger.info("Writing silver_taxi_trips parquet...")
    write_parquet(
        df=outputs["silver_taxi_df"],
        output_path=paths["silver_taxi_dir"],
        mode="overwrite",
        partition_cols=["pickup_year", "pickup_month"],
    )
    logger.info("silver_taxi_trips written to: %s", paths["silver_taxi_dir"])

    logger.info("Writing silver_weather_daily parquet...")
    write_parquet(
        df=outputs["silver_weather_df"],
        output_path=paths["silver_weather_dir"],
        mode="overwrite",
        partition_cols=["weather_year", "weather_month"],
    )
    logger.info("silver_weather_daily written to: %s", paths["silver_weather_dir"])

    logger.info("Writing silver_zone_lookup parquet...")
    write_parquet(
        df=outputs["silver_zone_df"],
        output_path=paths["silver_zone_dir"],
        mode="overwrite",
        partition_cols=None,
    )
    logger.info("silver_zone_lookup written to: %s", paths["silver_zone_dir"])


# =========================================================
# 7) MAIN ORCHESTRATION
# =========================================================
def run_bronze_to_silver() -> None:
    """
    Flow bronze -> silver:
    1) load config
    2) resolve paths
    3) build spark
    4) read bronze parquet
    5) transform -> 3 silver base tables
    6) log overview các output
    7) write parquet
    """
    config = load_app_config()
    paths = _resolve_paths(config)

    logger = get_logger(
        name="spark_bronze_to_silver",
        log_dir=config["paths"]["log_dir"],
    )

    logger.info("=" * 60)
    logger.info("START SPARK BRONZE -> SILVER")
    logger.info("=" * 60)
    logger.info("Resolved paths: %s", {k: str(v) for k, v in paths.items()})

    spark = build_spark(app_name="spark_bronze_to_silver")

    try:
        logger.info("Reading bronze inputs...")
        bronze_inputs = read_all_bronze_inputs(
            spark=spark,
            paths=paths,
            logger=logger,
        )

        taxi_bronze_df = bronze_inputs["taxi_bronze_df"]
        weather_bronze_df = bronze_inputs["weather_bronze_df"]
        zone_bronze_df = bronze_inputs["zone_bronze_df"]

        logger.info("Transforming taxi bronze -> silver_taxi_trips...")
        silver_taxi_df = transform_taxi_to_silver(taxi_bronze_df)
        log_dataframe_overview(logger, "silver_taxi_trips", silver_taxi_df)

        logger.info("Transforming weather bronze -> silver_weather_daily...")
        silver_weather_df = transform_weather_to_silver(weather_bronze_df)
        log_dataframe_overview(logger, "silver_weather_daily", silver_weather_df)

        logger.info("Transforming zone bronze -> silver_zone_lookup...")
        silver_zone_df = transform_zone_to_silver(zone_bronze_df)
        log_dataframe_overview(logger, "silver_zone_lookup", silver_zone_df)

        silver_outputs = {
            "silver_taxi_df": silver_taxi_df,
            "silver_weather_df": silver_weather_df,
            "silver_zone_df": silver_zone_df,
        }

        write_all_silver_outputs(
            outputs=silver_outputs,
            paths=paths,
            logger=logger,
        )

        logger.info("=" * 60)
        logger.info("SPARK BRONZE -> SILVER FINISHED SUCCESSFULLY")
        logger.info("=" * 60)

    finally:
        spark.stop()


# =========================================================
# 8) ENTRYPOINT
# =========================================================
def main() -> None:
    run_bronze_to_silver()


if __name__ == "__main__":
    main()