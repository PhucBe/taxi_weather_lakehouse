from __future__ import annotations
from pathlib import Path
from typing import Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from src.common.config import load_app_config
from src.common.logger import get_logger


# CONSTANTS - BỘ CỘT CHUẨN CHO GOLD
DIM_DATE_COLUMNS = [
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

DIM_ZONE_COLUMNS = [
    "location_id",
    "borough",
    "zone",
    "service_zone",
    "gold_loaded_at",
]

DIM_WEATHER_COLUMNS = [
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

FACT_TAXI_COLUMNS = [
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


# Ép mọi giá trị path trong config thành pathlib.Path.
def _as_path(value: Any) -> Path:
    if isinstance(value, Path):
        return value
    
    return Path(str(value))


# Tạo folder nếu chưa tồn tại.
def _ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


# Đọc path từ config và trả về 1 dict các đường dẫn đã normalize.
def _resolve_paths(config: dict[str, Any]) -> dict[str, Path]:
    paths_cfg = config["paths"]

    local_data_dir = _as_path(paths_cfg.get("local_data_dir", "data"))

    # Silver input
    silver_dir = _as_path(paths_cfg.get("silver_dir", local_data_dir / "silver"))
    silver_taxi_dir = _as_path(paths_cfg.get("silver_taxi_dir", silver_dir / "taxi_trips"))
    silver_weather_dir = _as_path(paths_cfg.get("silver_weather_dir", silver_dir / "weather_daily"))
    silver_zone_dir = _as_path(paths_cfg.get("silver_zone_dir", silver_dir / "zone_lookup"))

    # Gold output
    gold_dir = _as_path(paths_cfg.get("gold_dir", local_data_dir / "gold"))
    gold_dim_date_dir = _as_path(paths_cfg.get("gold_dim_date_dir", gold_dir / "dim_date"))
    gold_dim_zone_dir = _as_path(paths_cfg.get("gold_dim_zone_dir", gold_dir / "dim_zone"))
    gold_dim_weather_dir = _as_path(paths_cfg.get("gold_dim_weather_dir", gold_dir / "dim_weather"))
    gold_fact_taxi_dir = _as_path(paths_cfg.get("gold_fact_taxi_dir", gold_dir / "fact_taxi_trips"))

    return {
        "silver_dir": silver_dir,
        "silver_taxi_dir": silver_taxi_dir,
        "silver_weather_dir": silver_weather_dir,
        "silver_zone_dir": silver_zone_dir,
        "gold_dir": gold_dir,
        "gold_dim_date_dir": gold_dim_date_dir,
        "gold_dim_zone_dir": gold_dim_zone_dir,
        "gold_dim_weather_dir": gold_dim_weather_dir,
        "gold_fact_taxi_dir": gold_fact_taxi_dir,
    }


# Tạo SparkSession cho job silver -> gold.
def build_spark(app_name: str = "silver_to_gold") -> SparkSession:
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


# Log thông tin cơ bản của DataFrame sau transform: số dòng, danh sách cột, schema dạng simpleString.
def log_dataframe_overview(logger, dataset_name: str, df: DataFrame) -> None:
    row_count = df.count()

    logger.info("[%s] row_count=%s", dataset_name, row_count)
    logger.info("[%s] columns=%s", dataset_name, ", ".join(df.columns))
    logger.info("[%s] schema=%s", dataset_name, df.schema.simpleString())


# 3) Đọc 1 dataset silver parquet.
def read_silver_parquet(
    spark: SparkSession,
    input_path: Path,
    label: str,
    logger,
) -> DataFrame:
    if not input_path.exists():
        raise FileNotFoundError(f"[{label}] input path not found: {input_path}")

    if not input_path.is_dir():
        raise ValueError(f"[{label}] input path exists but is not a directory: {input_path}")

    logger.info("[%s] reading silver parquet from: %s", label, input_path)
    
    return spark.read.parquet(str(input_path))


# Đọc toàn bộ input silver và trả về 1 dict DataFrame.
def read_all_silver_inputs(
    spark: SparkSession,
    paths: dict[str, Path],
    logger,
) -> dict[str, DataFrame]:
    silver_taxi_df = read_silver_parquet(
        spark=spark,
        input_path=paths["silver_taxi_dir"],
        label="silver_taxi_trips",
        logger=logger,
    )

    silver_weather_df = read_silver_parquet(
        spark=spark,
        input_path=paths["silver_weather_dir"],
        label="silver_weather_daily",
        logger=logger,
    )

    silver_zone_df = read_silver_parquet(
        spark=spark,
        input_path=paths["silver_zone_dir"],
        label="silver_zone_lookup",
        logger=logger,
    )

    return {
        "silver_taxi_df": silver_taxi_df,
        "silver_weather_df": silver_weather_df,
        "silver_zone_df": silver_zone_df,
    }


# Build dim_date từ silver_taxi_trips.
def build_dim_date(taxi_df: DataFrame) -> DataFrame:
    df = (
        taxi_df
        .select(F.col("pickup_date").alias("date_day"))
        .filter(F.col("date_day").isNotNull())
        .dropDuplicates(["date_day"])
        .withColumn("year_num", F.year(F.col("date_day")))
        .withColumn("quarter_num", F.quarter(F.col("date_day")))
        .withColumn("month_num", F.month(F.col("date_day")))
        .withColumn("month_name", F.date_format(F.col("date_day"), "MMMM"))
        .withColumn("day_num_in_month", F.dayofmonth(F.col("date_day")))
        .withColumn("day_of_week_num", F.dayofweek(F.col("date_day")))
        .withColumn("day_of_week_name", F.date_format(F.col("date_day"), "EEEE"))
        .withColumn(
            "is_weekend",
            F.col("day_of_week_num").isin([1, 7]),
        )
        .withColumn("gold_loaded_at", F.current_timestamp())
    )

    return df.select(*DIM_DATE_COLUMNS)


# Build dim_zone từ silver_zone_lookup.
def build_dim_zone(zone_df: DataFrame) -> DataFrame:
    df = (
        zone_df
        .select(
            "location_id",
            "borough",
            "zone",
            "service_zone",
        )
        .filter(F.col("location_id").isNotNull())
        .dropDuplicates(["location_id"])
        .withColumn("borough", F.trim(F.col("borough")))
        .withColumn("zone", F.trim(F.col("zone")))
        .withColumn("service_zone", F.trim(F.col("service_zone")))
        .withColumn("gold_loaded_at", F.current_timestamp())
    )

    return df.select(*DIM_ZONE_COLUMNS)


# Build dim_weather từ silver_weather_daily.
def build_dim_weather(weather_df: DataFrame) -> DataFrame:
    df = (
        weather_df
        .select(
            "weather_date",
            "temperature_max",
            "temperature_min",
            "temperature_mean",
            "precipitation_sum",
            "snowfall_sum",
            "is_rainy_day",
            "is_snowy_day",
        )
        .filter(F.col("weather_date").isNotNull())
        .dropDuplicates(["weather_date"])
        .withColumn("gold_loaded_at", F.current_timestamp())
    )

    return df.select(*DIM_WEATHER_COLUMNS)


# Fill 0 cho các cột tiền mang tính phụ phí / thành phần cộng dồn có thể bị null từ upstream.
def fill_optional_amount_columns_for_fact(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("extra_amount", F.coalesce(F.col("extra_amount"), F.lit(0.0)))
        .withColumn("mta_tax_amount", F.coalesce(F.col("mta_tax_amount"), F.lit(0.0)))
        .withColumn("tip_amount", F.coalesce(F.col("tip_amount"), F.lit(0.0)))
        .withColumn("tolls_amount", F.coalesce(F.col("tolls_amount"), F.lit(0.0)))
        .withColumn("improvement_surcharge_amount", F.coalesce(F.col("improvement_surcharge_amount"), F.lit(0.0)))
        .withColumn("congestion_surcharge_amount", F.coalesce(F.col("congestion_surcharge_amount"), F.lit(0.0)))
        .withColumn("airport_fee_amount", F.coalesce(F.col("airport_fee_amount"), F.lit(0.0)))
    )


# Build fact_taxi_trips
def build_fact_taxi_trips(
    taxi_df: DataFrame,
    dim_date_df: DataFrame,
    dim_zone_df: DataFrame,
    dim_weather_df: DataFrame,
) -> DataFrame:
    # 1) Chuẩn bị các projection nhỏ từ dimensions
    dd = dim_date_df.select("date_day").alias("dd")

    pz = dim_zone_df.select(
        F.col("location_id").alias("pickup_zone_key"),
    ).alias("pz")

    dz = dim_zone_df.select(
        F.col("location_id").alias("dropoff_zone_key"),
    ).alias("dz")

    w = dim_weather_df.select("weather_date").alias("w")

    # 2) Alias taxi để select rõ nguồn cột
    t = taxi_df.alias("t")

    # 3) Join taxi với các dimensions
    df = (
        t
        .join(dd, F.col("t.pickup_date") == F.col("dd.date_day"), how="left")
        .join(pz, F.col("t.pickup_location_id") == F.col("pz.pickup_zone_key"), how="left")
        .join(dz, F.col("t.dropoff_location_id") == F.col("dz.dropoff_zone_key"), how="left")
        .join(w, F.col("t.pickup_date") == F.col("w.weather_date"), how="left")
    )

    # 4) Select explicit columns cho fact cuối cùng
    df = df.select(
        F.col("t.trip_id").alias("trip_id"),
        F.col("t.pickup_datetime").alias("pickup_datetime"),
        F.col("t.dropoff_datetime").alias("dropoff_datetime"),
        F.col("t.pickup_date").alias("pickup_date"),
        F.col("t.pickup_year").alias("pickup_year"),
        F.col("t.pickup_month").alias("pickup_month"),
        F.col("t.pickup_hour").alias("pickup_hour"),
        F.col("w.weather_date").alias("weather_date"),
        F.col("t.pickup_location_id").alias("pickup_location_id"),
        F.col("t.dropoff_location_id").alias("dropoff_location_id"),
        F.col("t.vendor_id").alias("vendor_id"),
        F.col("t.rate_code_id").alias("rate_code_id"),
        F.col("t.payment_type_code").alias("payment_type_code"),
        F.col("t.store_and_fwd_flag").alias("store_and_fwd_flag"),
        F.col("t.passenger_count").alias("passenger_count"),
        F.col("t.trip_distance").alias("trip_distance"),
        F.col("t.trip_duration_minutes").alias("trip_duration_minutes"),
        F.col("t.fare_amount").alias("fare_amount"),
        F.col("t.extra_amount").alias("extra_amount"),
        F.col("t.mta_tax_amount").alias("mta_tax_amount"),
        F.col("t.tip_amount").alias("tip_amount"),
        F.col("t.tolls_amount").alias("tolls_amount"),
        F.col("t.improvement_surcharge_amount").alias("improvement_surcharge_amount"),
        F.col("t.congestion_surcharge_amount").alias("congestion_surcharge_amount"),
        F.col("t.airport_fee_amount").alias("airport_fee_amount"),
        F.col("t.total_amount").alias("total_amount"),
    )

    # 5) Chuẩn hóa null ở các khoản phí optional về 0.0
    df = fill_optional_amount_columns_for_fact(df)

    # 6) Drop duplicate nhẹ theo trip_id + thêm metadata gold
    df = (
        df
        .dropDuplicates(["trip_id"])
        .withColumn("gold_loaded_at", F.current_timestamp())
    )

    return df.select(*FACT_TAXI_COLUMNS)


# Ghi DataFrame ra parquet.
def write_parquet(
    df: DataFrame,
    output_path: Path,
    mode: str = "overwrite",
    partition_cols: list[str] | None = None,
) -> None:
    _ensure_directory(output_path)

    writer = df.write.mode(mode)

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.parquet(str(output_path))


# Ghi toàn bộ output gold ra parquet.
def write_all_gold_outputs(
    outputs: dict[str, DataFrame],
    paths: dict[str, Path],
    logger,
) -> None:
    logger.info("Writing dim_date parquet...")

    write_parquet(
        df=outputs["dim_date_df"],
        output_path=paths["gold_dim_date_dir"],
        mode="overwrite",
        partition_cols=None,
    )

    logger.info("dim_date written to: %s", paths["gold_dim_date_dir"])

    logger.info("Writing dim_zone parquet...")

    write_parquet(
        df=outputs["dim_zone_df"],
        output_path=paths["gold_dim_zone_dir"],
        mode="overwrite",
        partition_cols=None,
    )

    logger.info("dim_zone written to: %s", paths["gold_dim_zone_dir"])

    logger.info("Writing dim_weather parquet...")

    write_parquet(
        df=outputs["dim_weather_df"],
        output_path=paths["gold_dim_weather_dir"],
        mode="overwrite",
        partition_cols=None,
    )

    logger.info("dim_weather written to: %s", paths["gold_dim_weather_dir"])

    logger.info("Writing fact_taxi_trips parquet...")

    write_parquet(
        df=outputs["fact_taxi_df"],
        output_path=paths["gold_fact_taxi_dir"],
        mode="overwrite",
        partition_cols=["pickup_year", "pickup_month"],
    )

    logger.info("fact_taxi_trips written to: %s", paths["gold_fact_taxi_dir"])


# Hàm orchestration chính chạy toàn bộ flow silver -> gold
def run_silver_to_gold() -> None:
    config = load_app_config()
    paths = _resolve_paths(config)

    logger = get_logger(
        name="spark_silver_to_gold",
        log_dir=config["paths"]["log_dir"],
    )

    logger.info("=" * 60)
    logger.info("START SPARK SILVER -> GOLD")
    logger.info("=" * 60)
    logger.info("Resolved paths: %s", {k: str(v) for k, v in paths.items()})

    spark = build_spark(app_name="spark_silver_to_gold")

    try:
        # READ SILVER INPUTS
        logger.info("Reading silver inputs...")

        silver_inputs = read_all_silver_inputs(
            spark=spark,
            paths=paths,
            logger=logger,
        )

        silver_taxi_df = silver_inputs["silver_taxi_df"]
        silver_weather_df = silver_inputs["silver_weather_df"]
        silver_zone_df = silver_inputs["silver_zone_df"]

        # BUILD GOLD DIMENSIONS
        logger.info("Building dim_date...")

        dim_date_df = build_dim_date(silver_taxi_df)
        log_dataframe_overview(logger, "dim_date", dim_date_df)

        logger.info("Building dim_zone...")

        dim_zone_df = build_dim_zone(silver_zone_df)
        log_dataframe_overview(logger, "dim_zone", dim_zone_df)

        logger.info("Building dim_weather...")

        dim_weather_df = build_dim_weather(silver_weather_df)
        log_dataframe_overview(logger, "dim_weather", dim_weather_df)

        # BUILD GOLD FACT
        logger.info("Building fact_taxi_trips...")

        fact_taxi_df = build_fact_taxi_trips(
            taxi_df=silver_taxi_df,
            dim_date_df=dim_date_df,
            dim_zone_df=dim_zone_df,
            dim_weather_df=dim_weather_df,
        )
        log_dataframe_overview(logger, "fact_taxi_trips", fact_taxi_df)

        # WRITE OUTPUTS
        gold_outputs = {
            "dim_date_df": dim_date_df,
            "dim_zone_df": dim_zone_df,
            "dim_weather_df": dim_weather_df,
            "fact_taxi_df": fact_taxi_df,
        }

        write_all_gold_outputs(
            outputs=gold_outputs,
            paths=paths,
            logger=logger,
        )

        logger.info("=" * 60)
        logger.info("SPARK SILVER -> GOLD FINISHED SUCCESSFULLY")
        logger.info("=" * 60)

    finally:
        spark.stop()


def main() -> None:
    run_silver_to_gold()


if __name__ == "__main__":
    main()