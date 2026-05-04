from __future__ import annotations
from pathlib import Path
from typing import Any
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from src.common.config import load_app_config
from src.common.logger import get_logger


# CONSTANTS - BỘ CỘT CHUẨN CHO SERVING
MART_DAILY_DEMAND_COLUMNS = [
    "pickup_date",
    "pickup_year",
    "pickup_month",
    "quarter_num",
    "month_name",
    "day_num_in_month",
    "day_of_week_num",
    "day_of_week_name",
    "is_weekend",
    "trip_count",
    "total_passenger_count",
    "total_trip_distance",
    "total_trip_duration_minutes",
    "total_revenue",
    "total_fare_amount",
    "total_tip_amount",
    "total_tolls_amount",
    "avg_trip_distance",
    "avg_trip_duration_minutes",
    "avg_total_amount",
    "avg_fare_amount",
    "avg_tip_amount",
    "negative_total_amount_trip_count",
    "serving_loaded_at",
]

MART_DAILY_PAYMENT_MIX_COLUMNS = [
    "pickup_date",
    "pickup_year",
    "pickup_month",
    "payment_type_code",
    "payment_type_name",
    "trip_count",
    "total_revenue",
    "total_fare_amount",
    "total_tip_amount",
    "avg_total_amount",
    "avg_fare_amount",
    "avg_tip_amount",
    "payment_trip_share_pct",
    "payment_revenue_share_pct",
    "serving_loaded_at",
]

MART_WEATHER_IMPACT_COLUMNS = [
    "pickup_date",
    "pickup_year",
    "pickup_month",
    "day_of_week_num",
    "day_of_week_name",
    "is_weekend",
    "weather_date",
    "is_rainy_day",
    "is_snowy_day",
    "temperature_max",
    "temperature_min",
    "temperature_mean",
    "precipitation_sum",
    "snowfall_sum",
    "trip_count",
    "total_revenue",
    "total_fare_amount",
    "total_tip_amount",
    "avg_total_amount",
    "avg_trip_distance",
    "avg_trip_duration_minutes",
    "negative_total_amount_trip_count",
    "serving_loaded_at",
]

MART_ZONE_DEMAND_COLUMNS = [
    "pickup_date",
    "pickup_year",
    "pickup_month",
    "pickup_location_id",
    "borough",
    "zone",
    "service_zone",
    "trip_count",
    "total_revenue",
    "total_fare_amount",
    "total_tip_amount",
    "avg_total_amount",
    "avg_trip_distance",
    "avg_trip_duration_minutes",
    "negative_total_amount_trip_count",
    "serving_loaded_at",
]


# Ép mọi giá trị path trong config thành pathlib.Path.
def _as_path(value: Any) -> Path:
    if isinstance(value, Path):
        return value
    
    return Path(str(value))


# Tạo folder nếu chưa tồn tại.
def _ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


# Đọc path từ config và trả về dict các đường dẫn đã normalize.
def _resolve_paths(config: dict[str, Any]) -> dict[str, Path]:
    paths_cfg = config["paths"]

    local_data_dir = _as_path(paths_cfg.get("local_data_dir", "data"))

    gold_dir = _as_path(paths_cfg.get("gold_dir", local_data_dir / "gold"))
    gold_dim_date_dir = _as_path(paths_cfg.get("gold_dim_date_dir", gold_dir / "dim_date"))
    gold_dim_zone_dir = _as_path(paths_cfg.get("gold_dim_zone_dir", gold_dir / "dim_zone"))
    gold_dim_weather_dir = _as_path(paths_cfg.get("gold_dim_weather_dir", gold_dir / "dim_weather"))
    gold_fact_taxi_dir = _as_path(paths_cfg.get("gold_fact_taxi_dir", gold_dir / "fact_taxi_trips"))

    serving_dir = _as_path(paths_cfg.get("serving_dir", local_data_dir / "serving"))
    serving_mart_daily_demand_dir = _as_path(paths_cfg.get("serving_mart_daily_demand_dir", serving_dir / "mart_daily_demand"))
    serving_mart_daily_payment_mix_dir = _as_path(paths_cfg.get("serving_mart_daily_payment_mix_dir", serving_dir / "mart_daily_payment_mix"))
    serving_mart_weather_impact_dir = _as_path(paths_cfg.get("serving_mart_weather_impact_dir", serving_dir / "mart_weather_impact"))
    serving_mart_zone_demand_dir = _as_path(paths_cfg.get("serving_mart_zone_demand_dir", serving_dir / "mart_zone_demand"))

    return {
        "gold_dir": gold_dir,
        "gold_dim_date_dir": gold_dim_date_dir,
        "gold_dim_zone_dir": gold_dim_zone_dir,
        "gold_dim_weather_dir": gold_dim_weather_dir,
        "gold_fact_taxi_dir": gold_fact_taxi_dir,
        "serving_dir": serving_dir,
        "serving_mart_daily_demand_dir": serving_mart_daily_demand_dir,
        "serving_mart_daily_payment_mix_dir": serving_mart_daily_payment_mix_dir,
        "serving_mart_weather_impact_dir": serving_mart_weather_impact_dir,
        "serving_mart_zone_demand_dir": serving_mart_zone_demand_dir,
    }


# Tạo SparkSession cho job gold -> serving.
def build_spark(app_name: str = "gold_to_serving") -> SparkSession:
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


# Log thông tin cơ bản của DataFrame sau transform: số dòng, danh sách cột, schema dạng simpleString
def log_dataframe_overview(logger, dataset_name: str, df: DataFrame) -> None:
    row_count = df.count()
    logger.info("[%s] row_count=%s", dataset_name, row_count)
    logger.info("[%s] columns=%s", dataset_name, ", ".join(df.columns))
    logger.info("[%s] schema=%s", dataset_name, df.schema.simpleString())


# Đọc 1 dataset parquet.
def read_parquet_dataset(spark: SparkSession, input_path: Path, label: str, logger) -> DataFrame:
    if not input_path.exists():
        raise FileNotFoundError(f"[{label}] input path not found: {input_path}")

    if not input_path.is_dir():
        raise ValueError(f"[{label}] input path exists but is not a directory: {input_path}")

    logger.info("[%s] reading parquet from: %s", label, input_path)

    return spark.read.parquet(str(input_path))


# Đọc toàn bộ input gold và trả về dict DataFrame.
def read_all_gold_inputs(spark: SparkSession, paths: dict[str, Path], logger) -> dict[str, DataFrame]:
    dim_date_df = read_parquet_dataset(spark=spark, input_path=paths["gold_dim_date_dir"], label="dim_date", logger=logger)
    dim_zone_df = read_parquet_dataset(spark=spark, input_path=paths["gold_dim_zone_dir"], label="dim_zone", logger=logger)
    dim_weather_df = read_parquet_dataset(spark=spark, input_path=paths["gold_dim_weather_dir"], label="dim_weather", logger=logger)
    fact_taxi_df = read_parquet_dataset(spark=spark, input_path=paths["gold_fact_taxi_dir"], label="fact_taxi_trips", logger=logger)

    return {
        "dim_date_df": dim_date_df,
        "dim_zone_df": dim_zone_df,
        "dim_weather_df": dim_weather_df,
        "fact_taxi_df": fact_taxi_df,
    }


# Chuẩn hóa payment_type_code thành tên dễ đọc cho serving.
def payment_type_name_expr(payment_type_col: str = "payment_type_code"):
    col = F.col(payment_type_col)

    return (
        F.when(col.isNull(), F.lit("Missing"))
        .when(col == F.lit(-1), F.lit("Missing"))
        .when(col == F.lit(0), F.lit("Unknown"))
        .when(col == F.lit(1), F.lit("Credit card"))
        .when(col == F.lit(2), F.lit("Cash"))
        .when(col == F.lit(3), F.lit("No charge"))
        .when(col == F.lit(4), F.lit("Dispute"))
        .when(col == F.lit(5), F.lit("Unknown"))
        .when(col == F.lit(6), F.lit("Voided trip"))
        .otherwise(F.lit("Other"))
    )


# Gắn timestamp kỹ thuật cho layer serving.
def add_serving_loaded_at(df: DataFrame) -> DataFrame:
    return df.withColumn("serving_loaded_at", F.current_timestamp())


# Build mart_daily_demand.
def build_mart_daily_demand(fact_df: DataFrame, dim_date_df: DataFrame) -> DataFrame:
    # 1) Chuẩn bị date dimension để join thêm descriptive attrs
    d = dim_date_df.select(
        "date_day",
        "quarter_num",
        "month_name",
        "day_num_in_month",
        "day_of_week_num",
        "day_of_week_name",
        "is_weekend",
    ).alias("d")

    f = fact_df.alias("f")

    # 2) Join fact với dim_date
    joined_df = f.join(
        d,
        F.col("f.pickup_date") == F.col("d.date_day"),
        how="left",
    )

    # 3) Aggregate theo ngày
    agg_df = (
        joined_df
        .groupBy(
            F.col("f.pickup_date").alias("pickup_date"),
            F.col("f.pickup_year").alias("pickup_year"),
            F.col("f.pickup_month").alias("pickup_month"),
            F.col("d.quarter_num").alias("quarter_num"),
            F.col("d.month_name").alias("month_name"),
            F.col("d.day_num_in_month").alias("day_num_in_month"),
            F.col("d.day_of_week_num").alias("day_of_week_num"),
            F.col("d.day_of_week_name").alias("day_of_week_name"),
            F.col("d.is_weekend").alias("is_weekend"),
        )
        .agg(
            F.count(F.lit(1)).alias("trip_count"),
            F.sum("f.passenger_count").alias("total_passenger_count"),
            F.sum("f.trip_distance").alias("total_trip_distance"),
            F.sum("f.trip_duration_minutes").alias("total_trip_duration_minutes"),
            F.sum("f.total_amount").alias("total_revenue"),
            F.sum("f.fare_amount").alias("total_fare_amount"),
            F.sum("f.tip_amount").alias("total_tip_amount"),
            F.sum("f.tolls_amount").alias("total_tolls_amount"),
            F.avg("f.trip_distance").alias("avg_trip_distance"),
            F.avg("f.trip_duration_minutes").alias("avg_trip_duration_minutes"),
            F.avg("f.total_amount").alias("avg_total_amount"),
            F.avg("f.fare_amount").alias("avg_fare_amount"),
            F.avg("f.tip_amount").alias("avg_tip_amount"),
            F.sum(
                F.when(F.col("f.total_amount") < 0, F.lit(1)).otherwise(F.lit(0))
            ).alias("negative_total_amount_trip_count"),
        )
    )

    agg_df = add_serving_loaded_at(agg_df)

    return agg_df.select(*MART_DAILY_DEMAND_COLUMNS)


# Build mart_daily_payment_mix.
def build_mart_daily_payment_mix(fact_df: DataFrame) -> DataFrame:
    # 1) Chuẩn hóa payment_type_code cho serving
    base_df = (
        fact_df
        .withColumn("payment_type_code", F.coalesce(F.col("payment_type_code"), F.lit(-1)))
        .withColumn("payment_type_name", payment_type_name_expr("payment_type_code"))
    )

    # 2) Aggregate theo ngày x payment_type
    agg_df = (
        base_df
        .groupBy(
            "pickup_date",
            "pickup_year",
            "pickup_month",
            "payment_type_code",
            "payment_type_name",
        )
        .agg(
            F.count(F.lit(1)).alias("trip_count"),
            F.sum("total_amount").alias("total_revenue"),
            F.sum("fare_amount").alias("total_fare_amount"),
            F.sum("tip_amount").alias("total_tip_amount"),
            F.avg("total_amount").alias("avg_total_amount"),
            F.avg("fare_amount").alias("avg_fare_amount"),
            F.avg("tip_amount").alias("avg_tip_amount"),
        )
    )

    # 3) Tính share theo từng ngày
    day_window = Window.partitionBy("pickup_date")

    agg_df = (
        agg_df
        .withColumn("daily_trip_count_total", F.sum("trip_count").over(day_window))
        .withColumn("daily_revenue_total", F.sum("total_revenue").over(day_window))
        .withColumn(
            "payment_trip_share_pct",
            F.when(
                F.col("daily_trip_count_total") > 0,
                F.col("trip_count") / F.col("daily_trip_count_total"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "payment_revenue_share_pct",
            F.when(
                F.col("daily_revenue_total") != 0,
                F.col("total_revenue") / F.col("daily_revenue_total"),
            ).otherwise(F.lit(0.0)),
        )
        .drop("daily_trip_count_total", "daily_revenue_total")
    )

    agg_df = add_serving_loaded_at(agg_df)

    return agg_df.select(*MART_DAILY_PAYMENT_MIX_COLUMNS)


# Build mart_weather_impact.
def build_mart_weather_impact(fact_df: DataFrame, dim_date_df: DataFrame, dim_weather_df: DataFrame) -> DataFrame:
    d = dim_date_df.select(
        "date_day",
        "day_of_week_num",
        "day_of_week_name",
        "is_weekend",
    ).alias("d")

    w = dim_weather_df.select(
        "weather_date",
        "is_rainy_day",
        "is_snowy_day",
        "temperature_max",
        "temperature_min",
        "temperature_mean",
        "precipitation_sum",
        "snowfall_sum",
    ).alias("w")

    f = fact_df.alias("f")

    joined_df = (
        f
        .join(d, F.col("f.pickup_date") == F.col("d.date_day"), how="left")
        .join(w, F.col("f.weather_date") == F.col("w.weather_date"), how="left")
    )

    agg_df = (
        joined_df
        .groupBy(
            F.col("f.pickup_date").alias("pickup_date"),
            F.col("f.pickup_year").alias("pickup_year"),
            F.col("f.pickup_month").alias("pickup_month"),
            F.col("d.day_of_week_num").alias("day_of_week_num"),
            F.col("d.day_of_week_name").alias("day_of_week_name"),
            F.col("d.is_weekend").alias("is_weekend"),
            F.col("w.weather_date").alias("weather_date"),
            F.col("w.is_rainy_day").alias("is_rainy_day"),
            F.col("w.is_snowy_day").alias("is_snowy_day"),
            F.col("w.temperature_max").alias("temperature_max"),
            F.col("w.temperature_min").alias("temperature_min"),
            F.col("w.temperature_mean").alias("temperature_mean"),
            F.col("w.precipitation_sum").alias("precipitation_sum"),
            F.col("w.snowfall_sum").alias("snowfall_sum"),
        )
        .agg(
            F.count(F.lit(1)).alias("trip_count"),
            F.sum("f.total_amount").alias("total_revenue"),
            F.sum("f.fare_amount").alias("total_fare_amount"),
            F.sum("f.tip_amount").alias("total_tip_amount"),
            F.avg("f.total_amount").alias("avg_total_amount"),
            F.avg("f.trip_distance").alias("avg_trip_distance"),
            F.avg("f.trip_duration_minutes").alias("avg_trip_duration_minutes"),
            F.sum(
                F.when(F.col("f.total_amount") < 0, F.lit(1)).otherwise(F.lit(0))
            ).alias("negative_total_amount_trip_count"),
        )
    )

    agg_df = add_serving_loaded_at(agg_df)

    return agg_df.select(*MART_WEATHER_IMPACT_COLUMNS)


# Build mart_zone_demand.
def build_mart_zone_demand(fact_df: DataFrame, dim_zone_df: DataFrame) -> DataFrame:
    z = dim_zone_df.select(
        "location_id",
        "borough",
        "zone",
        "service_zone",
    ).alias("z")

    f = fact_df.alias("f")

    joined_df = f.join(
        z,
        F.col("f.pickup_location_id") == F.col("z.location_id"),
        how="left",
    )

    agg_df = (
        joined_df
        .groupBy(
            F.col("f.pickup_date").alias("pickup_date"),
            F.col("f.pickup_year").alias("pickup_year"),
            F.col("f.pickup_month").alias("pickup_month"),
            F.col("f.pickup_location_id").alias("pickup_location_id"),
            F.col("z.borough").alias("borough"),
            F.col("z.zone").alias("zone"),
            F.col("z.service_zone").alias("service_zone"),
        )
        .agg(
            F.count(F.lit(1)).alias("trip_count"),
            F.sum("f.total_amount").alias("total_revenue"),
            F.sum("f.fare_amount").alias("total_fare_amount"),
            F.sum("f.tip_amount").alias("total_tip_amount"),
            F.avg("f.total_amount").alias("avg_total_amount"),
            F.avg("f.trip_distance").alias("avg_trip_distance"),
            F.avg("f.trip_duration_minutes").alias("avg_trip_duration_minutes"),
            F.sum(
                F.when(F.col("f.total_amount") < 0, F.lit(1)).otherwise(F.lit(0))
            ).alias("negative_total_amount_trip_count"),
        )
    )

    agg_df = add_serving_loaded_at(agg_df)

    return agg_df.select(*MART_ZONE_DEMAND_COLUMNS)


# Ghi DataFrame ra parquet.
def write_parquet(df: DataFrame, output_path: Path, mode: str = "overwrite", partition_cols: list[str] | None = None) -> None:
    _ensure_directory(output_path)

    writer = df.write.mode(mode)

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.parquet(str(output_path))


# Ghi toàn bộ output serving ra parquet.
def write_all_serving_outputs(outputs: dict[str, DataFrame], paths: dict[str, Path], logger) -> None:
    logger.info("Writing mart_daily_demand parquet...")
    write_parquet(
        df=outputs["mart_daily_demand_df"],
        output_path=paths["serving_mart_daily_demand_dir"],
        mode="overwrite",
        partition_cols=["pickup_year", "pickup_month"],
    )
    logger.info(
        "mart_daily_demand written to: %s",
        paths["serving_mart_daily_demand_dir"],
    )

    logger.info("Writing mart_daily_payment_mix parquet...")
    write_parquet(
        df=outputs["mart_daily_payment_mix_df"],
        output_path=paths["serving_mart_daily_payment_mix_dir"],
        mode="overwrite",
        partition_cols=["pickup_year", "pickup_month"],
    )
    logger.info(
        "mart_daily_payment_mix written to: %s",
        paths["serving_mart_daily_payment_mix_dir"],
    )

    logger.info("Writing mart_weather_impact parquet...")
    write_parquet(
        df=outputs["mart_weather_impact_df"],
        output_path=paths["serving_mart_weather_impact_dir"],
        mode="overwrite",
        partition_cols=["pickup_year", "pickup_month"],
    )
    logger.info(
        "mart_weather_impact written to: %s",
        paths["serving_mart_weather_impact_dir"],
    )

    logger.info("Writing mart_zone_demand parquet...")
    write_parquet(
        df=outputs["mart_zone_demand_df"],
        output_path=paths["serving_mart_zone_demand_dir"],
        mode="overwrite",
        partition_cols=["pickup_year", "pickup_month"],
    )
    logger.info(
        "mart_zone_demand written to: %s",
        paths["serving_mart_zone_demand_dir"],
    )


# Hàm orchestration chính chạy toàn bộ flow gold -> serving
def run_gold_to_serving() -> None:
    config = load_app_config()
    paths = _resolve_paths(config)
    logger = get_logger(name="spark_gold_to_serving", log_dir=config["paths"]["log_dir"])

    logger.info("=" * 60)
    logger.info("START SPARK GOLD -> SERVING")
    logger.info("=" * 60)
    logger.info("Resolved paths: %s", {k: str(v) for k, v in paths.items()})

    spark = build_spark(app_name="spark_gold_to_serving")

    try:
        # READ GOLD INPUTS
        logger.info("Reading gold inputs...")
        gold_inputs = read_all_gold_inputs(spark=spark, paths=paths, logger=logger)
        dim_date_df = gold_inputs["dim_date_df"]
        dim_zone_df = gold_inputs["dim_zone_df"]
        dim_weather_df = gold_inputs["dim_weather_df"]
        fact_taxi_df = gold_inputs["fact_taxi_df"]

        # BUILD SERVING MARTS
        logger.info("Building mart_daily_demand...")
        mart_daily_demand_df = build_mart_daily_demand(fact_df=fact_taxi_df, dim_date_df=dim_date_df)
        log_dataframe_overview(logger, "mart_daily_demand", mart_daily_demand_df)

        logger.info("Building mart_daily_payment_mix...")
        mart_daily_payment_mix_df = build_mart_daily_payment_mix(fact_df=fact_taxi_df)
        log_dataframe_overview(logger, "mart_daily_payment_mix", mart_daily_payment_mix_df)

        logger.info("Building mart_weather_impact...")
        mart_weather_impact_df = build_mart_weather_impact(fact_df=fact_taxi_df, dim_date_df=dim_date_df, dim_weather_df=dim_weather_df)
        log_dataframe_overview(logger, "mart_weather_impact", mart_weather_impact_df)

        logger.info("Building mart_zone_demand...")
        mart_zone_demand_df = build_mart_zone_demand(fact_df=fact_taxi_df, dim_zone_df=dim_zone_df)
        log_dataframe_overview(logger, "mart_zone_demand", mart_zone_demand_df)

        # WRITE OUTPUTS
        serving_outputs = {
            "mart_daily_demand_df": mart_daily_demand_df,
            "mart_daily_payment_mix_df": mart_daily_payment_mix_df,
            "mart_weather_impact_df": mart_weather_impact_df,
            "mart_zone_demand_df": mart_zone_demand_df,
        }
        write_all_serving_outputs(outputs=serving_outputs, paths=paths, logger=logger)

        logger.info("=" * 60)
        logger.info("SPARK GOLD -> SERVING FINISHED SUCCESSFULLY")
        logger.info("=" * 60)

    finally:
        spark.stop()


def main() -> None:
    run_gold_to_serving()


if __name__ == "__main__":
    main()