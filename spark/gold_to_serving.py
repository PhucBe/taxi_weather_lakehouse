from __future__ import annotations

from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession, Window
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
# gold_to_serving.py
# ---------------------------------------------------------
# Mục tiêu:
# - Đọc 4 bảng gold parquet:
#     + dim_date
#     + dim_zone
#     + dim_weather
#     + fact_taxi_trips
#
# - Transform thành 4 bảng serving:
#     + mart_daily_demand
#     + mart_daily_payment_mix
#     + mart_weather_impact
#     + mart_zone_demand
#
# Triết lý của layer serving:
# - serving là lớp presentation / BI-ready
# - metric đã aggregate sẵn cho dashboard
# - query ở BI nhẹ hơn nhiều so với đọc trực tiếp fact
# - business logic tổng hợp được khóa trong pipeline
#
# Lưu ý:
# - Phiên bản này giữ nguyên toàn bộ fact rows để bảo toàn
#   reconciliation với gold
# - Các dòng total_amount âm KHÔNG bị loại bỏ ở đây
# - Thay vào đó, marts có thêm cột negative_total_amount_trip_count
#   để downstream dễ theo dõi adjustment / refund / anomaly
# =========================================================


# =========================================================
# 1) CONSTANTS - BỘ CỘT CHUẨN CHO SERVING
# =========================================================
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

    Input:
    - gold_dim_date_dir
    - gold_dim_zone_dir
    - gold_dim_weather_dir
    - gold_fact_taxi_dir

    Output:
    - serving_mart_daily_demand_dir
    - serving_mart_daily_payment_mix_dir
    - serving_mart_weather_impact_dir
    - serving_mart_zone_demand_dir
    """
    paths_cfg = config["paths"]

    local_data_dir = _as_path(paths_cfg.get("local_data_dir", "data"))

    # Gold input
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

    # Serving output
    serving_dir = _as_path(paths_cfg.get("serving_dir", local_data_dir / "serving"))
    serving_mart_daily_demand_dir = _as_path(
        paths_cfg.get("serving_mart_daily_demand_dir", serving_dir / "mart_daily_demand")
    )
    serving_mart_daily_payment_mix_dir = _as_path(
        paths_cfg.get(
            "serving_mart_daily_payment_mix_dir",
            serving_dir / "mart_daily_payment_mix",
        )
    )
    serving_mart_weather_impact_dir = _as_path(
        paths_cfg.get(
            "serving_mart_weather_impact_dir",
            serving_dir / "mart_weather_impact",
        )
    )
    serving_mart_zone_demand_dir = _as_path(
        paths_cfg.get("serving_mart_zone_demand_dir", serving_dir / "mart_zone_demand")
    )

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


def build_spark(app_name: str = "gold_to_serving") -> SparkSession:
    """
    Tạo SparkSession cho job gold -> serving.

    Các config ở đây đủ dùng cho local prototype:
    - shuffle partitions vừa phải
    - partition overwrite mode dynamic để overwrite partition linh hoạt
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
    Log thông tin cơ bản của DataFrame sau transform:
    - số dòng
    - danh sách cột
    - schema dạng simpleString
    """
    row_count = df.count()
    logger.info("[%s] row_count=%s", dataset_name, row_count)
    logger.info("[%s] columns=%s", dataset_name, ", ".join(df.columns))
    logger.info("[%s] schema=%s", dataset_name, df.schema.simpleString())


def read_parquet_dataset(
    spark: SparkSession,
    input_path: Path,
    label: str,
    logger,
) -> DataFrame:
    """
    Đọc 1 dataset parquet.

    Rule:
    - path phải tồn tại
    - path phải là directory
    """
    if not input_path.exists():
        raise FileNotFoundError(f"[{label}] input path not found: {input_path}")

    if not input_path.is_dir():
        raise ValueError(f"[{label}] input path exists but is not a directory: {input_path}")

    logger.info("[%s] reading parquet from: %s", label, input_path)
    return spark.read.parquet(str(input_path))


def read_all_gold_inputs(
    spark: SparkSession,
    paths: dict[str, Path],
    logger,
) -> dict[str, DataFrame]:
    """
    Đọc toàn bộ input gold và trả về dict DataFrame.
    """
    dim_date_df = read_parquet_dataset(
        spark=spark,
        input_path=paths["gold_dim_date_dir"],
        label="dim_date",
        logger=logger,
    )

    dim_zone_df = read_parquet_dataset(
        spark=spark,
        input_path=paths["gold_dim_zone_dir"],
        label="dim_zone",
        logger=logger,
    )

    dim_weather_df = read_parquet_dataset(
        spark=spark,
        input_path=paths["gold_dim_weather_dir"],
        label="dim_weather",
        logger=logger,
    )

    fact_taxi_df = read_parquet_dataset(
        spark=spark,
        input_path=paths["gold_fact_taxi_dir"],
        label="fact_taxi_trips",
        logger=logger,
    )

    return {
        "dim_date_df": dim_date_df,
        "dim_zone_df": dim_zone_df,
        "dim_weather_df": dim_weather_df,
        "fact_taxi_df": fact_taxi_df,
    }


def payment_type_name_expr(payment_type_col: str = "payment_type_code"):
    """
    Chuẩn hóa payment_type_code thành tên dễ đọc cho serving.

    Mapping này phục vụ presentation/dashboard.
    Nếu sau này bạn dùng seed mapping chính thức, có thể thay bằng join seed.
    """
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


def add_serving_loaded_at(df: DataFrame) -> DataFrame:
    """
    Gắn timestamp kỹ thuật cho layer serving.
    """
    return df.withColumn("serving_loaded_at", F.current_timestamp())


# =========================================================
# 3) BUILD SERVING MARTS
# =========================================================
def build_mart_daily_demand(
    fact_df: DataFrame,
    dim_date_df: DataFrame,
) -> DataFrame:
    """
    Build mart_daily_demand.

    Grain:
    - 1 row = 1 pickup_date

    Mục đích:
    - KPI daily demand / revenue / distance / duration
    - line chart theo ngày
    - card tổng quan theo ngày
    """
    # -----------------------------------------------------
    # 1) Chuẩn bị date dimension để join thêm descriptive attrs
    # -----------------------------------------------------
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

    # -----------------------------------------------------
    # 2) Join fact với dim_date
    # -----------------------------------------------------
    joined_df = f.join(
        d,
        F.col("f.pickup_date") == F.col("d.date_day"),
        how="left",
    )

    # -----------------------------------------------------
    # 3) Aggregate theo ngày
    # -----------------------------------------------------
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


def build_mart_daily_payment_mix(
    fact_df: DataFrame,
) -> DataFrame:
    """
    Build mart_daily_payment_mix.

    Grain:
    - 1 row = 1 pickup_date x 1 payment_type_code

    Mục đích:
    - payment mix theo ngày
    - trip share / revenue share theo payment type
    """
    # -----------------------------------------------------
    # 1) Chuẩn hóa payment_type_code cho serving
    #    - nếu null thì gán -1 = Missing
    # -----------------------------------------------------
    base_df = (
        fact_df
        .withColumn("payment_type_code", F.coalesce(F.col("payment_type_code"), F.lit(-1)))
        .withColumn("payment_type_name", payment_type_name_expr("payment_type_code"))
    )

    # -----------------------------------------------------
    # 2) Aggregate theo ngày x payment_type
    # -----------------------------------------------------
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

    # -----------------------------------------------------
    # 3) Tính share theo từng ngày
    # -----------------------------------------------------
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


def build_mart_weather_impact(
    fact_df: DataFrame,
    dim_date_df: DataFrame,
    dim_weather_df: DataFrame,
) -> DataFrame:
    """
    Build mart_weather_impact.

    Grain:
    - 1 row = 1 pickup_date

    Mục đích:
    - xem tác động thời tiết đến demand / revenue / behavior
    - downstream có thể group tiếp rainy vs non-rainy
    """
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


def build_mart_zone_demand(
    fact_df: DataFrame,
    dim_zone_df: DataFrame,
) -> DataFrame:
    """
    Build mart_zone_demand.

    Grain:
    - 1 row = 1 pickup_date x 1 pickup_location_id

    Mục đích:
    - phục vụ bảng/xếp hạng zone demand theo ngày
    - nền cho map / heatmap / top zones
    """
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


# =========================================================
# 4) WRITE SERVING PARQUET
# =========================================================
def write_parquet(
    df: DataFrame,
    output_path: Path,
    mode: str = "overwrite",
    partition_cols: list[str] | None = None,
) -> None:
    """
    Ghi DataFrame ra parquet.

    mode="overwrite":
    - phù hợp prototype / local pipeline

    partition_cols:
    - daily_demand = ["pickup_year", "pickup_month"]
    - daily_payment_mix = ["pickup_year", "pickup_month"]
    - weather_impact = ["pickup_year", "pickup_month"]
    - zone_demand = ["pickup_year", "pickup_month"]
    """
    _ensure_directory(output_path)

    writer = df.write.mode(mode)

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.parquet(str(output_path))


def write_all_serving_outputs(
    outputs: dict[str, DataFrame],
    paths: dict[str, Path],
    logger,
) -> None:
    """
    Ghi toàn bộ output serving ra parquet.
    """
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


# =========================================================
# 5) MAIN ORCHESTRATION
# =========================================================
def run_gold_to_serving() -> None:
    """
    Hàm orchestration chính chạy toàn bộ flow gold -> serving:
    1) load config
    2) resolve paths
    3) build spark
    4) read gold parquet
    5) build 4 marts serving
    6) log overview các output
    7) write parquet
    8) log thành công
    """
    config = load_app_config()
    paths = _resolve_paths(config)

    logger = get_logger(
        name="spark_gold_to_serving",
        log_dir=config["paths"]["log_dir"],
    )

    logger.info("=" * 60)
    logger.info("START SPARK GOLD -> SERVING")
    logger.info("=" * 60)
    logger.info("Resolved paths: %s", {k: str(v) for k, v in paths.items()})

    spark = build_spark(app_name="spark_gold_to_serving")

    try:
        # -------------------------------------------------
        # READ GOLD INPUTS
        # -------------------------------------------------
        logger.info("Reading gold inputs...")
        gold_inputs = read_all_gold_inputs(
            spark=spark,
            paths=paths,
            logger=logger,
        )

        dim_date_df = gold_inputs["dim_date_df"]
        dim_zone_df = gold_inputs["dim_zone_df"]
        dim_weather_df = gold_inputs["dim_weather_df"]
        fact_taxi_df = gold_inputs["fact_taxi_df"]

        # -------------------------------------------------
        # BUILD SERVING MARTS
        # -------------------------------------------------
        logger.info("Building mart_daily_demand...")
        mart_daily_demand_df = build_mart_daily_demand(
            fact_df=fact_taxi_df,
            dim_date_df=dim_date_df,
        )
        log_dataframe_overview(logger, "mart_daily_demand", mart_daily_demand_df)

        logger.info("Building mart_daily_payment_mix...")
        mart_daily_payment_mix_df = build_mart_daily_payment_mix(
            fact_df=fact_taxi_df,
        )
        log_dataframe_overview(
            logger,
            "mart_daily_payment_mix",
            mart_daily_payment_mix_df,
        )

        logger.info("Building mart_weather_impact...")
        mart_weather_impact_df = build_mart_weather_impact(
            fact_df=fact_taxi_df,
            dim_date_df=dim_date_df,
            dim_weather_df=dim_weather_df,
        )
        log_dataframe_overview(logger, "mart_weather_impact", mart_weather_impact_df)

        logger.info("Building mart_zone_demand...")
        mart_zone_demand_df = build_mart_zone_demand(
            fact_df=fact_taxi_df,
            dim_zone_df=dim_zone_df,
        )
        log_dataframe_overview(logger, "mart_zone_demand", mart_zone_demand_df)

        # -------------------------------------------------
        # WRITE OUTPUTS
        # -------------------------------------------------
        serving_outputs = {
            "mart_daily_demand_df": mart_daily_demand_df,
            "mart_daily_payment_mix_df": mart_daily_payment_mix_df,
            "mart_weather_impact_df": mart_weather_impact_df,
            "mart_zone_demand_df": mart_zone_demand_df,
        }

        write_all_serving_outputs(
            outputs=serving_outputs,
            paths=paths,
            logger=logger,
        )

        logger.info("=" * 60)
        logger.info("SPARK GOLD -> SERVING FINISHED SUCCESSFULLY")
        logger.info("=" * 60)

    finally:
        spark.stop()


# =========================================================
# 6) ENTRYPOINT
# =========================================================
def main() -> None:
    """
    Entry point chuẩn Python.
    """
    run_gold_to_serving()


if __name__ == "__main__":
    main()