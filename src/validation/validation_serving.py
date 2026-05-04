from __future__ import annotations
from pathlib import Path
from typing import Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from src.common.config import load_app_config
from src.common.logger import get_logger


# CONSTANTS - BỘ CỘT CHUẨN CỦA SERVING
EXPECTED_MART_DAILY_DEMAND_COLUMNS = [
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

EXPECTED_MART_DAILY_PAYMENT_MIX_COLUMNS = [
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

EXPECTED_MART_WEATHER_IMPACT_COLUMNS = [
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

EXPECTED_MART_ZONE_DEMAND_COLUMNS = [
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

FLOAT_TOLERANCE = 0.01
SHARE_TOLERANCE = 0.0001


# Chuyển mọi giá trị path thành Path object.
def _as_path(value: Any) -> Path:
    if isinstance(value, Path):
        return value
    
    return Path(str(value))


# Resolve toàn bộ path cần dùng cho validation serving.
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
        "gold_dim_date_dir": gold_dim_date_dir,
        "gold_dim_zone_dir": gold_dim_zone_dir,
        "gold_dim_weather_dir": gold_dim_weather_dir,
        "gold_fact_taxi_dir": gold_fact_taxi_dir,
        "serving_mart_daily_demand_dir": serving_mart_daily_demand_dir,
        "serving_mart_daily_payment_mix_dir": serving_mart_daily_payment_mix_dir,
        "serving_mart_weather_impact_dir": serving_mart_weather_impact_dir,
        "serving_mart_zone_demand_dir": serving_mart_zone_demand_dir,
    }


# Đảm bảo folder tồn tại và đúng là directory.
def _require_existing_dir(path: Path, label: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"{label} directory not found: {path}")
    
    if not path.is_dir():
        raise ValueError(f"{label} exists but is not a directory: {path}")


# Liệt kê toàn bộ file *.parquet bên trong 1 folder.
def _list_parquet_files(path: Path) -> list[Path]:
    return sorted(path.rglob("*.parquet"))


# Đảm bảo folder có file parquet thật.
def _assert_has_parquet_files(path: Path, label: str) -> list[Path]:
    parquet_files = _list_parquet_files(path)

    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {label}: {path}")
    
    return parquet_files


# Kiểm tra DataFrame có đủ đúng các cột mong đợi hay không.
def _assert_columns_match_by_name(df: DataFrame, expected_columns: list[str], dataset_name: str) -> None:
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

# Tính tổng một cột numeric. Nếu toàn bộ null thì trả về 0.0.
def _sum_column(df: DataFrame, column_name: str) -> float:
    value = df.select(F.sum(F.col(column_name)).alias("v")).collect()[0]["v"]

    return float(value) if value is not None else 0.0


# Tạo SparkSession cho job validation serving.
def build_spark(app_name: str = "validation_serving") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    return spark


# Đọc parquet dataset từ folder.
def read_parquet_dataset(spark: SparkSession, dataset_path: Path, logger, label: str) -> DataFrame:
    _require_existing_dir(dataset_path, label)
    parquet_files = _assert_has_parquet_files(dataset_path, label)

    logger.info("[%s] path=%s", label, dataset_path)
    logger.info("[%s] parquet file count=%s", label, len(parquet_files))

    return spark.read.parquet(str(dataset_path))


# Dataset serving không được rỗng.
def validate_non_empty(df: DataFrame, dataset_name: str) -> None:
    row_count = _count_rows(df)

    if row_count == 0:
        raise ValueError(f"[{dataset_name}] serving dataset is empty")


# Kiểm tra key phải unique.
def validate_unique_key(df: DataFrame, key_columns: list[str], dataset_name: str) -> None:
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


"""
Kiểm tra partition của dataset serving:
- có partition trong dữ liệu
- có partition directory trên disk
"""
def validate_partition_columns(df: DataFrame, dataset_name: str, output_path: Path, year_col: str = "pickup_year", month_col: str = "pickup_month", month_dir_name: str = "pickup_month=*", logger=None) -> None:
    partition_rows = (
        df.select(year_col, month_col)
        .distinct()
        .orderBy(year_col, month_col)
        .collect()
    )

    partitions = [(row[year_col], row[month_col]) for row in partition_rows]

    if logger is not None:
        logger.info("[%s] distinct partitions=%s", dataset_name, partitions)

    if not partitions:
        raise ValueError(f"[{dataset_name}] no partitions found in dataframe")

    partition_dirs = sorted(output_path.rglob(month_dir_name))

    if not partition_dirs:
        raise ValueError(f"[{dataset_name}] no partition directories found in {output_path}")

    if logger is not None:
        logger.info("[%s] partition directory count=%s", dataset_name, len(partition_dirs))


# So sánh 2 giá trị float với tolerance.
def validate_float_close(left_value: float, right_value: float, label: str, tolerance: float = FLOAT_TOLERANCE) -> None:
    if abs(left_value - right_value) > tolerance:
        raise ValueError(
            f"{label} mismatch | left={left_value}, right={right_value}, "
            f"diff={abs(left_value - right_value)}"
        )

# Chuẩn hóa payment_type_code thành payment_type_name theo đúng logic của gold_to_serving.py.
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


# Kiểm tra bộ cột mart_daily_demand đúng theo tên cột mong đợi.
def validate_mart_daily_demand_schema(df: DataFrame, logger) -> None:
    _assert_columns_match_by_name(df, EXPECTED_MART_DAILY_DEMAND_COLUMNS, "mart_daily_demand")
    _log_schema(logger, "mart_daily_demand", df)


# Kiểm tra các cột cốt lõi của mart_daily_demand không được null.
def validate_mart_daily_demand_not_null(df: DataFrame) -> None:
    null_counts = (
        df.select(
            F.sum(F.col("pickup_date").isNull().cast("int")).alias("pickup_date_nulls"),
            F.sum(F.col("pickup_year").isNull().cast("int")).alias("pickup_year_nulls"),
            F.sum(F.col("pickup_month").isNull().cast("int")).alias("pickup_month_nulls"),
            F.sum(F.col("quarter_num").isNull().cast("int")).alias("quarter_num_nulls"),
            F.sum(F.col("month_name").isNull().cast("int")).alias("month_name_nulls"),
            F.sum(F.col("day_num_in_month").isNull().cast("int")).alias("day_num_in_month_nulls"),
            F.sum(F.col("day_of_week_num").isNull().cast("int")).alias("day_of_week_num_nulls"),
            F.sum(F.col("day_of_week_name").isNull().cast("int")).alias("day_of_week_name_nulls"),
            F.sum(F.col("is_weekend").isNull().cast("int")).alias("is_weekend_nulls"),
            F.sum(F.col("trip_count").isNull().cast("int")).alias("trip_count_nulls"),
            F.sum(F.col("total_revenue").isNull().cast("int")).alias("total_revenue_nulls"),
            F.sum(F.col("avg_total_amount").isNull().cast("int")).alias("avg_total_amount_nulls"),
            F.sum(F.col("negative_total_amount_trip_count").isNull().cast("int")).alias("negative_total_amount_trip_count_nulls"),
            F.sum(F.col("serving_loaded_at").isNull().cast("int")).alias("serving_loaded_at_nulls"),
        ).collect()[0]
    )

    for field_name, value in null_counts.asDict().items():
        if value != 0:
            raise ValueError(f"[mart_daily_demand] {field_name} must be 0 but got {value}")


# mart_daily_demand có grain 1 row = 1 pickup_date.
def validate_mart_daily_demand_uniqueness(df: DataFrame) -> None:
    validate_unique_key(df, ["pickup_date"], "mart_daily_demand")


# Kiểm tra các date descriptors trong mart_daily_demand khớp với dim_date.
def validate_mart_daily_demand_date_consistency(mart_df: DataFrame, dim_date_df: DataFrame) -> None:
    d = dim_date_df.select(
        "date_day",
        "quarter_num",
        "month_name",
        "day_num_in_month",
        "day_of_week_num",
        "day_of_week_name",
        "is_weekend",
    ).alias("d")

    m = mart_df.alias("m")

    mismatch_count = (
        m
        .join(d, F.col("m.pickup_date") == F.col("d.date_day"), how="left")
        .filter(
            (F.col("d.date_day").isNull()) |
            (F.col("m.quarter_num") != F.col("d.quarter_num")) |
            (F.col("m.month_name") != F.col("d.month_name")) |
            (F.col("m.day_num_in_month") != F.col("d.day_num_in_month")) |
            (F.col("m.day_of_week_num") != F.col("d.day_of_week_num")) |
            (F.col("m.day_of_week_name") != F.col("d.day_of_week_name")) |
            (F.col("m.is_weekend") != F.col("d.is_weekend"))
        )
        .count()
    )

    if mismatch_count != 0:
        raise ValueError(
            f"[mart_daily_demand] date descriptors mismatch vs dim_date: {mismatch_count}"
        )


# Kiểm tra reconciliation của mart_daily_demand với fact_taxi_trips.
def validate_mart_daily_demand_reconciliation(mart_df: DataFrame, fact_df: DataFrame, logger) -> None:
    expected_row_count = fact_df.select("pickup_date").distinct().count()
    actual_row_count = mart_df.count()

    logger.info("[mart_daily_demand] expected distinct pickup_date count=%s", expected_row_count)
    logger.info("[mart_daily_demand] actual row_count=%s", actual_row_count)

    if actual_row_count != expected_row_count:
        raise ValueError(
            f"[mart_daily_demand] row_count != distinct pickup_date count from fact_taxi_trips | "
            f"expected={expected_row_count}, actual={actual_row_count}"
        )

    mart_trip_count_sum = _sum_column(mart_df, "trip_count")
    fact_row_count = float(fact_df.count())
    validate_float_close(mart_trip_count_sum, fact_row_count, "[mart_daily_demand] trip_count sum vs fact row_count", tolerance=0.0)

    mart_total_revenue_sum = _sum_column(mart_df, "total_revenue")
    fact_total_amount_sum = _sum_column(fact_df, "total_amount")
    validate_float_close(mart_total_revenue_sum, fact_total_amount_sum, "[mart_daily_demand] total_revenue sum vs fact total_amount sum")

    mart_negative_trip_count_sum = _sum_column(mart_df, "negative_total_amount_trip_count")
    fact_negative_trip_count = float(fact_df.filter(F.col("total_amount") < 0).count())
    validate_float_close(mart_negative_trip_count_sum, fact_negative_trip_count, "[mart_daily_demand] negative_total_amount_trip_count sum vs fact negative rows", tolerance=0.0)


# Kiểm tra bộ cột mart_daily_payment_mix đúng theo tên cột mong đợi.
def validate_mart_daily_payment_mix_schema(df: DataFrame, logger) -> None:
    _assert_columns_match_by_name(
        df,
        EXPECTED_MART_DAILY_PAYMENT_MIX_COLUMNS,
        "mart_daily_payment_mix",
    )
    _log_schema(logger, "mart_daily_payment_mix", df)


# Kiểm tra các cột cốt lõi của mart_daily_payment_mix không được null.
def validate_mart_daily_payment_mix_not_null(df: DataFrame) -> None:
    null_counts = (
        df.select(
            F.sum(F.col("pickup_date").isNull().cast("int")).alias("pickup_date_nulls"),
            F.sum(F.col("pickup_year").isNull().cast("int")).alias("pickup_year_nulls"),
            F.sum(F.col("pickup_month").isNull().cast("int")).alias("pickup_month_nulls"),
            F.sum(F.col("payment_type_code").isNull().cast("int")).alias("payment_type_code_nulls"),
            F.sum(F.col("payment_type_name").isNull().cast("int")).alias("payment_type_name_nulls"),
            F.sum(F.col("trip_count").isNull().cast("int")).alias("trip_count_nulls"),
            F.sum(F.col("total_revenue").isNull().cast("int")).alias("total_revenue_nulls"),
            F.sum(F.col("payment_trip_share_pct").isNull().cast("int")).alias("payment_trip_share_pct_nulls"),
            F.sum(F.col("payment_revenue_share_pct").isNull().cast("int")).alias("payment_revenue_share_pct_nulls"),
            F.sum(F.col("serving_loaded_at").isNull().cast("int")).alias("serving_loaded_at_nulls"),
        ).collect()[0]
    )

    for field_name, value in null_counts.asDict().items():
        if value != 0:
            raise ValueError(
                f"[mart_daily_payment_mix] {field_name} must be 0 but got {value}"
            )


# mart_daily_payment_mix có grain 1 row = 1 pickup_date x 1 payment_type_code.
def validate_mart_daily_payment_mix_uniqueness(df: DataFrame) -> None:
    validate_unique_key(
        df,
        ["pickup_date", "payment_type_code"],
        "mart_daily_payment_mix",
    )


# Kiểm tra payment_type_name khớp với payment_type_code theo cùng logic mapping của gold_to_serving.py.
def validate_mart_daily_payment_mix_payment_type_name(df: DataFrame) -> None:
    mismatch_count = (
        df.withColumn("expected_payment_type_name", payment_type_name_expr("payment_type_code"))
        .filter(F.col("payment_type_name") != F.col("expected_payment_type_name"))
        .count()
    )

    if mismatch_count != 0:
        raise ValueError(
            f"[mart_daily_payment_mix] payment_type_name mismatch found: {mismatch_count}"
        )


# Kiểm tra share logic của mart_daily_payment_mix.
def validate_mart_daily_payment_mix_shares(df: DataFrame, logger) -> None:
    invalid_trip_share_range_count = (
        df.filter(
            (F.col("payment_trip_share_pct") < 0 - SHARE_TOLERANCE) |
            (F.col("payment_trip_share_pct") > 1 + SHARE_TOLERANCE)
        ).count()
    )

    if invalid_trip_share_range_count != 0:
        raise ValueError(
            f"[mart_daily_payment_mix] payment_trip_share_pct خارج [0,1] found: "
            f"{invalid_trip_share_range_count}"
        )

    day_trip_share_bad_count = (
        df.groupBy("pickup_date")
        .agg(F.sum("payment_trip_share_pct").alias("trip_share_sum"))
        .filter(F.abs(F.col("trip_share_sum") - F.lit(1.0)) > F.lit(SHARE_TOLERANCE))
        .count()
    )

    if day_trip_share_bad_count != 0:
        raise ValueError(
            f"[mart_daily_payment_mix] payment_trip_share_pct daily sum != 1 found: "
            f"{day_trip_share_bad_count}"
        )

    day_revenue_share_bad_count = (
        df.groupBy("pickup_date")
        .agg(
            F.sum("payment_revenue_share_pct").alias("revenue_share_sum"),
            F.sum("total_revenue").alias("daily_revenue_sum"),
        )
        .filter(
            (F.abs(F.col("daily_revenue_sum")) > F.lit(FLOAT_TOLERANCE)) &
            (F.abs(F.col("revenue_share_sum") - F.lit(1.0)) > F.lit(SHARE_TOLERANCE))
        )
        .count()
    )

    if day_revenue_share_bad_count != 0:
        raise ValueError(
            f"[mart_daily_payment_mix] payment_revenue_share_pct daily sum != 1 found: "
            f"{day_revenue_share_bad_count}"
        )

    revenue_share_out_of_range_count = (
        df.filter(
            (F.col("payment_revenue_share_pct") < 0 - SHARE_TOLERANCE) |
            (F.col("payment_revenue_share_pct") > 1 + SHARE_TOLERANCE)
        ).count()
    )

    if revenue_share_out_of_range_count != 0:
        logger.warning(
            "[mart_daily_payment_mix] payment_revenue_share_pct outside [0,1] rows=%s",
            revenue_share_out_of_range_count,
        )


# Kiểm tra reconciliation của mart_daily_payment_mix với fact_taxi_trips.
def validate_mart_daily_payment_mix_reconciliation(mart_df: DataFrame, fact_df: DataFrame, logger) -> None:
    expected_row_count = (
        fact_df
        .withColumn("payment_type_code_norm", F.coalesce(F.col("payment_type_code"), F.lit(-1)))
        .select("pickup_date", "payment_type_code_norm")
        .distinct()
        .count()
    )
    actual_row_count = mart_df.count()

    logger.info("[mart_daily_payment_mix] expected distinct pickup_date x payment_type count=%s", expected_row_count)
    logger.info("[mart_daily_payment_mix] actual row_count=%s", actual_row_count)

    if actual_row_count != expected_row_count:
        raise ValueError(
            f"[mart_daily_payment_mix] row_count != distinct pickup_date x payment_type count "
            f"from fact_taxi_trips | expected={expected_row_count}, actual={actual_row_count}"
        )

    mart_trip_count_sum = _sum_column(mart_df, "trip_count")
    fact_row_count = float(fact_df.count())
    validate_float_close(mart_trip_count_sum, fact_row_count, "[mart_daily_payment_mix] trip_count sum vs fact row_count", tolerance=0.0)

    mart_total_revenue_sum = _sum_column(mart_df, "total_revenue")
    fact_total_amount_sum = _sum_column(fact_df, "total_amount")
    validate_float_close(mart_total_revenue_sum, fact_total_amount_sum, "[mart_daily_payment_mix] total_revenue sum vs fact total_amount sum")


# Kiểm tra bộ cột mart_weather_impact đúng theo tên cột mong đợi.
def validate_mart_weather_impact_schema(df: DataFrame, logger) -> None:
    _assert_columns_match_by_name(
        df,
        EXPECTED_MART_WEATHER_IMPACT_COLUMNS,
        "mart_weather_impact",
    )
    _log_schema(logger, "mart_weather_impact", df)


# Kiểm tra các cột cốt lõi của mart_weather_impact không được null.
def validate_mart_weather_impact_not_null(df: DataFrame) -> None:
    null_counts = (
        df.select(
            F.sum(F.col("pickup_date").isNull().cast("int")).alias("pickup_date_nulls"),
            F.sum(F.col("pickup_year").isNull().cast("int")).alias("pickup_year_nulls"),
            F.sum(F.col("pickup_month").isNull().cast("int")).alias("pickup_month_nulls"),
            F.sum(F.col("day_of_week_num").isNull().cast("int")).alias("day_of_week_num_nulls"),
            F.sum(F.col("day_of_week_name").isNull().cast("int")).alias("day_of_week_name_nulls"),
            F.sum(F.col("is_weekend").isNull().cast("int")).alias("is_weekend_nulls"),
            F.sum(F.col("weather_date").isNull().cast("int")).alias("weather_date_nulls"),
            F.sum(F.col("is_rainy_day").isNull().cast("int")).alias("is_rainy_day_nulls"),
            F.sum(F.col("is_snowy_day").isNull().cast("int")).alias("is_snowy_day_nulls"),
            F.sum(F.col("temperature_mean").isNull().cast("int")).alias("temperature_mean_nulls"),
            F.sum(F.col("precipitation_sum").isNull().cast("int")).alias("precipitation_sum_nulls"),
            F.sum(F.col("trip_count").isNull().cast("int")).alias("trip_count_nulls"),
            F.sum(F.col("total_revenue").isNull().cast("int")).alias("total_revenue_nulls"),
            F.sum(F.col("negative_total_amount_trip_count").isNull().cast("int")).alias("negative_total_amount_trip_count_nulls"),
            F.sum(F.col("serving_loaded_at").isNull().cast("int")).alias("serving_loaded_at_nulls"),
        ).collect()[0]
    )

    for field_name, value in null_counts.asDict().items():
        if value != 0:
            raise ValueError(
                f"[mart_weather_impact] {field_name} must be 0 but got {value}"
            )


# mart_weather_impact có grain 1 row = 1 pickup_date.
def validate_mart_weather_impact_uniqueness(df: DataFrame) -> None:
    validate_unique_key(df, ["pickup_date"], "mart_weather_impact")


# Kiểm tra weather/date descriptors trong mart_weather_impact khớp với dim_date và dim_weather.
def validate_mart_weather_impact_consistency(mart_df: DataFrame, dim_date_df: DataFrame, dim_weather_df: DataFrame) -> None:
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

    m = mart_df.alias("m")

    mismatch_count = (
        m
        .join(d, F.col("m.pickup_date") == F.col("d.date_day"), how="left")
        .join(w, F.col("m.weather_date") == F.col("w.weather_date"), how="left")
        .filter(
            (F.col("d.date_day").isNull()) |
            (F.col("w.weather_date").isNull()) |
            (F.col("m.weather_date") != F.col("m.pickup_date")) |
            (F.col("m.day_of_week_num") != F.col("d.day_of_week_num")) |
            (F.col("m.day_of_week_name") != F.col("d.day_of_week_name")) |
            (F.col("m.is_weekend") != F.col("d.is_weekend")) |
            (F.col("m.is_rainy_day") != F.col("w.is_rainy_day")) |
            (F.col("m.is_snowy_day") != F.col("w.is_snowy_day")) |
            (F.col("m.temperature_max") != F.col("w.temperature_max")) |
            (F.col("m.temperature_min") != F.col("w.temperature_min")) |
            (F.col("m.temperature_mean") != F.col("w.temperature_mean")) |
            (F.col("m.precipitation_sum") != F.col("w.precipitation_sum")) |
            (F.col("m.snowfall_sum") != F.col("w.snowfall_sum"))
        )
        .count()
    )

    if mismatch_count != 0:
        raise ValueError(
            f"[mart_weather_impact] descriptors mismatch vs dim_date/dim_weather: {mismatch_count}"
        )


# Kiểm tra reconciliation của mart_weather_impact với fact_taxi_trips.
def validate_mart_weather_impact_reconciliation(mart_df: DataFrame, fact_df: DataFrame, logger) -> None:
    expected_row_count = fact_df.select("pickup_date").distinct().count()
    actual_row_count = mart_df.count()

    logger.info("[mart_weather_impact] expected distinct pickup_date count=%s", expected_row_count)
    logger.info("[mart_weather_impact] actual row_count=%s", actual_row_count)

    if actual_row_count != expected_row_count:
        raise ValueError(
            f"[mart_weather_impact] row_count != distinct pickup_date count from fact_taxi_trips | "
            f"expected={expected_row_count}, actual={actual_row_count}"
        )

    mart_trip_count_sum = _sum_column(mart_df, "trip_count")
    fact_row_count = float(fact_df.count())
    validate_float_close(mart_trip_count_sum, fact_row_count, "[mart_weather_impact] trip_count sum vs fact row_count", tolerance=0.0)

    mart_total_revenue_sum = _sum_column(mart_df, "total_revenue")
    fact_total_amount_sum = _sum_column(fact_df, "total_amount")
    validate_float_close(mart_total_revenue_sum, fact_total_amount_sum, "[mart_weather_impact] total_revenue sum vs fact total_amount sum")

    mart_negative_trip_count_sum = _sum_column(mart_df, "negative_total_amount_trip_count")
    fact_negative_trip_count = float(fact_df.filter(F.col("total_amount") < 0).count())
    validate_float_close(mart_negative_trip_count_sum, fact_negative_trip_count, "[mart_weather_impact] negative_total_amount_trip_count sum vs fact negative rows", tolerance=0.0)


# Kiểm tra bộ cột mart_zone_demand đúng theo tên cột mong đợi.
def validate_mart_zone_demand_schema(df: DataFrame, logger) -> None:
    _assert_columns_match_by_name(
        df,
        EXPECTED_MART_ZONE_DEMAND_COLUMNS,
        "mart_zone_demand",
    )
    _log_schema(logger, "mart_zone_demand", df)


# Kiểm tra các cột cốt lõi của mart_zone_demand không được null.
def validate_mart_zone_demand_not_null(df: DataFrame) -> None:
    null_counts = (
        df.select(
            F.sum(F.col("pickup_date").isNull().cast("int")).alias("pickup_date_nulls"),
            F.sum(F.col("pickup_year").isNull().cast("int")).alias("pickup_year_nulls"),
            F.sum(F.col("pickup_month").isNull().cast("int")).alias("pickup_month_nulls"),
            F.sum(F.col("pickup_location_id").isNull().cast("int")).alias("pickup_location_id_nulls"),
            F.sum(F.col("borough").isNull().cast("int")).alias("borough_nulls"),
            F.sum(F.col("zone").isNull().cast("int")).alias("zone_nulls"),
            F.sum(F.col("service_zone").isNull().cast("int")).alias("service_zone_nulls"),
            F.sum(F.col("trip_count").isNull().cast("int")).alias("trip_count_nulls"),
            F.sum(F.col("total_revenue").isNull().cast("int")).alias("total_revenue_nulls"),
            F.sum(F.col("negative_total_amount_trip_count").isNull().cast("int")).alias("negative_total_amount_trip_count_nulls"),
            F.sum(F.col("serving_loaded_at").isNull().cast("int")).alias("serving_loaded_at_nulls"),
        ).collect()[0]
    )

    for field_name, value in null_counts.asDict().items():
        if value != 0:
            raise ValueError(f"[mart_zone_demand] {field_name} must be 0 but got {value}")

    blank_counts = (
        df.select(
            F.sum((F.trim(F.col("borough")) == "").cast("int")).alias("blank_borough_rows"),
            F.sum((F.trim(F.col("zone")) == "").cast("int")).alias("blank_zone_rows"),
            F.sum((F.trim(F.col("service_zone")) == "").cast("int")).alias("blank_service_zone_rows"),
        ).collect()[0]
    )

    for field_name, value in blank_counts.asDict().items():
        if value != 0:
            raise ValueError(f"[mart_zone_demand] {field_name} must be 0 but got {value}")


# mart_zone_demand có grain 1 row = 1 pickup_date x 1 pickup_location_id.
def validate_mart_zone_demand_uniqueness(df: DataFrame) -> None:
    validate_unique_key(
        df,
        ["pickup_date", "pickup_location_id"],
        "mart_zone_demand",
    )


# Kiểm tra zone descriptors trong mart_zone_demand khớp với dim_zone.
def validate_mart_zone_demand_consistency(mart_df: DataFrame, dim_zone_df: DataFrame) -> None:
    z = dim_zone_df.select(
        "location_id",
        "borough",
        "zone",
        "service_zone",
    ).alias("z")

    m = mart_df.alias("m")

    mismatch_count = (
        m
        .join(z, F.col("m.pickup_location_id") == F.col("z.location_id"), how="left")
        .filter(
            (F.col("z.location_id").isNull()) |
            (F.col("m.borough") != F.col("z.borough")) |
            (F.col("m.zone") != F.col("z.zone")) |
            (F.col("m.service_zone") != F.col("z.service_zone"))
        )
        .count()
    )

    if mismatch_count != 0:
        raise ValueError(
            f"[mart_zone_demand] zone descriptors mismatch vs dim_zone: {mismatch_count}"
        )


# Kiểm tra reconciliation của mart_zone_demand với fact_taxi_trips.
def validate_mart_zone_demand_reconciliation(mart_df: DataFrame, fact_df: DataFrame, logger) -> None:
    expected_row_count = (
        fact_df
        .select("pickup_date", "pickup_location_id")
        .distinct()
        .count()
    )
    actual_row_count = mart_df.count()

    logger.info("[mart_zone_demand] expected distinct pickup_date x pickup_location_id count=%s", expected_row_count)
    logger.info("[mart_zone_demand] actual row_count=%s", actual_row_count)

    if actual_row_count != expected_row_count:
        raise ValueError(
            f"[mart_zone_demand] row_count != distinct pickup_date x pickup_location_id count "
            f"from fact_taxi_trips | expected={expected_row_count}, actual={actual_row_count}"
        )

    mart_trip_count_sum = _sum_column(mart_df, "trip_count")
    fact_row_count = float(fact_df.count())
    validate_float_close(mart_trip_count_sum, fact_row_count, "[mart_zone_demand] trip_count sum vs fact row_count", tolerance=0.0)

    mart_total_revenue_sum = _sum_column(mart_df, "total_revenue")
    fact_total_amount_sum = _sum_column(fact_df, "total_amount")
    validate_float_close(mart_total_revenue_sum, fact_total_amount_sum, "[mart_zone_demand] total_revenue sum vs fact total_amount sum")

    mart_negative_trip_count_sum = _sum_column(mart_df, "negative_total_amount_trip_count")
    fact_negative_trip_count = float(fact_df.filter(F.col("total_amount") < 0).count())
    validate_float_close(mart_negative_trip_count_sum, fact_negative_trip_count, "[mart_zone_demand] negative_total_amount_trip_count sum vs fact negative rows", tolerance=0.0)


# Chạy toàn bộ validation serving theo thứ tự
def main() -> None:
    config = load_app_config()
    paths = _resolve_paths(config)
    logger = get_logger(name="validation_serving", log_dir=config["paths"]["log_dir"])

    logger.info("=" * 60)
    logger.info("START SERVING VALIDATION")
    logger.info("=" * 60)
    logger.info("Resolved validation paths: %s", {k: str(v) for k, v in paths.items()})

    spark = build_spark(app_name="validation_serving")

    try:
        # READ GOLD SOURCES FOR RECONCILIATION
        logger.info("Reading gold sources for reconciliation...")
        dim_date_df = read_parquet_dataset(spark=spark, dataset_path=paths["gold_dim_date_dir"], logger=logger, label="dim_date")
        dim_zone_df = read_parquet_dataset(spark=spark, dataset_path=paths["gold_dim_zone_dir"], logger=logger, label="dim_zone")
        dim_weather_df = read_parquet_dataset(spark=spark, dataset_path=paths["gold_dim_weather_dir"], logger=logger, label="dim_weather")
        fact_taxi_df = read_parquet_dataset(spark=spark, dataset_path=paths["gold_fact_taxi_dir"], logger=logger, label="fact_taxi_trips")

        # READ SERVING OUTPUTS
        logger.info("Reading serving outputs...")
        mart_daily_demand_df = read_parquet_dataset(spark=spark, dataset_path=paths["serving_mart_daily_demand_dir"], logger=logger, label="mart_daily_demand")
        mart_daily_payment_mix_df = read_parquet_dataset(spark=spark, dataset_path=paths["serving_mart_daily_payment_mix_dir"], logger=logger, label="mart_daily_payment_mix")
        mart_weather_impact_df = read_parquet_dataset(spark=spark, dataset_path=paths["serving_mart_weather_impact_dir"], logger=logger, label="mart_weather_impact")
        mart_zone_demand_df = read_parquet_dataset(spark=spark, dataset_path=paths["serving_mart_zone_demand_dir"], logger=logger, label="mart_zone_demand")

        # MART_DAILY_DEMAND VALIDATION
        logger.info("Validating mart_daily_demand...")
        validate_non_empty(mart_daily_demand_df, "mart_daily_demand")
        validate_mart_daily_demand_schema(mart_daily_demand_df, logger)
        validate_mart_daily_demand_not_null(mart_daily_demand_df)
        validate_mart_daily_demand_uniqueness(mart_daily_demand_df)
        validate_mart_daily_demand_date_consistency(mart_df=mart_daily_demand_df, dim_date_df=dim_date_df)
        validate_mart_daily_demand_reconciliation(mart_df=mart_daily_demand_df, fact_df=fact_taxi_df, logger=logger)
        validate_partition_columns(df=mart_daily_demand_df, dataset_name="mart_daily_demand", output_path=paths["serving_mart_daily_demand_dir"], logger=logger)
        logger.info("mart_daily_demand validation passed.")

        # MART_DAILY_PAYMENT_MIX VALIDATION
        logger.info("Validating mart_daily_payment_mix...")
        validate_non_empty(mart_daily_payment_mix_df, "mart_daily_payment_mix")
        validate_mart_daily_payment_mix_schema(mart_daily_payment_mix_df, logger)
        validate_mart_daily_payment_mix_not_null(mart_daily_payment_mix_df)
        validate_mart_daily_payment_mix_uniqueness(mart_daily_payment_mix_df)
        validate_mart_daily_payment_mix_payment_type_name(mart_daily_payment_mix_df)
        validate_mart_daily_payment_mix_shares(mart_daily_payment_mix_df, logger)
        validate_mart_daily_payment_mix_reconciliation(mart_df=mart_daily_payment_mix_df, fact_df=fact_taxi_df, logger=logger)
        validate_partition_columns(df=mart_daily_payment_mix_df, dataset_name="mart_daily_payment_mix", output_path=paths["serving_mart_daily_payment_mix_dir"], logger=logger)
        logger.info("mart_daily_payment_mix validation passed.")

        # MART_WEATHER_IMPACT VALIDATION
        logger.info("Validating mart_weather_impact...")
        validate_non_empty(mart_weather_impact_df, "mart_weather_impact")
        validate_mart_weather_impact_schema(mart_weather_impact_df, logger)
        validate_mart_weather_impact_not_null(mart_weather_impact_df)
        validate_mart_weather_impact_uniqueness(mart_weather_impact_df)
        validate_mart_weather_impact_consistency(mart_df=mart_weather_impact_df, dim_date_df=dim_date_df, dim_weather_df=dim_weather_df)
        validate_mart_weather_impact_reconciliation(mart_df=mart_weather_impact_df, fact_df=fact_taxi_df, logger=logger)
        validate_partition_columns(df=mart_weather_impact_df, dataset_name="mart_weather_impact", output_path=paths["serving_mart_weather_impact_dir"], logger=logger)
        logger.info("mart_weather_impact validation passed.")

        # -------------------------------------------------
        # MART_ZONE_DEMAND VALIDATION
        # -------------------------------------------------
        logger.info("Validating mart_zone_demand...")
        validate_non_empty(mart_zone_demand_df, "mart_zone_demand")
        validate_mart_zone_demand_schema(mart_zone_demand_df, logger)
        validate_mart_zone_demand_not_null(mart_zone_demand_df)
        validate_mart_zone_demand_uniqueness(mart_zone_demand_df)
        validate_mart_zone_demand_consistency(mart_df=mart_zone_demand_df, dim_zone_df=dim_zone_df)
        validate_mart_zone_demand_reconciliation(mart_df=mart_zone_demand_df, fact_df=fact_taxi_df, logger=logger)
        validate_partition_columns(df=mart_zone_demand_df, dataset_name="mart_zone_demand", output_path=paths["serving_mart_zone_demand_dir"], logger=logger)
        logger.info("mart_zone_demand validation passed.")

        logger.info("=" * 60)
        logger.info("SERVING VALIDATION FINISHED SUCCESSFULLY")
        logger.info("=" * 60)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()