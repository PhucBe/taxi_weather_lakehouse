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
# Mục tiêu:
# - Đọc 3 bảng bronze parquet:
#     + taxi bronze
#     + weather bronze
#     + zone bronze
# - Transform thành 3 bảng silver base:
#     + silver_taxi_trips
#     + silver_weather_daily
#     + silver_zone_lookup
# - Build thêm 1 bảng reusable:
#     + silver_taxi_trips_enriched
# - Ghi output ra parquet
#
# Triết lý của layer silver:
# - sạch hơn bronze
# - schema business-friendly hơn bronze
# - loại bỏ invalid records rõ ràng
# - tạo derived columns để downstream dùng lại
# - sẵn sàng cho layer gold / BI
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

SILVER_TAXI_ENRICHED_COLUMNS = [
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
    Ép mọi giá trị path trong config thành pathlib.Path.

    Ví dụ:
    - "data/silver/taxi_trips" -> Path("data/silver/taxi_trips")
    - Path(...) giữ nguyên
    """
    if isinstance(value, Path):
        return value
    return Path(str(value))


def _ensure_directory(path: Path) -> None:
    """
    Tạo folder nếu chưa tồn tại.
    parents=True để tạo cả folder cha nếu cần.
    exist_ok=True để không lỗi nếu folder đã có sẵn.
    """
    path.mkdir(parents=True, exist_ok=True)


def _resolve_paths(config: dict[str, Any]) -> dict[str, Path]:
    """
    Đọc path từ config và trả về 1 dict các đường dẫn đã normalize.

    Ưu tiên:
    - dùng trực tiếp bronze_*_dir / silver_*_dir nếu config đã có
    - nếu chưa có, fallback từ bronze_dir / silver_dir
    - nếu chưa có nữa, fallback từ local_data_dir
    """
    paths_cfg = config["paths"]

    # Root local data dir để fallback
    local_data_dir = _as_path(paths_cfg.get("local_data_dir", "data"))

    # Bronze root + input datasets
    bronze_dir = _as_path(paths_cfg.get("bronze_dir", local_data_dir / "bronze"))
    bronze_taxi_dir = _as_path(paths_cfg.get("bronze_taxi_dir", bronze_dir / "taxi_trips_raw"))
    bronze_weather_dir = _as_path(paths_cfg.get("bronze_weather_dir", bronze_dir / "weather_raw"))
    bronze_zone_dir = _as_path(paths_cfg.get("bronze_zone_dir", bronze_dir / "zone_lookup"))

    # Silver root + output datasets
    silver_dir = _as_path(paths_cfg.get("silver_dir", local_data_dir / "silver"))
    silver_taxi_dir = _as_path(paths_cfg.get("silver_taxi_dir", silver_dir / "taxi_trips"))
    silver_weather_dir = _as_path(paths_cfg.get("silver_weather_dir", silver_dir / "weather_daily"))
    silver_zone_dir = _as_path(paths_cfg.get("silver_zone_dir", silver_dir / "zone_lookup"))
    silver_taxi_enriched_dir = _as_path(
        paths_cfg.get("silver_taxi_enriched_dir", silver_dir / "taxi_trips_enriched")
    )

    return {
        "bronze_dir": bronze_dir,
        "bronze_taxi_dir": bronze_taxi_dir,
        "bronze_weather_dir": bronze_weather_dir,
        "bronze_zone_dir": bronze_zone_dir,
        "silver_dir": silver_dir,
        "silver_taxi_dir": silver_taxi_dir,
        "silver_weather_dir": silver_weather_dir,
        "silver_zone_dir": silver_zone_dir,
        "silver_taxi_enriched_dir": silver_taxi_enriched_dir,
    }


def build_spark(app_name: str = "bronze_to_silver") -> SparkSession:
    """
    Tạo SparkSession cho job bronze -> silver.

    Các config ở đây đủ dùng cho local prototype:
    - shuffle partitions vừa phải
    - partition overwrite mode dynamic để overwrite partition linh hoạt hơn
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

    # In log gọn level WARN để đỡ quá nhiều noise khi chạy local.
    spark.sparkContext.setLogLevel("WARN")
    return spark


def log_dataframe_overview(logger, dataset_name: str, df: DataFrame) -> None:
    """
    Log thông tin cơ bản của DataFrame sau transform:
    - số dòng
    - danh sách cột
    - schema dạng simpleString

    count() tốn tài nguyên, nhưng với local prototype vẫn chấp nhận được.
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

    Rule:
    - path phải tồn tại
    - path phải là directory
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
    Đọc toàn bộ input bronze và trả về 1 dict DataFrame.

    Trả về:
    - taxi_bronze_df
    - weather_bronze_df
    - zone_bronze_df
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

    Bao gồm:
    - pickup_hour
    - trip_duration_minutes
    - chuẩn hóa pickup_year / pickup_month từ pickup_date nếu cần
    """
    df = (
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

    return df


def apply_taxi_quality_filters(df: DataFrame) -> DataFrame:
    """
    Áp business rules tối thiểu để taxi từ bronze đi vào silver.

    Hard filters:
    - pickup_datetime không được null
    - dropoff_datetime không được null
    - pickup_date không được null
    - pickup_location_id không được null
    - dropoff_location_id không được null
    - pickup phải sớm hơn dropoff
    - trip_duration_minutes phải > 0

    Lưu ý:
    - Đây là clean ở mức silver, chưa phải reject/outlier framework đầy đủ.
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
    weather chỉ cover 2023-01-01 đến 2023-03-31.
    """
    return df.filter(
        (F.col("pickup_date") >= F.to_date(F.lit("2023-01-01"))) &
        (F.col("pickup_date") <= F.to_date(F.lit("2023-03-31")))
    )


def build_trip_id(df: DataFrame) -> DataFrame:
    """
    Sinh surrogate key trip_id cho taxi silver.

    Ý tưởng:
    - hash từ các cột tương đối ổn định ở grain 1 trip
    - dùng sha2 256 để key đủ chắc cho local analytics pipeline

    Chú ý:
    - Nếu sau này muốn key ngắn hơn có thể đổi sang md5
    - Nếu muốn grain chặt hơn có thể thêm passenger_count / fare_amount
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

    Bronze taxi hiện tại đã:
    - cast type cơ bản
    - có pickup_date, pickup_year, pickup_month
    - có source_file, bronze_loaded_at

    Silver taxi sẽ:
    - rename sang tên business-friendly
    - cast lại các cột business chính
    - tạo derived columns
    - filter invalid rows
    - sinh trip_id
    - drop duplicate theo trip_id
    - thêm silver_loaded_at
    """
    # -----------------------------------------------------
    # 1) Rename cột từ bronze sang business-friendly
    # -----------------------------------------------------
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

    # -----------------------------------------------------
    # 2) Cast lại kiểu dữ liệu business
    # -----------------------------------------------------
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

    # -----------------------------------------------------
    # 3) Tạo derived columns dùng lại nhiều lần downstream
    # -----------------------------------------------------
    df = add_taxi_derived_columns(df)

    # -----------------------------------------------------
    # 4) Áp quality filters để loại invalid rows rõ ràng
    # -----------------------------------------------------
    df = apply_taxi_quality_filters(df)

    # -----------------------------------------------------
    # 5) Chỉ giữ taxi trong phạm vi dữ liệu thời tiết hiện có của project
    # -----------------------------------------------------
    df = apply_project_date_scope(df)

    # -----------------------------------------------------
    # 6) Sinh trip_id ở grain 1 trip
    # -----------------------------------------------------
    df = build_trip_id(df)

    # -----------------------------------------------------
    # 7) Loại duplicate nhẹ theo trip_id
    # -----------------------------------------------------
    df = df.dropDuplicates(["trip_id"])

    # -----------------------------------------------------
    # 8) Thêm metadata kỹ thuật của silver
    # -----------------------------------------------------
    df = df.withColumn("silver_loaded_at", F.current_timestamp())

    # -----------------------------------------------------
    # 9) Chọn lại thứ tự cột cuối cùng cho đẹp và ổn định
    # -----------------------------------------------------
    return df.select(*SILVER_TAXI_COLUMNS)


def transform_weather_to_silver(df: DataFrame) -> DataFrame:
    """
    Transform weather bronze -> silver_weather_daily.

    Bronze weather hiện tại đã:
    - có date dạng date
    - metric weather đã cast numeric
    - có weather_year, weather_month
    - có source_file, bronze_loaded_at

    Silver weather sẽ:
    - rename sang tên business-friendly
    - tạo cờ thời tiết đơn giản
    - drop duplicate theo weather_date
    - thêm silver_loaded_at
    """
    # -----------------------------------------------------
    # 1) Rename cột cho dễ hiểu ở downstream
    # -----------------------------------------------------
    df = (
        df
        .withColumnRenamed("date", "weather_date")
        .withColumnRenamed("temperature_2m_max", "temperature_max")
        .withColumnRenamed("temperature_2m_min", "temperature_min")
        .withColumnRenamed("temperature_2m_mean", "temperature_mean")
    )

    # -----------------------------------------------------
    # 2) Tạo cờ thời tiết cơ bản
    # -----------------------------------------------------
    df = (
        df
        .withColumn("is_rainy_day", F.col("precipitation_sum") > F.lit(0))
        .withColumn("is_snowy_day", F.col("snowfall_sum") > F.lit(0))
    )

    # -----------------------------------------------------
    # 3) Loại record không có ngày và drop duplicate theo ngày
    # -----------------------------------------------------
    df = df.filter(F.col("weather_date").isNotNull())
    df = df.dropDuplicates(["weather_date"])

    # -----------------------------------------------------
    # 4) Thêm metadata kỹ thuật của silver
    # -----------------------------------------------------
    df = df.withColumn("silver_loaded_at", F.current_timestamp())

    # -----------------------------------------------------
    # 5) Chọn lại thứ tự cột cuối cùng
    # -----------------------------------------------------
    return df.select(*SILVER_WEATHER_COLUMNS)


def transform_zone_to_silver(df: DataFrame) -> DataFrame:
    """
    Transform zone bronze -> silver_zone_lookup.

    Bronze zone hiện tại tương đối sạch rồi.
    Silver zone chủ yếu:
    - trim lại string
    - lọc bản ghi không có location_id
    - drop duplicate theo location_id
    - thêm silver_loaded_at
    """
    # -----------------------------------------------------
    # 1) Trim lại các cột string để đồng nhất
    # -----------------------------------------------------
    df = (
        df
        .withColumn("borough", F.trim(F.col("borough")))
        .withColumn("zone", F.trim(F.col("zone")))
        .withColumn("service_zone", F.trim(F.col("service_zone")))
    )

    # -----------------------------------------------------
    # 2) Loại record thiếu key lookup chính
    # -----------------------------------------------------
    df = df.filter(F.col("location_id").isNotNull())

    # -----------------------------------------------------
    # 3) Drop duplicate theo natural key của lookup
    # -----------------------------------------------------
    df = df.dropDuplicates(["location_id"])

    # -----------------------------------------------------
    # 4) Thêm metadata kỹ thuật của silver
    # -----------------------------------------------------
    df = df.withColumn("silver_loaded_at", F.current_timestamp())

    # -----------------------------------------------------
    # 5) Chọn lại thứ tự cột cuối cùng
    # -----------------------------------------------------
    return df.select(*SILVER_ZONE_COLUMNS)


# =========================================================
# 6) BUILD REUSABLE ENRICHED SILVER
# =========================================================
def build_taxi_trips_enriched(
    taxi_df: DataFrame,
    weather_df: DataFrame,
    zone_df: DataFrame,
) -> DataFrame:
    """
    Build silver_taxi_trips_enriched từ:
    - silver_taxi_trips
    - silver_weather_daily
    - silver_zone_lookup

    Logic:
    - join zone cho pickup_location_id
    - join zone cho dropoff_location_id
    - join weather theo pickup_date = weather_date

    Join type:
    - zone: left join
    - weather: left join

    Quan trọng:
    - chỉ giữ metadata của taxi_df trong bảng enriched
    - không select mù bằng tên cột string vì sau join sẽ có cột trùng tên
    """
    # -----------------------------------------------------
    # 1) Tạo 2 phiên bản zone đã rename sẵn để join
    # -----------------------------------------------------
    pickup_zone_df = zone_df.select(
        F.col("location_id").alias("pickup_location_id"),
        F.col("borough").alias("pickup_borough"),
        F.col("zone").alias("pickup_zone"),
        F.col("service_zone").alias("pickup_service_zone"),
    )

    dropoff_zone_df = zone_df.select(
        F.col("location_id").alias("dropoff_location_id"),
        F.col("borough").alias("dropoff_borough"),
        F.col("zone").alias("dropoff_zone"),
        F.col("service_zone").alias("dropoff_service_zone"),
    )

    # -----------------------------------------------------
    # 2) Chỉ lấy các cột business cần thiết của weather
    #    KHÔNG lấy source_file / bronze_loaded_at / silver_loaded_at
    #    để tránh trùng tên với taxi_df
    # -----------------------------------------------------
    weather_join_df = weather_df.select(
        "weather_date",
        "temperature_max",
        "temperature_min",
        "temperature_mean",
        "precipitation_sum",
        "snowfall_sum",
        "is_rainy_day",
        "is_snowy_day",
    )

    # -----------------------------------------------------
    # 3) Alias dataframe để select rõ nguồn cột
    # -----------------------------------------------------
    t = taxi_df.alias("t")
    pz = pickup_zone_df.alias("pz")
    dz = dropoff_zone_df.alias("dz")
    w = weather_join_df.alias("w")

    # -----------------------------------------------------
    # 4) Join zone + weather
    # -----------------------------------------------------
    df = (
        t
        .join(pz, on="pickup_location_id", how="left")
        .join(dz, on="dropoff_location_id", how="left")
        .join(w, F.col("t.pickup_date") == F.col("w.weather_date"), how="left")
    )

    # -----------------------------------------------------
    # 5) Select explicit để tránh ambiguous columns
    # -----------------------------------------------------
    return df.select(
        F.col("t.trip_id").alias("trip_id"),
        F.col("t.vendor_id").alias("vendor_id"),
        F.col("t.pickup_datetime").alias("pickup_datetime"),
        F.col("t.dropoff_datetime").alias("dropoff_datetime"),
        F.col("t.pickup_date").alias("pickup_date"),
        F.col("t.pickup_year").alias("pickup_year"),
        F.col("t.pickup_month").alias("pickup_month"),
        F.col("t.pickup_hour").alias("pickup_hour"),
        F.col("t.passenger_count").alias("passenger_count"),
        F.col("t.trip_distance").alias("trip_distance"),
        F.col("t.trip_duration_minutes").alias("trip_duration_minutes"),
        F.col("t.rate_code_id").alias("rate_code_id"),
        F.col("t.store_and_fwd_flag").alias("store_and_fwd_flag"),
        F.col("t.pickup_location_id").alias("pickup_location_id"),
        F.col("pz.pickup_borough").alias("pickup_borough"),
        F.col("pz.pickup_zone").alias("pickup_zone"),
        F.col("pz.pickup_service_zone").alias("pickup_service_zone"),
        F.col("t.dropoff_location_id").alias("dropoff_location_id"),
        F.col("dz.dropoff_borough").alias("dropoff_borough"),
        F.col("dz.dropoff_zone").alias("dropoff_zone"),
        F.col("dz.dropoff_service_zone").alias("dropoff_service_zone"),
        F.col("t.payment_type_code").alias("payment_type_code"),
        F.col("t.fare_amount").alias("fare_amount"),
        F.col("t.extra_amount").alias("extra_amount"),
        F.col("t.mta_tax_amount").alias("mta_tax_amount"),
        F.col("t.tip_amount").alias("tip_amount"),
        F.col("t.tolls_amount").alias("tolls_amount"),
        F.col("t.improvement_surcharge_amount").alias("improvement_surcharge_amount"),
        F.col("t.congestion_surcharge_amount").alias("congestion_surcharge_amount"),
        F.col("t.airport_fee_amount").alias("airport_fee_amount"),
        F.col("t.total_amount").alias("total_amount"),
        F.col("w.weather_date").alias("weather_date"),
        F.col("w.temperature_max").alias("temperature_max"),
        F.col("w.temperature_min").alias("temperature_min"),
        F.col("w.temperature_mean").alias("temperature_mean"),
        F.col("w.precipitation_sum").alias("precipitation_sum"),
        F.col("w.snowfall_sum").alias("snowfall_sum"),
        F.col("w.is_rainy_day").alias("is_rainy_day"),
        F.col("w.is_snowy_day").alias("is_snowy_day"),
        F.col("t.source_file").alias("source_file"),
        F.col("t.bronze_loaded_at").alias("bronze_loaded_at"),
        F.col("t.silver_loaded_at").alias("silver_loaded_at"),
    )


# =========================================================
# 7) WRITE SILVER PARQUET
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
    - silver_taxi_trips = ["pickup_year", "pickup_month"]
    - silver_weather_daily = ["weather_year", "weather_month"]
    - silver_zone_lookup = None
    - silver_taxi_trips_enriched = ["pickup_year", "pickup_month"]
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
    Ghi toàn bộ output silver ra parquet.

    outputs gồm:
    - silver_taxi_df
    - silver_weather_df
    - silver_zone_df
    - silver_taxi_enriched_df
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

    logger.info("Writing silver_taxi_trips_enriched parquet...")
    write_parquet(
        df=outputs["silver_taxi_enriched_df"],
        output_path=paths["silver_taxi_enriched_dir"],
        mode="overwrite",
        partition_cols=["pickup_year", "pickup_month"],
    )
    logger.info(
        "silver_taxi_trips_enriched written to: %s",
        paths["silver_taxi_enriched_dir"],
    )


# =========================================================
# 8) MAIN ORCHESTRATION
# =========================================================
def run_bronze_to_silver() -> None:
    """
    Hàm orchestration chính chạy toàn bộ flow bronze -> silver:
    1) load config
    2) resolve paths
    3) build spark
    4) read bronze parquet
    5) transform -> silver base
    6) build enriched silver
    7) log overview các output
    8) write parquet
    9) log thành công
    """
    # Load config từ project
    config = load_app_config()

    # Resolve toàn bộ path cần dùng
    paths = _resolve_paths(config)

    # Tạo logger theo cùng style pipeline hiện tại
    logger = get_logger(
        name="spark_bronze_to_silver",
        log_dir=config["paths"]["log_dir"],
    )

    logger.info("=" * 60)
    logger.info("START SPARK BRONZE -> SILVER")
    logger.info("=" * 60)
    logger.info("Resolved paths: %s", {k: str(v) for k, v in paths.items()})

    # Tạo SparkSession
    spark = build_spark(app_name="spark_bronze_to_silver")

    try:
        # -------------------------------------------------
        # READ BRONZE INPUTS
        # -------------------------------------------------
        logger.info("Reading bronze inputs...")
        bronze_inputs = read_all_bronze_inputs(
            spark=spark,
            paths=paths,
            logger=logger,
        )

        taxi_bronze_df = bronze_inputs["taxi_bronze_df"]
        weather_bronze_df = bronze_inputs["weather_bronze_df"]
        zone_bronze_df = bronze_inputs["zone_bronze_df"]

        # -------------------------------------------------
        # TRANSFORM TO SILVER BASE
        # -------------------------------------------------
        logger.info("Transforming taxi bronze -> silver_taxi_trips...")
        silver_taxi_df = transform_taxi_to_silver(taxi_bronze_df)
        log_dataframe_overview(logger, "silver_taxi_trips", silver_taxi_df)

        logger.info("Transforming weather bronze -> silver_weather_daily...")
        silver_weather_df = transform_weather_to_silver(weather_bronze_df)
        log_dataframe_overview(logger, "silver_weather_daily", silver_weather_df)

        logger.info("Transforming zone bronze -> silver_zone_lookup...")
        silver_zone_df = transform_zone_to_silver(zone_bronze_df)
        log_dataframe_overview(logger, "silver_zone_lookup", silver_zone_df)

        # -------------------------------------------------
        # BUILD ENRICHED SILVER
        # -------------------------------------------------
        logger.info("Building silver_taxi_trips_enriched...")
        silver_taxi_enriched_df = build_taxi_trips_enriched(
            taxi_df=silver_taxi_df,
            weather_df=silver_weather_df,
            zone_df=silver_zone_df,
        )
        log_dataframe_overview(
            logger,
            "silver_taxi_trips_enriched",
            silver_taxi_enriched_df,
        )

        # -------------------------------------------------
        # WRITE OUTPUTS
        # -------------------------------------------------
        silver_outputs = {
            "silver_taxi_df": silver_taxi_df,
            "silver_weather_df": silver_weather_df,
            "silver_zone_df": silver_zone_df,
            "silver_taxi_enriched_df": silver_taxi_enriched_df,
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
        # Dù job thành công hay lỗi thì vẫn stop Spark để giải phóng tài nguyên.
        spark.stop()


# =========================================================
# 9) ENTRYPOINT
# =========================================================
def main() -> None:
    """
    Entry point chuẩn Python.
    """
    run_bronze_to_silver()


if __name__ == "__main__":
    main()