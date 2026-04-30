from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pathlib import Path
from typing import Any
import glob

from src.common.config import load_app_config
from src.common.logger import get_logger


TAXI_RAW_COLUMNS = [
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

WEATHER_RAW_COLUMNS = [
    "date",
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "snowfall_sum",
]

ZONE_SOURCE_COLUMNS = [
    "LocationID",
    "Borough",
    "Zone",
    "service_zone",
]


# SCHEMA TƯỜNG MINH CHO INPUT CSV
TAXI_RAW_SCHEMA = T.StructType(
    [
        T.StructField("vendorid", T.StringType(), True),
        T.StructField("tpep_pickup_datetime", T.StringType(), True),
        T.StructField("tpep_dropoff_datetime", T.StringType(), True),
        T.StructField("passenger_count", T.StringType(), True),
        T.StructField("trip_distance", T.StringType(), True),
        T.StructField("ratecodeid", T.StringType(), True),
        T.StructField("store_and_fwd_flag", T.StringType(), True),
        T.StructField("pulocationid", T.StringType(), True),
        T.StructField("dolocationid", T.StringType(), True),
        T.StructField("payment_type", T.StringType(), True),
        T.StructField("fare_amount", T.StringType(), True),
        T.StructField("extra", T.StringType(), True),
        T.StructField("mta_tax", T.StringType(), True),
        T.StructField("tip_amount", T.StringType(), True),
        T.StructField("tolls_amount", T.StringType(), True),
        T.StructField("improvement_surcharge", T.StringType(), True),
        T.StructField("total_amount", T.StringType(), True),
        T.StructField("congestion_surcharge", T.StringType(), True),
        T.StructField("airport_fee", T.StringType(), True),
    ]
)

WEATHER_RAW_SCHEMA = T.StructType(
    [
        T.StructField("date", T.StringType(), True),
        T.StructField("temperature_2m_max", T.StringType(), True),
        T.StructField("temperature_2m_min", T.StringType(), True),
        T.StructField("temperature_2m_mean", T.StringType(), True),
        T.StructField("precipitation_sum", T.StringType(), True),
        T.StructField("snowfall_sum", T.StringType(), True),
    ]
)

ZONE_RAW_SCHEMA = T.StructType(
    [
        T.StructField("LocationID", T.StringType(), True),
        T.StructField("Borough", T.StringType(), True),
        T.StructField("Zone", T.StringType(), True),
        T.StructField("service_zone", T.StringType(), True),
    ]
)


# Hàm ép mọi giá trị path trong config thành pathlib.Path.
def _as_path(value: Any) -> Path:
    if isinstance(value, Path):
        return value
    
    return Path(str(value))


# Tạo folder nếu chưa tồn tại.
def _ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


# Kiểm tra xem pattern input có match ra file nào không.
def _require_glob_matches(pattern: str, label: str) -> list[str]:
    matches = sorted(glob.glob(pattern, recursive=True))

    if not matches:
        raise FileNotFoundError(f"No {label} files found for pattern: {pattern}")
    
    return matches


# Đảm bảo DataFrame có đủ tất cả cột expected_columns.
def _ensure_columns(df: DataFrame, expected_columns: list[str]) -> DataFrame:
    for column_name in expected_columns:
        if column_name not in df.columns:
            df = df.withColumn(column_name, F.lit(None).cast(T.StringType()))

    return df.select(*expected_columns)


# Đọc path từ config và trả về 1 dict các đường dẫn đã normalize.
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
        "taxi_input_glob": taxi_dir / "flat" / "**" / "*.csv",
        "weather_input_glob": weather_dir / "flat" / "**" / "*.csv",
        "zone_input_glob": zone_dir / "**" / "*.csv",
        "bronze_dir": bronze_dir,
        "bronze_taxi_dir": bronze_taxi_dir,
        "bronze_weather_dir": bronze_weather_dir,
        "bronze_zone_dir": bronze_zone_dir,
    }


# Tạo SparkSession cho job raw -> bronze.
def build_spark(app_name: str = "raw_to_bronze") -> SparkSession:
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


# Đọc taxi raw flat CSV bằng schema tường minh.
def read_taxi_raw_csv(spark: SparkSession, input_glob: str, logger) -> DataFrame:
    matched_files = _require_glob_matches(input_glob, "taxi raw CSV")

    logger.info("Found %s taxi raw CSV file(s).", len(matched_files))
    logger.info("Taxi input pattern: %s", input_glob)
    logger.info("Taxi matched files: %s", matched_files)

    return (
        spark.read
        .option("header", True)
        .schema(TAXI_RAW_SCHEMA)
        .csv(matched_files)
    )


# Đọc weather raw flat CSV bằng schema tường minh.
def read_weather_raw_csv(spark: SparkSession, input_glob: str, logger) -> DataFrame:
    matched_files = _require_glob_matches(input_glob, "weather raw CSV")

    logger.info("Found %s weather raw CSV file(s).", len(matched_files))
    logger.info("Weather input pattern: %s", input_glob)
    logger.info("Weather matched files: %s", matched_files)

    return (
        spark.read
        .option("header", True)
        .schema(WEATHER_RAW_SCHEMA)
        .csv(matched_files)
    )


# Đọc zone lookup CSV bằng schema tường minh.
def read_zone_raw_csv(spark: SparkSession, input_glob: str, logger) -> DataFrame:
    matched_files = _require_glob_matches(input_glob, "zone raw CSV")

    logger.info("Found %s zone raw CSV file(s).", len(matched_files))
    logger.info("Zone input pattern: %s", input_glob)
    logger.info("Zone matched files: %s", matched_files)

    return (
        spark.read
        .option("header", True)
        .schema(ZONE_RAW_SCHEMA)
        .csv(matched_files)
    )


# Transform taxi raw CSV -> bronze taxi parquet.
def transform_taxi_to_bronze(df: DataFrame) -> DataFrame:
    # Đảm bảo đủ toàn bộ cột taxi chuẩn.
    df = _ensure_columns(df, TAXI_RAW_COLUMNS)

    # Cast từng nhóm cột theo đúng kiểu dữ liệu mong muốn.
    df = (
        df
        .withColumn("vendorid", F.col("vendorid").cast(T.LongType()))
        .withColumn(
            "tpep_pickup_datetime",
            F.to_timestamp(F.col("tpep_pickup_datetime"), "yyyy-MM-dd HH:mm:ss"),
        )
        .withColumn(
            "tpep_dropoff_datetime",
            F.to_timestamp(F.col("tpep_dropoff_datetime"), "yyyy-MM-dd HH:mm:ss"),
        )
        .withColumn("passenger_count", F.col("passenger_count").cast(T.DoubleType()))
        .withColumn("trip_distance", F.col("trip_distance").cast(T.DoubleType()))
        .withColumn("ratecodeid", F.col("ratecodeid").cast(T.DoubleType()))
        .withColumn("store_and_fwd_flag", F.trim(F.col("store_and_fwd_flag")))
        .withColumn("pulocationid", F.col("pulocationid").cast(T.LongType()))
        .withColumn("dolocationid", F.col("dolocationid").cast(T.LongType()))
        .withColumn("payment_type", F.col("payment_type").cast(T.LongType()))
        .withColumn("fare_amount", F.col("fare_amount").cast(T.DoubleType()))
        .withColumn("extra", F.col("extra").cast(T.DoubleType()))
        .withColumn("mta_tax", F.col("mta_tax").cast(T.DoubleType()))
        .withColumn("tip_amount", F.col("tip_amount").cast(T.DoubleType()))
        .withColumn("tolls_amount", F.col("tolls_amount").cast(T.DoubleType()))
        .withColumn("improvement_surcharge", F.col("improvement_surcharge").cast(T.DoubleType()))
        .withColumn("total_amount", F.col("total_amount").cast(T.DoubleType()))
        .withColumn("congestion_surcharge", F.col("congestion_surcharge").cast(T.DoubleType()))
        .withColumn("airport_fee", F.col("airport_fee").cast(T.DoubleType()))
    )

    # Tạo cột ngày để downstream query / partition / aggregate dễ hơn.
    df = df.withColumn("pickup_date", F.to_date(F.col("tpep_pickup_datetime")))

    # Tạo cột partition từ pickup_date.
    df = df.withColumn("pickup_year", F.year(F.col("pickup_date")))
    df = df.withColumn("pickup_month", F.month(F.col("pickup_date")))

    # Thêm metadata kỹ thuật để debug lineage.
    df = df.withColumn("source_file", F.input_file_name())
    df = df.withColumn("bronze_loaded_at", F.current_timestamp())

    # Ở bronze mình lọc bỏ record không parse được pickup_date,
    df = df.filter(F.col("pickup_date").isNotNull())

    # Chọn lại thứ tự cột cuối cùng cho đẹp và ổn định.
    final_columns = [
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

    return df.select(*final_columns)


# Transform weather raw CSV -> bronze weather parquet.
def transform_weather_to_bronze(df: DataFrame) -> DataFrame:
    df = _ensure_columns(df, WEATHER_RAW_COLUMNS)

    df = (
        df
        .withColumn("date", F.to_date(F.col("date"), "yyyy-MM-dd"))
        .withColumn("temperature_2m_max", F.col("temperature_2m_max").cast(T.DoubleType()))
        .withColumn("temperature_2m_min", F.col("temperature_2m_min").cast(T.DoubleType()))
        .withColumn("temperature_2m_mean", F.col("temperature_2m_mean").cast(T.DoubleType()))
        .withColumn("precipitation_sum", F.col("precipitation_sum").cast(T.DoubleType()))
        .withColumn("snowfall_sum", F.col("snowfall_sum").cast(T.DoubleType()))
    )

    df = df.withColumn("weather_year", F.year(F.col("date")))
    df = df.withColumn("weather_month", F.month(F.col("date")))
    df = df.withColumn("source_file", F.input_file_name())
    df = df.withColumn("bronze_loaded_at", F.current_timestamp())

    # Weather phải có date để partition / join downstream.
    df = df.filter(F.col("date").isNotNull())

    final_columns = [
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

    return df.select(*final_columns)


# Transform zone raw CSV -> bronze zone parquet.
def transform_zone_to_bronze(df: DataFrame) -> DataFrame:
    # Rename từ source column sang column chuẩn bronze.
    rename_map = {
        "LocationID": "location_id",
        "Borough": "borough",
        "Zone": "zone",
        "service_zone": "service_zone",
    }

    for source_col, target_col in rename_map.items():
        if source_col in df.columns and source_col != target_col:
            df = df.withColumnRenamed(source_col, target_col)

    # Đảm bảo đủ cột sau rename.
    zone_final_columns = ["location_id", "borough", "zone", "service_zone"]
    df = _ensure_columns(df, zone_final_columns)

    # Cast type + thêm metadata.
    df = (
        df
        .withColumn("location_id", F.col("location_id").cast(T.IntegerType()))
        .withColumn("borough", F.trim(F.col("borough")))
        .withColumn("zone", F.trim(F.col("zone")))
        .withColumn("service_zone", F.trim(F.col("service_zone")))
        .withColumn("source_file", F.input_file_name())
        .withColumn("bronze_loaded_at", F.current_timestamp())
    )

    # Drop duplicate nhẹ cho zone lookup.
    df = df.dropDuplicates(["location_id", "borough", "zone", "service_zone"])

    # Không giữ record không có location_id.
    df = df.filter(F.col("location_id").isNotNull())

    final_columns = [
        "location_id",
        "borough",
        "zone",
        "service_zone",
        "source_file",
        "bronze_loaded_at",
    ]

    return df.select(*final_columns)


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


# Hàm main chạy toàn bộ flow raw -> bronze:
def main() -> None:
    # Load config từ project.
    config = load_app_config()

    # Resolve toàn bộ path cần dùng.
    paths = _resolve_paths(config)

    # Tạo logger theo cùng style với pipeline hiện tại.
    logger = get_logger(
        name="spark_raw_to_bronze",
        log_dir=config["paths"]["log_dir"],
    )

    logger.info("=" * 50)
    logger.info("START SPARK RAW -> BRONZE")
    logger.info("=" * 50)

    logger.info("Resolved paths: %s", {k: str(v) for k, v in paths.items()})

    # Tạo SparkSession.
    spark = build_spark(app_name="spark_raw_to_bronze")

    try:
        # TAXI
        logger.info("Reading taxi raw CSV...")

        taxi_raw_df = read_taxi_raw_csv(
            spark=spark,
            input_glob=str(paths["taxi_input_glob"]),
            logger=logger,
        )

        logger.info("Transforming taxi raw -> bronze...")

        taxi_bronze_df = transform_taxi_to_bronze(taxi_raw_df)
        log_dataframe_overview(logger, "taxi_bronze", taxi_bronze_df)

        logger.info("Writing taxi bronze parquet...")

        write_parquet(
            df=taxi_bronze_df,
            output_path=paths["bronze_taxi_dir"],
            mode="overwrite",
            partition_cols=["pickup_year", "pickup_month"],
        )

        logger.info("Taxi bronze written to: %s", paths["bronze_taxi_dir"])

        # WEATHER
        logger.info("Reading weather raw CSV...")

        weather_raw_df = read_weather_raw_csv(
            spark=spark,
            input_glob=str(paths["weather_input_glob"]),
            logger=logger,
        )

        logger.info("Transforming weather raw -> bronze...")

        weather_bronze_df = transform_weather_to_bronze(weather_raw_df)
        log_dataframe_overview(logger, "weather_bronze", weather_bronze_df)

        logger.info("Writing weather bronze parquet...")

        write_parquet(
            df=weather_bronze_df,
            output_path=paths["bronze_weather_dir"],
            mode="overwrite",
            partition_cols=["weather_year", "weather_month"],
        )

        logger.info("Weather bronze written to: %s", paths["bronze_weather_dir"])

        # ZONE
        logger.info("Reading zone raw CSV...")

        zone_raw_df = read_zone_raw_csv(
            spark=spark,
            input_glob=str(paths["zone_input_glob"]),
            logger=logger,
        )

        logger.info("Transforming zone raw -> bronze...")

        zone_bronze_df = transform_zone_to_bronze(zone_raw_df)
        log_dataframe_overview(logger, "zone_bronze", zone_bronze_df)

        logger.info("Writing zone bronze parquet...")

        write_parquet(
            df=zone_bronze_df,
            output_path=paths["bronze_zone_dir"],
            mode="overwrite",
            partition_cols=None,
        )

        logger.info("Zone bronze written to: %s", paths["bronze_zone_dir"])

        logger.info("=" * 60)
        logger.info("SPARK RAW -> BRONZE FINISHED SUCCESSFULLY")
        logger.info("=" * 60)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()