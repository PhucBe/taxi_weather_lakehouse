from __future__ import annotations
from pathlib import Path
from typing import Iterable
import boto3
import redshift_connector
from pyspark.sql import SparkSession
from src.common.utils import sanitize_identifier
from src.common.config import load_app_config
from src.common.logger import get_logger


# Ghi nhớ các bảng đã truncate trong một lần chạy process hiện tại.
# Mục đích:
# - nếu chạy export_all_serving_to_redshift() cho 4 marts liên tiếp,
#   mỗi bảng chỉ truncate đúng 1 lần.
_TRUNCATED_TABLES: set[str] = set()


# Reset trạng thái truncate giữa các lần chạy pipeline.
def reset_truncate_state() -> None:
    _TRUNCATED_TABLES.clear()


# Tạo kết nối Redshift từ config đã load.
def get_redshift_connection(config: dict):
    conn = redshift_connector.connect(
        host=config["redshift"]["host"],
        port=config["redshift"]["port"],
        database=config["redshift"]["database"],
        user=config["redshift"]["user"],
        password=config["redshift"]["password"],
    )
    conn.autocommit = True

    return conn


# Chạy một câu SQL đơn.
def _execute_sql(conn, sql: str) -> None:
    with conn.cursor() as cur:
        cur.execute(sql)


# Chạy query và trả về giá trị đầu tiên.
def _fetch_one_value(conn, sql: str):
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()

    return None if row is None else row[0]


# Tạo SparkSession cho job export serving -> Redshift.
def build_spark(app_name: str = "export_serving_to_redshift") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    return spark


# Ép mọi giá trị path thành pathlib.Path.
def _as_path(value) -> Path:
    if isinstance(value, Path):
        return value
    
    return Path(str(value))


# Tạo folder nếu chưa tồn tại.
def _ensure_directory(path: str | Path) -> Path:
    path = _as_path(path)
    path.mkdir(parents=True, exist_ok=True)

    return path


# Đảm bảo directory local tồn tại trước khi export/load.
def _require_existing_dir(path: str | Path, label: str) -> Path:
    dir_path = _as_path(path)

    if not dir_path.exists():
        raise FileNotFoundError(f"{label} not found: {dir_path}")
    
    if not dir_path.is_dir():
        raise ValueError(f"{label} is not a directory: {dir_path}")
    
    return dir_path


# Xóa thư mục local nếu đã tồn tại.
def _remove_dir_if_exists(path: str | Path) -> None:
    import shutil

    dir_path = _as_path(path)

    if dir_path.exists():
        shutil.rmtree(dir_path)


# Lấy local_data_dir từ config.
def _get_local_data_dir(config: dict) -> Path:
    return _as_path(config["paths"].get("local_data_dir", "data"))


# Lấy root directory của layer serving.
def _get_serving_dir(config: dict) -> Path:
    return _as_path(
        config["paths"].get("serving_dir", _get_local_data_dir(config) / "serving")
    )


# Lấy thư mục local tạm để export parquet không partition trước khi upload lên S3.
def _get_serving_export_local_dir(config: dict) -> Path:
    return _as_path(
        config["paths"].get(
            "serving_export_local_dir",
            _get_local_data_dir(config) / "export_serving_redshift",
        )
    )


# Lấy schema đích trên Redshift cho layer serving.
def _get_serving_schema(config: dict) -> str:
    return sanitize_identifier(
        config["redshift"].get(
            "schema_serving",
            config["redshift"].get("serving_schema", "serving"),
        )
    )


# Lấy tên bucket S3 từ config.
def _get_s3_bucket_name(config: dict) -> str:
    bucket_name = (
        config["aws"].get("s3_bucket")
        or config["aws"].get("bucket_name")
        or config["aws"].get("bucket")
    )

    if not bucket_name:
        raise ValueError(
            "Missing S3 bucket in config. Please provide one of: "
            "config['aws']['s3_bucket'|'bucket_name'|'bucket']"
        )
    return str(bucket_name)


# Lấy AWS region từ config.
def _get_aws_region(config: dict) -> str:
    return str(config["aws"]["region"])


# Lấy IAM role ARN dùng cho COPY vào Redshift.
def _get_redshift_iam_role_arn(config: dict) -> str:
    iam_role_arn = config["redshift"].get("iam_role_arn")

    if not iam_role_arn:
        raise ValueError("Missing config['redshift']['iam_role_arn']")
    
    return str(iam_role_arn)


# Lấy S3 prefix cho serving export.
def _get_serving_export_s3_prefix(config: dict) -> str:
    prefix = (
        config["aws"].get("serving_export_prefix")
        or config["aws"].get("s3_serving_export_prefix")
        or "serving_exports"
    )

    return str(prefix).strip("/")


# Có truncate trước khi load serving hay không.
def _get_truncate_before_load(config: dict) -> bool:
    if "serving" in config and "truncate_before_load" in config["serving"]:
        return bool(config["serving"]["truncate_before_load"])

    if "ingestion" in config and "truncate_before_load" in config["ingestion"]:
        return bool(config["ingestion"]["truncate_before_load"])

    return True


# Lấy directory local của từng mart trong layer serving.
def _get_local_serving_dir(config: dict, dataset_name: str) -> Path:
    serving_dir = _get_serving_dir(config)

    key_map = {
        "mart_daily_demand": "serving_mart_daily_demand_dir",
        "mart_daily_payment_mix": "serving_mart_daily_payment_mix_dir",
        "mart_weather_impact": "serving_mart_weather_impact_dir",
        "mart_zone_demand": "serving_mart_zone_demand_dir",
    }

    default_dir_map = {
        "mart_daily_demand": serving_dir / "mart_daily_demand",
        "mart_daily_payment_mix": serving_dir / "mart_daily_payment_mix",
        "mart_weather_impact": serving_dir / "mart_weather_impact",
        "mart_zone_demand": serving_dir / "mart_zone_demand",
    }

    if dataset_name not in key_map:
        raise ValueError(f"Unsupported serving dataset: {dataset_name}")

    return _as_path(config["paths"].get(key_map[dataset_name], default_dir_map[dataset_name]))


# Lấy directory local tạm cho parquet đã flatten của từng mart.
def _get_local_export_dir(config: dict, dataset_name: str) -> Path:
    export_root = _get_serving_export_local_dir(config)

    return export_root / dataset_name


# Lấy S3 prefix đích cho từng mart.
def _get_s3_prefix_for_dataset(config: dict, dataset_name: str) -> str:
    base_prefix = _get_serving_export_s3_prefix(config)

    return f"{base_prefix}/{dataset_name}".strip("/")


# Đảm bảo schema serving tồn tại.
def create_schema_if_not_exists(conn, schema_name: str, logger) -> None:
    schema_name = sanitize_identifier(schema_name)
    logger.info("Ensuring schema exists: %s", schema_name)
    sql = f"create schema if not exists {schema_name};"
    _execute_sql(conn, sql)


# Truncate bảng đúng 1 lần trong 1 run nếu config yêu cầu.
def _truncate_table_once_if_needed(conn, schema_name: str, table_name: str, truncate_before_load: bool, logger) -> None:
    if not truncate_before_load:
        return

    schema_name = sanitize_identifier(schema_name)
    table_name = sanitize_identifier(table_name)
    full_table_name = f"{schema_name}.{table_name}"

    if full_table_name in _TRUNCATED_TABLES:
        logger.info("Table already truncated in this run, skipping: %s", full_table_name)

        return

    logger.info("Truncating table once for this run: %s", full_table_name)

    sql = f"truncate table {full_table_name};"
    _execute_sql(conn, sql)
    _TRUNCATED_TABLES.add(full_table_name)


# Tạo SQL create table if not exists từ danh sách cột.
def _build_create_table_sql(schema_name: str, table_name: str, columns: Iterable[tuple[str, str]]) -> str:
    schema_name = sanitize_identifier(schema_name)
    table_name = sanitize_identifier(table_name)
    columns_sql = ",\n    ".join([f"{sanitize_identifier(col)} {dtype}" for col, dtype in columns])

    return f"""
    create table if not exists {schema_name}.{table_name} (
        {columns_sql}
    );
    """


# Tạo bảng mart_daily_demand trong Redshift. Thứ tự cột phải khớp với parquet export đã flatten.
def create_mart_daily_demand_table_if_not_exists(conn, schema_name: str, table_name: str, logger) -> None:
    columns = [
        ("pickup_date", "date"),
        ("pickup_year", "integer"),
        ("pickup_month", "integer"),
        ("quarter_num", "integer"),
        ("month_name", "varchar(20)"),
        ("day_num_in_month", "integer"),
        ("day_of_week_num", "integer"),
        ("day_of_week_name", "varchar(20)"),
        ("is_weekend", "boolean"),
        ("trip_count", "bigint"),
        ("total_passenger_count", "bigint"),
        ("total_trip_distance", "double precision"),
        ("total_trip_duration_minutes", "double precision"),
        ("total_revenue", "double precision"),
        ("total_fare_amount", "double precision"),
        ("total_tip_amount", "double precision"),
        ("total_tolls_amount", "double precision"),
        ("avg_trip_distance", "double precision"),
        ("avg_trip_duration_minutes", "double precision"),
        ("avg_total_amount", "double precision"),
        ("avg_fare_amount", "double precision"),
        ("avg_tip_amount", "double precision"),
        ("negative_total_amount_trip_count", "bigint"),
        ("serving_loaded_at", "timestamp"),
    ]

    logger.info("Ensuring serving table exists: %s.%s", schema_name, table_name)

    sql = _build_create_table_sql(schema_name, table_name, columns)
    _execute_sql(conn, sql)


# Tạo bảng mart_daily_payment_mix trong Redshift.
def create_mart_daily_payment_mix_table_if_not_exists(conn, schema_name: str, table_name: str, logger) -> None:
    columns = [
        ("pickup_date", "date"),
        ("pickup_year", "integer"),
        ("pickup_month", "integer"),
        ("payment_type_code", "integer"),
        ("payment_type_name", "varchar(50)"),
        ("trip_count", "bigint"),
        ("total_revenue", "double precision"),
        ("total_fare_amount", "double precision"),
        ("total_tip_amount", "double precision"),
        ("avg_total_amount", "double precision"),
        ("avg_fare_amount", "double precision"),
        ("avg_tip_amount", "double precision"),
        ("payment_trip_share_pct", "double precision"),
        ("payment_revenue_share_pct", "double precision"),
        ("serving_loaded_at", "timestamp"),
    ]

    logger.info("Ensuring serving table exists: %s.%s", schema_name, table_name)

    sql = _build_create_table_sql(schema_name, table_name, columns)
    _execute_sql(conn, sql)


# Tạo bảng mart_weather_impact trong Redshift.
def create_mart_weather_impact_table_if_not_exists(conn, schema_name: str, table_name: str, logger) -> None:
    columns = [
        ("pickup_date", "date"),
        ("pickup_year", "integer"),
        ("pickup_month", "integer"),
        ("day_of_week_num", "integer"),
        ("day_of_week_name", "varchar(20)"),
        ("is_weekend", "boolean"),
        ("weather_date", "date"),
        ("is_rainy_day", "boolean"),
        ("is_snowy_day", "boolean"),
        ("temperature_max", "double precision"),
        ("temperature_min", "double precision"),
        ("temperature_mean", "double precision"),
        ("precipitation_sum", "double precision"),
        ("snowfall_sum", "double precision"),
        ("trip_count", "bigint"),
        ("total_revenue", "double precision"),
        ("total_fare_amount", "double precision"),
        ("total_tip_amount", "double precision"),
        ("avg_total_amount", "double precision"),
        ("avg_trip_distance", "double precision"),
        ("avg_trip_duration_minutes", "double precision"),
        ("negative_total_amount_trip_count", "bigint"),
        ("serving_loaded_at", "timestamp"),
    ]

    logger.info("Ensuring serving table exists: %s.%s", schema_name, table_name)

    sql = _build_create_table_sql(schema_name, table_name, columns)
    _execute_sql(conn, sql)


# Tạo bảng mart_zone_demand trong Redshift.
def create_mart_zone_demand_table_if_not_exists(conn, schema_name: str, table_name: str, logger) -> None:
    columns = [
        ("pickup_date", "date"),
        ("pickup_year", "integer"),
        ("pickup_month", "integer"),
        ("pickup_location_id", "integer"),
        ("borough", "varchar(100)"),
        ("zone", "varchar(255)"),
        ("service_zone", "varchar(255)"),
        ("trip_count", "bigint"),
        ("total_revenue", "double precision"),
        ("total_fare_amount", "double precision"),
        ("total_tip_amount", "double precision"),
        ("avg_total_amount", "double precision"),
        ("avg_trip_distance", "double precision"),
        ("avg_trip_duration_minutes", "double precision"),
        ("negative_total_amount_trip_count", "bigint"),
        ("serving_loaded_at", "timestamp"),
    ]

    logger.info("Ensuring serving table exists: %s.%s", schema_name, table_name)

    sql = _build_create_table_sql(schema_name, table_name, columns)
    _execute_sql(conn, sql)


# Đọc serving parquet partitioned ở local rồi ghi lại thành parquet không partition.
def _flatten_serving_parquet_to_local_export(spark: SparkSession, source_dir: str | Path, export_dir: str | Path, ordered_columns: list[str], coalesce_n: int, dataset_name: str, logger) -> int:
    source_dir = _require_existing_dir(source_dir, f"{dataset_name} source dir")
    export_dir = _as_path(export_dir)

    logger.info("Flattening local serving parquet: %s", dataset_name)

    df = spark.read.parquet(str(source_dir)).select(*ordered_columns)
    row_count = df.count()

    logger.info("[%s] local row_count=%s", dataset_name, row_count)
    logger.info("[%s] columns=%s", dataset_name, ", ".join(df.columns))
    logger.info("[%s] schema=%s", dataset_name, df.schema.simpleString())

    _remove_dir_if_exists(export_dir)
    _ensure_directory(export_dir.parent)

    df.coalesce(coalesce_n).write.mode("overwrite").parquet(str(export_dir))

    return row_count


# Xóa object cũ trong S3 prefix để mỗi lần load là full refresh sạch.
def _delete_s3_prefix_if_exists(s3_client, bucket_name: str, s3_prefix: str, logger) -> None:
    logger.info("Deleting existing S3 prefix if exists: s3://%s/%s", bucket_name, s3_prefix)

    paginator = s3_client.get_paginator("list_objects_v2")
    objects_to_delete: list[dict[str, str]] = []

    for page in paginator.paginate(Bucket=bucket_name, Prefix=s3_prefix):
        for obj in page.get("Contents", []):
            objects_to_delete.append({"Key": obj["Key"]})

            if len(objects_to_delete) == 1000:
                s3_client.delete_objects(
                    Bucket=bucket_name,
                    Delete={"Objects": objects_to_delete},
                )
                objects_to_delete = []

    if objects_to_delete:
        s3_client.delete_objects(
            Bucket=bucket_name,
            Delete={"Objects": objects_to_delete},
        )


# Upload toàn bộ file parquet trong local_dir lên S3 prefix.
def _upload_parquet_directory_to_s3(s3_client, local_dir: str | Path, bucket_name: str, s3_prefix: str, logger) -> None:
    local_dir = _require_existing_dir(local_dir, "Local parquet export dir")
    parquet_files = sorted(local_dir.rglob("*.parquet"))

    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found to upload in {local_dir}")

    logger.info("Uploading %s parquet files to s3://%s/%s", len(parquet_files), bucket_name, s3_prefix)

    for file_path in parquet_files:
        relative_path = file_path.relative_to(local_dir).as_posix()
        s3_key = f"{s3_prefix}/{relative_path}".strip("/")
        s3_client.upload_file(str(file_path), bucket_name, s3_key)


# COPY parquet từ S3 vào Redshift.
def copy_parquet_from_s3_to_redshift(conn, schema_name: str, table_name: str, bucket_name: str, s3_prefix: str, iam_role_arn: str, logger) -> None:
    schema_name = sanitize_identifier(schema_name)
    table_name = sanitize_identifier(table_name)
    s3_uri = f"s3://{bucket_name}/{s3_prefix}/"

    sql = f"""
    copy {schema_name}.{table_name}
    from '{s3_uri}'
    iam_role '{iam_role_arn}'
    format as parquet;
    """

    logger.info("COPY PARQUET into %s.%s from %s", schema_name, table_name, s3_uri)

    _execute_sql(conn, sql)


# Danh sách cột chuẩn của mart_daily_demand.
def _get_mart_daily_demand_columns() -> list[str]:
    return [
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


# Danh sách cột chuẩn của mart_daily_payment_mix.
def _get_mart_daily_payment_mix_columns() -> list[str]:
    return [
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


# Danh sách cột chuẩn của mart_weather_impact.
def _get_mart_weather_impact_columns() -> list[str]:
    return [
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


# Danh sách cột chuẩn của mart_zone_demand.
def _get_mart_zone_demand_columns() -> list[str]:
    return [
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


# Generic helper để export 1 dataset serving vào Redshift.
def _export_one_serving_dataset_to_redshift(config: dict, dataset_name: str, table_name: str, ordered_columns: list[str], create_table_fn, coalesce_n: int, logger) -> None:
    schema_name = _get_serving_schema(config)
    bucket_name = _get_s3_bucket_name(config)
    region = _get_aws_region(config)
    iam_role_arn = _get_redshift_iam_role_arn(config)
    s3_prefix = _get_s3_prefix_for_dataset(config, dataset_name)
    local_source_dir = _get_local_serving_dir(config, dataset_name)
    local_export_dir = _get_local_export_dir(config, dataset_name)

    logger.info("=" * 60)
    logger.info("EXPORT DATASET TO REDSHIFT: %s", dataset_name)
    logger.info("=" * 60)
    logger.info("Local source dir: %s", local_source_dir)
    logger.info("Local export dir: %s", local_export_dir)
    logger.info("S3 target: s3://%s/%s", bucket_name, s3_prefix)
    logger.info("AWS region: %s", region)
    logger.info("Redshift target: %s.%s", schema_name, table_name)

    spark = build_spark(app_name=f"export_{dataset_name}_to_redshift")
    conn = get_redshift_connection(config)
    s3_client = boto3.client("s3", region_name=region)

    try:
        local_row_count = _flatten_serving_parquet_to_local_export(
            spark=spark,
            source_dir=local_source_dir,
            export_dir=local_export_dir,
            ordered_columns=ordered_columns,
            coalesce_n=coalesce_n,
            dataset_name=dataset_name,
            logger=logger
        )
        _delete_s3_prefix_if_exists(
            s3_client=s3_client,
            bucket_name=bucket_name,
            s3_prefix=s3_prefix,
            logger=logger
        )
        _upload_parquet_directory_to_s3(
            s3_client=s3_client,
            local_dir=local_export_dir,
            bucket_name=bucket_name,
            s3_prefix=s3_prefix,
            logger=logger
        )
        create_schema_if_not_exists(
            conn,
            schema_name=schema_name,
            logger=logger
        )
        create_table_fn(
            conn,
            schema_name=schema_name,
            table_name=table_name,
            logger=logger
        )
        _truncate_table_once_if_needed(
            conn,
            schema_name=schema_name,
            table_name=table_name,
            truncate_before_load=_get_truncate_before_load(config),
            logger=logger
        )
        copy_parquet_from_s3_to_redshift(
            conn=conn,
            schema_name=schema_name,
            table_name=table_name,
            bucket_name=bucket_name,
            s3_prefix=s3_prefix,
            iam_role_arn=iam_role_arn,
            logger=logger
        )
        redshift_row_count = _fetch_one_value(
            conn,
            f"select count(*) from {schema_name}.{sanitize_identifier(table_name)};"
        )

        logger.info("[%s] local_row_count=%s | redshift_row_count=%s", dataset_name, local_row_count, redshift_row_count)

        if int(redshift_row_count) != int(local_row_count):
            raise ValueError(
                f"[{dataset_name}] row count mismatch after Redshift load | "
                f"local={local_row_count}, redshift={redshift_row_count}"
            )

    finally:
        conn.close()
        spark.stop()


# Export mart_daily_demand vào Redshift serving schema.
def load_mart_daily_demand_to_redshift_serving(config: dict, logger) -> None:
    _export_one_serving_dataset_to_redshift(
        config=config,
        dataset_name="mart_daily_demand",
        table_name="mart_daily_demand",
        ordered_columns=_get_mart_daily_demand_columns(),
        create_table_fn=create_mart_daily_demand_table_if_not_exists,
        coalesce_n=1,
        logger=logger,
    )


# Export mart_daily_payment_mix vào Redshift serving schema.
def load_mart_daily_payment_mix_to_redshift_serving(config: dict, logger) -> None:
    _export_one_serving_dataset_to_redshift(
        config=config,
        dataset_name="mart_daily_payment_mix",
        table_name="mart_daily_payment_mix",
        ordered_columns=_get_mart_daily_payment_mix_columns(),
        create_table_fn=create_mart_daily_payment_mix_table_if_not_exists,
        coalesce_n=1,
        logger=logger,
    )


# Export mart_weather_impact vào Redshift serving schema.
def load_mart_weather_impact_to_redshift_serving(config: dict, logger) -> None:
    _export_one_serving_dataset_to_redshift(
        config=config,
        dataset_name="mart_weather_impact",
        table_name="mart_weather_impact",
        ordered_columns=_get_mart_weather_impact_columns(),
        create_table_fn=create_mart_weather_impact_table_if_not_exists,
        coalesce_n=1,
        logger=logger,
    )


# Export mart_zone_demand vào Redshift serving schema.
def load_mart_zone_demand_to_redshift_serving(config: dict, logger) -> None:
    _export_one_serving_dataset_to_redshift(
        config=config,
        dataset_name="mart_zone_demand",
        table_name="mart_zone_demand",
        ordered_columns=_get_mart_zone_demand_columns(),
        create_table_fn=create_mart_zone_demand_table_if_not_exists,
        coalesce_n=4,
        logger=logger,
    )


# Export toàn bộ 4 marts serving vào Redshift.
def export_all_serving_to_redshift(config: dict, logger) -> None:
    reset_truncate_state()
    load_mart_daily_demand_to_redshift_serving(config=config, logger=logger)
    load_mart_daily_payment_mix_to_redshift_serving(config=config, logger=logger)
    load_mart_weather_impact_to_redshift_serving(config=config, logger=logger)
    load_mart_zone_demand_to_redshift_serving(config=config, logger=logger)


def main() -> None:
    config = load_app_config()
    logger = get_logger(name="export_serving_to_redshift", log_dir=config["paths"]["log_dir"])

    logger.info("============================================================")
    logger.info("START EXPORT SERVING -> REDSHIFT")
    logger.info("============================================================")

    export_all_serving_to_redshift(config=config, logger=logger)

    logger.info("============================================================")
    logger.info("EXPORT SERVING -> REDSHIFT FINISHED SUCCESSFULLY")
    logger.info("============================================================")


if __name__ == "__main__":
    main()