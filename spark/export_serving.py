from __future__ import annotations

from pathlib import Path
from typing import Iterable

import boto3
import redshift_connector
from pyspark.sql import SparkSession

from src.common.utils import sanitize_identifier

try:
    from src.common.config import load_app_config
    from src.common.logger import get_logger
except ImportError:  # pragma: no cover
    from src.utils.config import load_app_config  # type: ignore
    from src.utils.logger import get_logger  # type: ignore


# Ghi nhớ các bảng đã truncate trong một lần chạy process hiện tại.
# Mục đích:
# - nếu chạy export_all_serving_to_redshift() cho 4 marts liên tiếp,
#   mỗi bảng chỉ truncate đúng 1 lần.
_TRUNCATED_TABLES: set[str] = set()


def reset_truncate_state() -> None:
    """Reset trạng thái truncate giữa các lần chạy pipeline."""
    _TRUNCATED_TABLES.clear()


# =========================
# Connection helpers
# =========================
def get_redshift_connection(config: dict):
    """Tạo kết nối Redshift từ config đã load."""
    conn = redshift_connector.connect(
        host=config["redshift"]["host"],
        port=config["redshift"]["port"],
        database=config["redshift"]["database"],
        user=config["redshift"]["user"],
        password=config["redshift"]["password"],
    )
    conn.autocommit = True
    return conn


def _execute_sql(conn, sql: str) -> None:
    """Chạy một câu SQL đơn."""
    with conn.cursor() as cur:
        cur.execute(sql)


def _fetch_one_value(conn, sql: str):
    """Chạy query và trả về giá trị đầu tiên."""
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
    return None if row is None else row[0]


def build_spark(app_name: str = "export_serving_to_redshift") -> SparkSession:
    """
    Tạo SparkSession cho job export serving -> Redshift.

    Spark ở đây chỉ dùng để:
    - đọc parquet partitioned ở local
    - materialize lại thành parquet không partition
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


# =========================
# Local path / config helpers
# =========================
def _as_path(value) -> Path:
    """Ép mọi giá trị path thành pathlib.Path."""
    if isinstance(value, Path):
        return value
    return Path(str(value))


def _ensure_directory(path: str | Path) -> Path:
    """Tạo folder nếu chưa tồn tại."""
    path = _as_path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _require_existing_dir(path: str | Path, label: str) -> Path:
    """Đảm bảo directory local tồn tại trước khi export/load."""
    dir_path = _as_path(path)
    if not dir_path.exists():
        raise FileNotFoundError(f"{label} not found: {dir_path}")
    if not dir_path.is_dir():
        raise ValueError(f"{label} is not a directory: {dir_path}")
    return dir_path


def _remove_dir_if_exists(path: str | Path) -> None:
    """Xóa thư mục local nếu đã tồn tại."""
    import shutil

    dir_path = _as_path(path)
    if dir_path.exists():
        shutil.rmtree(dir_path)


def _get_local_data_dir(config: dict) -> Path:
    """Lấy local_data_dir từ config."""
    return _as_path(config["paths"].get("local_data_dir", "data"))


def _get_serving_dir(config: dict) -> Path:
    """Lấy root directory của layer serving."""
    return _as_path(
        config["paths"].get("serving_dir", _get_local_data_dir(config) / "serving")
    )


def _get_serving_export_local_dir(config: dict) -> Path:
    """
    Lấy thư mục local tạm để export parquet không partition
    trước khi upload lên S3.
    """
    return _as_path(
        config["paths"].get(
            "serving_export_local_dir",
            _get_local_data_dir(config) / "export_serving_redshift",
        )
    )


def _get_serving_schema(config: dict) -> str:
    """
    Lấy schema đích trên Redshift cho layer serving.

    Ưu tiên:
    - redshift.schema_serving
    - redshift.serving_schema
    - fallback: serving
    """
    return sanitize_identifier(
        config["redshift"].get(
            "schema_serving",
            config["redshift"].get("serving_schema", "serving"),
        )
    )


def _get_s3_bucket_name(config: dict) -> str:
    """
    Lấy tên bucket S3 từ config.

    Ưu tiên:
    - aws.s3_bucket
    - aws.bucket_name
    - aws.bucket
    """
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


def _get_aws_region(config: dict) -> str:
    """Lấy AWS region từ config."""
    return str(config["aws"]["region"])


def _get_redshift_iam_role_arn(config: dict) -> str:
    """Lấy IAM role ARN dùng cho COPY vào Redshift."""
    iam_role_arn = config["redshift"].get("iam_role_arn")
    if not iam_role_arn:
        raise ValueError("Missing config['redshift']['iam_role_arn']")
    return str(iam_role_arn)


def _get_serving_export_s3_prefix(config: dict) -> str:
    """
    Lấy S3 prefix cho serving export.

    Ví dụ:
    serving_exports
    """
    prefix = (
        config["aws"].get("serving_export_prefix")
        or config["aws"].get("s3_serving_export_prefix")
        or "serving_exports"
    )
    return str(prefix).strip("/")


def _get_truncate_before_load(config: dict) -> bool:
    """
    Có truncate trước khi load serving hay không.

    Ưu tiên:
    - serving.truncate_before_load
    - ingestion.truncate_before_load
    - fallback: True
    """
    if "serving" in config and "truncate_before_load" in config["serving"]:
        return bool(config["serving"]["truncate_before_load"])

    if "ingestion" in config and "truncate_before_load" in config["ingestion"]:
        return bool(config["ingestion"]["truncate_before_load"])

    return True


def _get_local_serving_dir(config: dict, dataset_name: str) -> Path:
    """
    Lấy directory local của từng mart trong layer serving.
    """
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


def _get_local_export_dir(config: dict, dataset_name: str) -> Path:
    """
    Lấy directory local tạm cho parquet đã flatten của từng mart.
    """
    export_root = _get_serving_export_local_dir(config)
    return export_root / dataset_name


def _get_s3_prefix_for_dataset(config: dict, dataset_name: str) -> str:
    """
    Lấy S3 prefix đích cho từng mart.

    Ví dụ:
    serving_exports/mart_daily_demand
    """
    base_prefix = _get_serving_export_s3_prefix(config)
    return f"{base_prefix}/{dataset_name}".strip("/")


# =========================
# Table DDL helpers
# =========================
def create_schema_if_not_exists(conn, schema_name: str, logger) -> None:
    """Đảm bảo schema serving tồn tại."""
    schema_name = sanitize_identifier(schema_name)
    logger.info("Ensuring schema exists: %s", schema_name)
    sql = f"create schema if not exists {schema_name};"
    _execute_sql(conn, sql)


def _truncate_table_once_if_needed(
    conn,
    schema_name: str,
    table_name: str,
    truncate_before_load: bool,
    logger,
) -> None:
    """
    Truncate bảng đúng 1 lần trong 1 run nếu config yêu cầu.
    """
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


def _build_create_table_sql(
    schema_name: str,
    table_name: str,
    columns: Iterable[tuple[str, str]],
) -> str:
    """Tạo SQL create table if not exists từ danh sách cột."""
    schema_name = sanitize_identifier(schema_name)
    table_name = sanitize_identifier(table_name)

    columns_sql = ",\n    ".join(
        [f"{sanitize_identifier(col)} {dtype}" for col, dtype in columns]
    )

    return f"""
    create table if not exists {schema_name}.{table_name} (
        {columns_sql}
    );
    """


def create_mart_daily_demand_table_if_not_exists(
    conn,
    schema_name: str,
    table_name: str,
    logger,
) -> None:
    """
    Tạo bảng mart_daily_demand trong Redshift.
    Thứ tự cột phải khớp với parquet export đã flatten.
    """
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


def create_mart_daily_payment_mix_table_if_not_exists(
    conn,
    schema_name: str,
    table_name: str,
    logger,
) -> None:
    """
    Tạo bảng mart_daily_payment_mix trong Redshift.
    """
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


def create_mart_weather_impact_table_if_not_exists(
    conn,
    schema_name: str,
    table_name: str,
    logger,
) -> None:
    """
    Tạo bảng mart_weather_impact trong Redshift.
    """
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


def create_mart_zone_demand_table_if_not_exists(
    conn,
    schema_name: str,
    table_name: str,
    logger,
) -> None:
    """
    Tạo bảng mart_zone_demand trong Redshift.
    """
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


# =========================
# Local parquet flatten helpers
# =========================
def _flatten_serving_parquet_to_local_export(
    spark: SparkSession,
    source_dir: str | Path,
    export_dir: str | Path,
    ordered_columns: list[str],
    coalesce_n: int,
    dataset_name: str,
    logger,
) -> int:
    """
    Đọc serving parquet partitioned ở local rồi ghi lại thành parquet không partition.

    Vì sao cần bước này:
    - serving local hiện đang partition theo pickup_year / pickup_month
    - khi export sang Redshift, an toàn nhất là materialize lại đủ tất cả cột
      vào body của file parquet
    - sau đó COPY parquet từ S3 vào Redshift sẽ ít rủi ro hơn

    Return:
    - row_count của dataset export
    """
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


# =========================
# S3 upload helpers
# =========================
def _delete_s3_prefix_if_exists(
    s3_client,
    bucket_name: str,
    s3_prefix: str,
    logger,
) -> None:
    """
    Xóa object cũ trong S3 prefix để mỗi lần load là full refresh sạch.
    """
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


def _upload_parquet_directory_to_s3(
    s3_client,
    local_dir: str | Path,
    bucket_name: str,
    s3_prefix: str,
    logger,
) -> None:
    """
    Upload toàn bộ file parquet trong local_dir lên S3 prefix.

    Chỉ upload *.parquet:
    - tránh đẩy các file như _SUCCESS lên S3
    - giảm rủi ro COPY parquet bị lỗi vì object không phải parquet
    """
    local_dir = _require_existing_dir(local_dir, "Local parquet export dir")

    parquet_files = sorted(local_dir.rglob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found to upload in {local_dir}")

    logger.info(
        "Uploading %s parquet files to s3://%s/%s",
        len(parquet_files),
        bucket_name,
        s3_prefix,
    )

    for file_path in parquet_files:
        relative_path = file_path.relative_to(local_dir).as_posix()
        s3_key = f"{s3_prefix}/{relative_path}".strip("/")
        s3_client.upload_file(str(file_path), bucket_name, s3_key)


# =========================
# COPY helpers
# =========================
def copy_parquet_from_s3_to_redshift(
    conn,
    schema_name: str,
    table_name: str,
    bucket_name: str,
    s3_prefix: str,
    iam_role_arn: str,
    logger,
) -> None:
    """
    COPY parquet từ S3 vào Redshift.

    Lưu ý:
    - dùng FORMAT AS PARQUET
    - không dùng csv options như raw layer
    """
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


# =========================
# Dataset specs helpers
# =========================
def _get_mart_daily_demand_columns() -> list[str]:
    """Danh sách cột chuẩn của mart_daily_demand."""
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


def _get_mart_daily_payment_mix_columns() -> list[str]:
    """Danh sách cột chuẩn của mart_daily_payment_mix."""
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


def _get_mart_weather_impact_columns() -> list[str]:
    """Danh sách cột chuẩn của mart_weather_impact."""
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


def _get_mart_zone_demand_columns() -> list[str]:
    """Danh sách cột chuẩn của mart_zone_demand."""
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


# =========================
# Generic internal loader
# =========================
def _export_one_serving_dataset_to_redshift(
    config: dict,
    dataset_name: str,
    table_name: str,
    ordered_columns: list[str],
    create_table_fn,
    coalesce_n: int,
    logger,
) -> None:
    """
    Generic helper để export 1 dataset serving vào Redshift.

    Flow:
    1) đọc serving parquet partitioned ở local
    2) flatten thành parquet không partition ở local export dir
    3) upload parquet export lên S3
    4) create schema/table nếu chưa có
    5) truncate nếu config yêu cầu
    6) COPY parquet từ S3 vào Redshift
    7) verify row count local vs Redshift
    """
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
            logger=logger,
        )

        _delete_s3_prefix_if_exists(
            s3_client=s3_client,
            bucket_name=bucket_name,
            s3_prefix=s3_prefix,
            logger=logger,
        )

        _upload_parquet_directory_to_s3(
            s3_client=s3_client,
            local_dir=local_export_dir,
            bucket_name=bucket_name,
            s3_prefix=s3_prefix,
            logger=logger,
        )

        create_schema_if_not_exists(conn, schema_name=schema_name, logger=logger)
        create_table_fn(
            conn,
            schema_name=schema_name,
            table_name=table_name,
            logger=logger,
        )

        _truncate_table_once_if_needed(
            conn,
            schema_name=schema_name,
            table_name=table_name,
            truncate_before_load=_get_truncate_before_load(config),
            logger=logger,
        )

        copy_parquet_from_s3_to_redshift(
            conn=conn,
            schema_name=schema_name,
            table_name=table_name,
            bucket_name=bucket_name,
            s3_prefix=s3_prefix,
            iam_role_arn=iam_role_arn,
            logger=logger,
        )

        redshift_row_count = _fetch_one_value(
            conn,
            f"select count(*) from {schema_name}.{sanitize_identifier(table_name)};",
        )

        logger.info(
            "[%s] local_row_count=%s | redshift_row_count=%s",
            dataset_name,
            local_row_count,
            redshift_row_count,
        )

        if int(redshift_row_count) != int(local_row_count):
            raise ValueError(
                f"[{dataset_name}] row count mismatch after Redshift load | "
                f"local={local_row_count}, redshift={redshift_row_count}"
            )

    finally:
        conn.close()
        spark.stop()


# =========================
# Public orchestration helpers
# =========================
def load_mart_daily_demand_to_redshift_serving(
    config: dict,
    logger,
) -> None:
    """Export mart_daily_demand vào Redshift serving schema."""
    _export_one_serving_dataset_to_redshift(
        config=config,
        dataset_name="mart_daily_demand",
        table_name="mart_daily_demand",
        ordered_columns=_get_mart_daily_demand_columns(),
        create_table_fn=create_mart_daily_demand_table_if_not_exists,
        coalesce_n=1,
        logger=logger,
    )


def load_mart_daily_payment_mix_to_redshift_serving(
    config: dict,
    logger,
) -> None:
    """Export mart_daily_payment_mix vào Redshift serving schema."""
    _export_one_serving_dataset_to_redshift(
        config=config,
        dataset_name="mart_daily_payment_mix",
        table_name="mart_daily_payment_mix",
        ordered_columns=_get_mart_daily_payment_mix_columns(),
        create_table_fn=create_mart_daily_payment_mix_table_if_not_exists,
        coalesce_n=1,
        logger=logger,
    )


def load_mart_weather_impact_to_redshift_serving(
    config: dict,
    logger,
) -> None:
    """Export mart_weather_impact vào Redshift serving schema."""
    _export_one_serving_dataset_to_redshift(
        config=config,
        dataset_name="mart_weather_impact",
        table_name="mart_weather_impact",
        ordered_columns=_get_mart_weather_impact_columns(),
        create_table_fn=create_mart_weather_impact_table_if_not_exists,
        coalesce_n=1,
        logger=logger,
    )


def load_mart_zone_demand_to_redshift_serving(
    config: dict,
    logger,
) -> None:
    """Export mart_zone_demand vào Redshift serving schema."""
    _export_one_serving_dataset_to_redshift(
        config=config,
        dataset_name="mart_zone_demand",
        table_name="mart_zone_demand",
        ordered_columns=_get_mart_zone_demand_columns(),
        create_table_fn=create_mart_zone_demand_table_if_not_exists,
        coalesce_n=4,
        logger=logger,
    )


def export_all_serving_to_redshift(config: dict, logger) -> None:
    """
    Export toàn bộ 4 marts serving vào Redshift.

    Thứ tự chạy:
    1) mart_daily_demand
    2) mart_daily_payment_mix
    3) mart_weather_impact
    4) mart_zone_demand
    """
    reset_truncate_state()

    load_mart_daily_demand_to_redshift_serving(config=config, logger=logger)
    load_mart_daily_payment_mix_to_redshift_serving(config=config, logger=logger)
    load_mart_weather_impact_to_redshift_serving(config=config, logger=logger)
    load_mart_zone_demand_to_redshift_serving(config=config, logger=logger)


# =========================
# Script entrypoint
# =========================
def main() -> None:
    """
    Entry point để chạy export serving -> Redshift từ command line.
    """
    config = load_app_config()
    logger = get_logger(
        name="export_serving_to_redshift",
        log_dir=config["paths"]["log_dir"],
    )

    logger.info("============================================================")
    logger.info("START EXPORT SERVING -> REDSHIFT")
    logger.info("============================================================")

    export_all_serving_to_redshift(config=config, logger=logger)

    logger.info("============================================================")
    logger.info("EXPORT SERVING -> REDSHIFT FINISHED SUCCESSFULLY")
    logger.info("============================================================")


if __name__ == "__main__":
    main()