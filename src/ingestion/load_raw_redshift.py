from __future__ import annotations

from pathlib import Path
from typing import Iterable

import redshift_connector

from src.common.utils import sanitize_identifier


# Ghi nhớ các bảng đã truncate trong một lần chạy process hiện tại.
# Mục đích: nếu load nhiều file taxi CSV (2023-01, 2023-02, 2023-03),
# bảng raw_taxi_trips chỉ bị truncate 1 lần ở đầu run, không bị xóa lại ở mỗi tháng.
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


def _require_existing_file(path: str | Path, label: str) -> Path:
    """Đảm bảo file local tồn tại trước khi load."""
    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(f"{label} not found: {file_path}")
    if not file_path.is_file():
        raise ValueError(f"{label} is not a file: {file_path}")
    return file_path


def create_schema_if_not_exists(conn, schema_name: str, logger) -> None:
    """Đảm bảo schema raw tồn tại."""
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
    Điều này đặc biệt quan trọng khi taxi load theo nhiều tháng.
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


# =========================
# Table DDL helpers
# =========================
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


def create_taxi_raw_table_if_not_exists(conn, schema_name: str, table_name: str, logger) -> None:
    """
    Tạo bảng raw taxi theo schema CSV chuẩn hóa.

    Thứ tự cột phải khớp với file CSV do prepare_taxi_flat_file.py tạo ra.
    """
    columns = [
        ("vendorid", "bigint"),
        ("tpep_pickup_datetime", "timestamp"),
        ("tpep_dropoff_datetime", "timestamp"),
        ("passenger_count", "double precision"),
        ("trip_distance", "double precision"),
        ("ratecodeid", "double precision"),
        ("store_and_fwd_flag", "varchar(10)"),
        ("pulocationid", "bigint"),
        ("dolocationid", "bigint"),
        ("payment_type", "bigint"),
        ("fare_amount", "double precision"),
        ("extra", "double precision"),
        ("mta_tax", "double precision"),
        ("tip_amount", "double precision"),
        ("tolls_amount", "double precision"),
        ("improvement_surcharge", "double precision"),
        ("total_amount", "double precision"),
        ("congestion_surcharge", "double precision"),
        ("airport_fee", "double precision"),
    ]

    logger.info("Ensuring raw taxi table exists: %s.%s", schema_name, table_name)
    sql = _build_create_table_sql(schema_name, table_name, columns)
    _execute_sql(conn, sql)


def create_weather_raw_table_if_not_exists(conn, schema_name: str, table_name: str, logger) -> None:
    """
    Tạo bảng raw weather daily.
    Thứ tự cột phải khớp với weather flat CSV.
    """
    columns = [
        ("date", "date"),
        ("temperature_2m_max", "double precision"),
        ("temperature_2m_min", "double precision"),
        ("temperature_2m_mean", "double precision"),
        ("precipitation_sum", "double precision"),
        ("snowfall_sum", "double precision"),
    ]

    logger.info("Ensuring raw weather table exists: %s.%s", schema_name, table_name)
    sql = _build_create_table_sql(schema_name, table_name, columns)
    _execute_sql(conn, sql)


def create_zone_raw_table_if_not_exists(conn, schema_name: str, table_name: str, logger) -> None:
    """
    Tạo bảng raw zone lookup.
    Thứ tự cột phải khớp với zone lookup CSV.
    """
    columns = [
        ("location_id", "integer"),
        ("borough", "varchar(100)"),
        ("zone", "varchar(255)"),
        ("service_zone", "varchar(255)"),
    ]

    logger.info("Ensuring raw zone table exists: %s.%s", schema_name, table_name)
    sql = _build_create_table_sql(schema_name, table_name, columns)
    _execute_sql(conn, sql)


# =========================
# COPY helpers
# =========================
def copy_csv_from_s3_to_redshift(
    conn,
    schema_name: str,
    table_name: str,
    bucket_name: str,
    s3_key: str,
    iam_role_arn: str,
    region: str,
    delimiter: str,
    logger,
) -> None:
    """COPY CSV từ S3 vào Redshift."""
    schema_name = sanitize_identifier(schema_name)
    table_name = sanitize_identifier(table_name)
    s3_uri = f"s3://{bucket_name}/{s3_key}"

    sql = f"""
    copy {schema_name}.{table_name}
    from '{s3_uri}'
    iam_role '{iam_role_arn}'
    region '{region}'
    csv
    ignoreheader 1
    delimiter '{delimiter}'
    emptyasnull
    blanksasnull
    truncatecolumns
    acceptinvchars
    dateformat 'auto'
    timeformat 'auto';
    """

    logger.info("COPY CSV into %s.%s from %s", schema_name, table_name, s3_uri)
    _execute_sql(conn, sql)


# =========================
# Public orchestration helpers
# =========================
def load_taxi_csv_to_redshift_raw(
    config: dict,
    local_csv_path: str | Path,
    raw_table: str,
    bucket_name: str,
    s3_key: str,
    logger,
) -> None:
    """
    Load taxi CSV từ S3 vào bảng raw taxi trong Redshift.
    local_csv_path được dùng để fail sớm nếu file local chưa được tạo thành công.
    """
    local_csv_path = _require_existing_file(local_csv_path, "Taxi CSV")

    conn = get_redshift_connection(config)

    try:
        schema_name = config["redshift"]["schema_raw"]
        delimiter = config["ingestion"]["csv_delimiter"]

        create_schema_if_not_exists(conn, schema_name=schema_name, logger=logger)
        create_taxi_raw_table_if_not_exists(
            conn,
            schema_name=schema_name,
            table_name=raw_table,
            logger=logger,
        )

        _truncate_table_once_if_needed(
            conn,
            schema_name=schema_name,
            table_name=raw_table,
            truncate_before_load=config["ingestion"]["truncate_before_load"],
            logger=logger,
        )

        copy_csv_from_s3_to_redshift(
            conn=conn,
            schema_name=schema_name,
            table_name=raw_table,
            bucket_name=bucket_name,
            s3_key=s3_key,
            iam_role_arn=config["redshift"]["iam_role_arn"],
            region=config["aws"]["region"],
            delimiter=delimiter,
            logger=logger,
        )
    finally:
        conn.close()


def load_weather_csv_to_redshift_raw(
    config: dict,
    local_csv_path: str | Path,
    raw_table: str,
    bucket_name: str,
    s3_key: str,
    logger,
) -> None:
    """
    Load weather daily CSV từ S3 vào bảng raw weather trong Redshift.
    local_csv_path được dùng để fail sớm nếu file local chưa được tạo thành công.
    """
    local_csv_path = _require_existing_file(local_csv_path, "Weather CSV")

    conn = get_redshift_connection(config)

    try:
        schema_name = config["redshift"]["schema_raw"]
        delimiter = config["ingestion"]["csv_delimiter"]

        create_schema_if_not_exists(conn, schema_name=schema_name, logger=logger)
        create_weather_raw_table_if_not_exists(
            conn,
            schema_name=schema_name,
            table_name=raw_table,
            logger=logger,
        )

        _truncate_table_once_if_needed(
            conn,
            schema_name=schema_name,
            table_name=raw_table,
            truncate_before_load=config["ingestion"]["truncate_before_load"],
            logger=logger,
        )

        copy_csv_from_s3_to_redshift(
            conn=conn,
            schema_name=schema_name,
            table_name=raw_table,
            bucket_name=bucket_name,
            s3_key=s3_key,
            iam_role_arn=config["redshift"]["iam_role_arn"],
            region=config["aws"]["region"],
            delimiter=delimiter,
            logger=logger,
        )
    finally:
        conn.close()


def load_zone_csv_to_redshift_raw(
    config: dict,
    local_csv_path: str | Path,
    raw_table: str,
    bucket_name: str,
    s3_key: str,
    logger,
) -> None:
    """
    Load zone lookup CSV từ S3 vào bảng raw zone trong Redshift.
    local_csv_path được dùng để fail sớm nếu file local chưa được tải thành công.
    """
    local_csv_path = _require_existing_file(local_csv_path, "Zone lookup CSV")

    conn = get_redshift_connection(config)

    try:
        schema_name = config["redshift"]["schema_raw"]
        delimiter = config["ingestion"]["csv_delimiter"]

        create_schema_if_not_exists(conn, schema_name=schema_name, logger=logger)
        create_zone_raw_table_if_not_exists(
            conn,
            schema_name=schema_name,
            table_name=raw_table,
            logger=logger,
        )

        _truncate_table_once_if_needed(
            conn,
            schema_name=schema_name,
            table_name=raw_table,
            truncate_before_load=config["ingestion"]["truncate_before_load"],
            logger=logger,
        )

        copy_csv_from_s3_to_redshift(
            conn=conn,
            schema_name=schema_name,
            table_name=raw_table,
            bucket_name=bucket_name,
            s3_key=s3_key,
            iam_role_arn=config["redshift"]["iam_role_arn"],
            region=config["aws"]["region"],
            delimiter=delimiter,
            logger=logger,
        )
    finally:
        conn.close()