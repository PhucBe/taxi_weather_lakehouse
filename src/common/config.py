from __future__ import annotations
from dotenv import load_dotenv
from pathlib import Path
from typing import Any
import os
import re
import yaml


ROOT_DIR = Path(__file__).resolve().parents[2] # Thư mục gốc project
CONFIG_PATH = ROOT_DIR / "config" / "settings.yml" # Chốt cứng 1 file config duy nhất
YEAR_MONTH_PATTERN = re.compile(r"^\d{4}-\d{2}$") # Validate định dạng YYYY-MM


def _required_env(name: str) -> str:
    """Lấy biến môi trường bắt buộc. Thiếu hoặc rỗng thì fail sớm."""
    value = os.getenv(name)
    if value is None or str(value).strip() == "":
        raise ValueError(f"Missing required environment variable: {name}")
    return value.strip()


def _require_section(config: dict[str, Any], section_name: str) -> dict[str, Any]:
    """Đảm bảo section tồn tại và là dict."""
    section = config.get(section_name)
    if not isinstance(section, dict):
        raise ValueError(f"Missing or invalid '{section_name}' section in config file")
    return section


def _require_key(section: dict[str, Any], key_name: str, full_name: str) -> Any:
    """Đảm bảo key tồn tại trong section."""
    if key_name not in section:
        raise ValueError(f"Missing '{full_name}' in config file")
    return section[key_name]


def _resolve_paths(config: dict[str, Any]) -> None:
    """
    Đổi tất cả key trong paths có hậu tố _dir hoặc _path
    từ đường dẫn tương đối thành tuyệt đối theo ROOT_DIR.
    """
    paths = _require_section(config, "paths")

    for key, value in list(paths.items()):
        if isinstance(value, str) and (key.endswith("_dir") or key.endswith("_path")):
            paths[key] = str((ROOT_DIR / value).resolve())

    config["paths"] = paths


def _normalize_prototype(config: dict[str, Any]) -> None:
    prototype = config.get("prototype")
    if not isinstance(prototype, dict):
        raise ValueError("Missing or invalid 'prototype' section in config file")

    year_months = prototype.get("year_months")
    if not isinstance(year_months, list) or not year_months:
        raise ValueError("prototype.year_months must be a non-empty list")

    for ym in year_months:
        if not isinstance(ym, str) or not YEAR_MONTH_PATTERN.match(ym):
            raise ValueError(
                "Each item in prototype.year_months must match format YYYY-MM"
            )


def _validate_weather(config: dict[str, Any]) -> None:
    """Validate section weather nếu có."""
    weather = config.get("weather")
    if weather is None:
        return

    if not isinstance(weather, dict):
        raise ValueError("Invalid 'weather' section in config file")

    if "latitude" in weather and not isinstance(weather["latitude"], (int, float)):
        raise ValueError("weather.latitude must be a number")

    if "longitude" in weather and not isinstance(weather["longitude"], (int, float)):
        raise ValueError("weather.longitude must be a number")

    if "daily_variables" in weather:
        daily_variables = weather["daily_variables"]
        if not isinstance(daily_variables, list) or not all(
            isinstance(x, str) and x.strip() for x in daily_variables
        ):
            raise ValueError("weather.daily_variables must be a non-empty list[str]")


def _validate_source_urls(config: dict[str, Any]) -> None:
    """Validate section source_urls nếu có."""
    source_urls = config.get("source_urls")
    if source_urls is None:
        return

    if not isinstance(source_urls, dict):
        raise ValueError("Invalid 'source_urls' section in config file")

    for key in ("taxi_url_template", "weather_base_url", "zone_lookup_url"):
        if key in source_urls and not isinstance(source_urls[key], str):
            raise ValueError(f"source_urls.{key} must be a string")


def _inject_redshift_runtime_values(config: dict[str, Any]) -> None:
    """Đọc thông tin Redshift từ .env."""
    redshift = _require_section(config, "redshift")

    redshift["host"] = _required_env("REDSHIFT_HOST")
    redshift["port"] = int(os.getenv("REDSHIFT_PORT", "5439"))
    redshift["database"] = _required_env("REDSHIFT_DATABASE")
    redshift["user"] = _required_env("REDSHIFT_USER")
    redshift["password"] = _required_env("REDSHIFT_PASSWORD")
    redshift["iam_role_arn"] = _required_env("REDSHIFT_IAM_ROLE_ARN")

    config["redshift"] = redshift


def _validate_required_sections(config: dict[str, Any]) -> None:
    """Validate các section/key cốt lõi cho raw ingestion -> S3 -> Redshift raw."""
    project = _require_section(config, "project")
    paths = _require_section(config, "paths")
    aws = _require_section(config, "aws")
    redshift = _require_section(config, "redshift")
    ingestion = _require_section(config, "ingestion")

    _require_key(project, "name", "project.name")
    _require_key(project, "env", "project.env")
    _require_key(project, "timezone", "project.timezone")

    _require_key(paths, "local_data_dir", "paths.local_data_dir")
    _require_key(paths, "log_dir", "paths.log_dir")

    _require_key(aws, "region", "aws.region")
    _require_key(aws, "bucket_name", "aws.bucket_name")
    _require_key(aws, "s3_prefix_raw", "aws.s3_prefix_raw")
    _require_key(aws, "s3_prefix_taxi", "aws.s3_prefix_taxi")
    _require_key(aws, "s3_prefix_weather", "aws.s3_prefix_weather")
    _require_key(aws, "s3_prefix_zone", "aws.s3_prefix_zone")

    _require_key(redshift, "schema_raw", "redshift.schema_raw")

    _require_key(ingestion, "csv_delimiter", "ingestion.csv_delimiter")
    _require_key(ingestion, "encoding", "ingestion.encoding")
    _require_key(ingestion, "truncate_before_load", "ingestion.truncate_before_load")
    _require_key(ingestion, "fail_on_missing_file", "ingestion.fail_on_missing_file")


def load_app_config() -> dict[str, Any]:
    """
    Load app config từ duy nhất 1 file:
      config/settings.yml
    """
    load_dotenv(ROOT_DIR / ".env")

    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Config file not found: {CONFIG_PATH}")

    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    if not isinstance(config, dict):
        raise ValueError(f"Invalid YAML config format: {CONFIG_PATH}")

    _normalize_prototype(config)
    _validate_weather(config)
    _validate_source_urls(config)
    _validate_required_sections(config)
    _resolve_paths(config)
    _inject_redshift_runtime_values(config)

    config["runtime"] = {
        "root_dir": str(ROOT_DIR),
        "config_path": str(CONFIG_PATH),
    }

    return config