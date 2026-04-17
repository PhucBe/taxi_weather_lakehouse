from __future__ import annotations

from calendar import monthrange
from pathlib import Path
from typing import Any
import json
import re

import requests


YEAR_MONTH_PATTERN = re.compile(r"^\d{4}-\d{2}$")


def _validate_year_month(year_month: str) -> tuple[int, int]:
    """
    Validate định dạng YYYY-MM và trả về (year, month).
    """
    if not isinstance(year_month, str) or not YEAR_MONTH_PATTERN.match(year_month):
        raise ValueError(f"Invalid year_month format: {year_month}. Expected YYYY-MM")

    year_str, month_str = year_month.split("-")
    year = int(year_str)
    month = int(month_str)

    if month < 1 or month > 12:
        raise ValueError(f"Invalid month in year_month: {year_month}")

    return year, month


def _build_month_date_range(year_month: str) -> tuple[str, str]:
    """
    Trả về (start_date, end_date) cho 1 tháng.
    Ví dụ:
    2023-01 -> ('2023-01-01', '2023-01-31')
    """
    year, month = _validate_year_month(year_month)
    last_day = monthrange(year, month)[1]

    start_date = f"{year:04d}-{month:02d}-01"
    end_date = f"{year:04d}-{month:02d}-{last_day:02d}"
    return start_date, end_date


def build_weather_date_range(year_months: list[str]) -> tuple[str, str]:
    """
    Từ danh sách year_months, lấy khoảng ngày bao trùm từ tháng sớm nhất
    đến tháng muộn nhất.

    Ví dụ:
    ['2023-01', '2023-02', '2023-03']
    -> ('2023-01-01', '2023-03-31')
    """
    if not isinstance(year_months, list) or not year_months:
        raise ValueError("year_months must be a non-empty list[str]")

    validated = [_validate_year_month(item) for item in year_months]
    validated.sort()

    first_year, first_month = validated[0]
    last_year, last_month = validated[-1]

    start_date = f"{first_year:04d}-{first_month:02d}-01"
    last_day = monthrange(last_year, last_month)[1]
    end_date = f"{last_year:04d}-{last_month:02d}-{last_day:02d}"

    return start_date, end_date


def build_output_path(config: dict[str, Any], start_date: str, end_date: str) -> Path:
    """
    Tạo đường dẫn local để lưu raw weather JSON.
    """
    weather_dir = Path(config["paths"]["weather_dir"])
    return (
        weather_dir
        / "json"
        / f"start_date={start_date}"
        / f"end_date={end_date}"
        / f"open_meteo_daily_{start_date}_{end_date}.json"
    )


def build_weather_request_params(
    config: dict[str, Any],
    start_date: str,
    end_date: str,
) -> dict[str, Any]:
    """
    Tạo params gọi Open-Meteo từ config.
    """
    weather_cfg = config["weather"]
    timezone_name = weather_cfg.get("timezone") or config["project"]["timezone"]
    daily_variables = weather_cfg["daily_variables"]

    return {
        "latitude": weather_cfg["latitude"],
        "longitude": weather_cfg["longitude"],
        "start_date": start_date,
        "end_date": end_date,
        "daily": ",".join(daily_variables),
        "timezone": timezone_name,
    }


def _validate_weather_payload(payload: dict[str, Any], expected_daily_variables: list[str]) -> int:
    """
    Validate payload trả về từ Open-Meteo.
    Trả về số dòng daily.
    """
    if not isinstance(payload, dict):
        raise ValueError("Weather API payload must be a dictionary")

    daily = payload.get("daily")
    if not isinstance(daily, dict):
        raise ValueError("Weather API response does not contain valid 'daily' field")

    time_values = daily.get("time")
    if not isinstance(time_values, list) or not time_values:
        raise ValueError("Weather API returned 0 daily rows")

    row_count = len(time_values)

    for field in expected_daily_variables:
        if field not in daily:
            raise ValueError(f"Weather API response missing daily field: {field}")

        values = daily[field]
        if not isinstance(values, list):
            raise ValueError(f"Weather API field '{field}' must be a list")

        if len(values) != row_count:
            raise ValueError(
                f"Weather API field '{field}' length mismatch: expected {row_count}, got {len(values)}"
            )

    return row_count


def fetch_weather_raw(
    config: dict[str, Any],
    year_months: list[str],
    logger,
    overwrite: bool = False,
    timeout: int = 120,
) -> dict[str, Any]:
    """
    Fetch weather raw từ Open-Meteo cho toàn bộ khoảng tháng trong year_months.

    Trả về metadata + payload, ví dụ:
    {
        "dataset": "weather",
        "start_date": "2023-01-01",
        "end_date": "2023-03-31",
        "local_path": ".../open_meteo_daily_2023-01-01_2023-03-31.json",
        "filename": "open_meteo_daily_2023-01-01_2023-03-31.json",
        "file_format": "json",
        "payload": {...},
        "source_url": "...",
        "daily_row_count": 90,
    }
    """
    start_date, end_date = build_weather_date_range(year_months)
    output_path = build_output_path(config=config, start_date=start_date, end_date=end_date)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    expected_daily_variables = config["weather"]["daily_variables"]

    if output_path.exists() and not overwrite:
        logger.info("Weather raw JSON already exists, skip API call: %s", output_path)

        with output_path.open("r", encoding="utf-8") as f:
            payload = json.load(f)

        row_count = _validate_weather_payload(
            payload=payload,
            expected_daily_variables=expected_daily_variables,
        )

        return {
            "dataset": "weather",
            "start_date": start_date,
            "end_date": end_date,
            "local_path": str(output_path),
            "filename": output_path.name,
            "file_format": "json",
            "payload": payload,
            "source_url": payload.get("_request_url", ""),
            "daily_row_count": row_count,
        }

    base_url = config["source_urls"]["weather_base_url"]
    params = build_weather_request_params(
        config=config,
        start_date=start_date,
        end_date=end_date,
    )

    logger.info(
        "Calling weather API | start_date=%s | end_date=%s | latitude=%s | longitude=%s | timezone=%s | daily=%s",
        start_date,
        end_date,
        params["latitude"],
        params["longitude"],
        params["timezone"],
        params["daily"],
    )

    try:
        response = requests.get(base_url, params=params, timeout=timeout)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(
            f"Failed to fetch weather API for range {start_date} -> {end_date}"
        ) from exc

    payload = response.json()
    row_count = _validate_weather_payload(
        payload=payload,
        expected_daily_variables=expected_daily_variables,
    )

    # Lưu thêm request URL vào raw JSON để trace/debug dễ hơn
    payload["_request_url"] = response.url

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    logger.info(
        "Saved weather raw JSON | path=%s | daily_row_count=%s",
        output_path,
        row_count,
    )

    return {
        "dataset": "weather",
        "start_date": start_date,
        "end_date": end_date,
        "local_path": str(output_path),
        "filename": output_path.name,
        "file_format": "json",
        "payload": payload,
        "source_url": response.url,
        "daily_row_count": row_count,
    }