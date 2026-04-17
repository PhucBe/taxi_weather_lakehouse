from __future__ import annotations

from pathlib import Path
from typing import Any
import csv
import json


# Thứ tự cột đầu ra phải khớp đúng schema weather raw đã chốt
OUTPUT_COLUMNS = [
    "date",
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "snowfall_sum",
]


def _load_weather_payload(weather_raw: dict[str, Any]) -> dict[str, Any]:
    """
    Lấy payload weather từ:
    1) weather_raw["payload"] nếu đã có sẵn trong memory
    2) weather_raw["local_path"] nếu cần đọc lại từ file JSON
    """
    payload = weather_raw.get("payload")
    if isinstance(payload, dict):
        return payload

    local_path = weather_raw.get("local_path")
    if not local_path:
        raise ValueError(
            "weather_raw must contain either 'payload' or 'local_path'"
        )

    json_path = Path(local_path)
    if not json_path.exists():
        raise FileNotFoundError(f"Weather raw JSON not found: {json_path}")

    with json_path.open("r", encoding="utf-8") as f:
        payload = json.load(f)

    if not isinstance(payload, dict):
        raise ValueError("Weather raw JSON payload must be a dictionary")

    return payload


def _validate_daily_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """
    Validate phần daily trong payload Open-Meteo.
    Đồng thời đảm bảo các field cần cho CSV flat đều có đủ và cùng độ dài.
    """
    daily = payload.get("daily")
    if not isinstance(daily, dict):
        raise ValueError("Weather payload does not contain valid 'daily' field")

    if "time" not in daily or not isinstance(daily["time"], list) or not daily["time"]:
        raise ValueError("Weather payload 'daily.time' is missing or empty")

    row_count = len(daily["time"])

    required_source_fields = [
        "time",
        "temperature_2m_max",
        "temperature_2m_min",
        "temperature_2m_mean",
        "precipitation_sum",
        "snowfall_sum",
    ]

    for field in required_source_fields:
        if field not in daily:
            raise ValueError(f"Weather payload missing daily field: {field}")

        values = daily[field]
        if not isinstance(values, list):
            raise ValueError(f"Weather payload field '{field}' must be a list")

        if len(values) != row_count:
            raise ValueError(
                f"Weather payload field '{field}' length mismatch: expected {row_count}, got {len(values)}"
            )

    return daily


def _build_output_path(
    config: dict[str, Any],
    start_date: str,
    end_date: str,
) -> Path:
    """
    Tạo đường dẫn output cho weather flat CSV.

    Ví dụ:
    data/raw/weather/flat/start_date=2023-01-01/end_date=2023-03-31/weather_daily_2023-01-01_2023-03-31.csv
    """
    weather_dir = Path(config["paths"]["weather_dir"])
    return (
        weather_dir
        / "flat"
        / f"start_date={start_date}"
        / f"end_date={end_date}"
        / f"weather_daily_{start_date}_{end_date}.csv"
    )


def prepare_weather_flat_file(
    config: dict[str, Any],
    weather_raw: dict[str, Any],
    logger,
    overwrite: bool = False,
) -> Path:
    """
    Chuyển weather raw JSON thành flat CSV để upload S3 và COPY vào Redshift raw.

    Đầu vào mong đợi:
    weather_raw = {
        "start_date": "2023-01-01",
        "end_date": "2023-03-31",
        "local_path": "...json",
        "payload": {...},   # optional nếu đã có trong memory
        ...
    }

    Đầu ra:
    Path tới file CSV flat.
    """
    if not isinstance(weather_raw, dict):
        raise ValueError("weather_raw must be a dictionary")

    start_date = weather_raw.get("start_date")
    end_date = weather_raw.get("end_date")

    if not start_date or not end_date:
        raise ValueError("weather_raw must contain 'start_date' and 'end_date'")

    output_path = _build_output_path(
        config=config,
        start_date=start_date,
        end_date=end_date,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_path.exists() and not overwrite:
        logger.info("Weather flat CSV already exists, skip rebuild: %s", output_path)
        return output_path

    payload = _load_weather_payload(weather_raw)
    daily = _validate_daily_payload(payload)

    row_count = len(daily["time"])

    logger.info(
        "Preparing weather flat CSV | start_date=%s | end_date=%s | row_count=%s | output=%s",
        start_date,
        end_date,
        row_count,
        output_path,
    )

    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()

        for i in range(row_count):
            writer.writerow(
                {
                    "date": daily["time"][i],
                    "temperature_2m_max": daily["temperature_2m_max"][i],
                    "temperature_2m_min": daily["temperature_2m_min"][i],
                    "temperature_2m_mean": daily["temperature_2m_mean"][i],
                    "precipitation_sum": daily["precipitation_sum"][i],
                    "snowfall_sum": daily["snowfall_sum"][i],
                }
            )

    logger.info(
        "Weather flat CSV created successfully | path=%s | row_count=%s",
        output_path,
        row_count,
    )

    return output_path