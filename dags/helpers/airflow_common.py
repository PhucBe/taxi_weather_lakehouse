from __future__ import annotations

# =========================================================
# airflow_common.py
# ---------------------------------------------------------
# Mục tiêu:
# - Chứa helper dùng chung cho nhiều DAG Airflow
# - Chuẩn hóa:
#     + default_args
#     + timezone
#     + params cho manual run / backfill
#     + failure callback
#     + runtime config parsing
#     + env injection cho task
#     + wrapper để gọi Python entrypoint trong project
#     + factory tạo PythonOperator / EmptyOperator
#
# Triết lý:
# - DAG file chỉ nên mô tả dependency graph
# - logic lặp lại đưa vào helpers
# - task nên chạy theo "entrypoint function" của codebase hiện có
# =========================================================

import importlib
import os
import re
from contextlib import contextmanager
from copy import deepcopy
from datetime import timedelta
from typing import Any

import pendulum
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, get_current_context
from airflow.utils.log.logging_mixin import LoggingMixin


# =========================================================
# 1) GLOBAL CONSTANTS
# =========================================================
PROJECT_TIMEZONE = pendulum.timezone("Asia/Ho_Chi_Minh")

DEFAULT_OWNER = "data-engineering"
DEFAULT_RETRIES = 1
DEFAULT_RETRY_DELAY_MINUTES = 5
DEFAULT_EXECUTION_TIMEOUT_MINUTES = 120

YEAR_MONTH_PATTERN = re.compile(r"^\d{4}-\d{2}$")
DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")

DEFAULT_POOL_S3 = "s3_pool"
DEFAULT_POOL_REDSHIFT = "redshift_pool"


# =========================================================
# 2) PARAMS / DEFAULT ARGS
# =========================================================
def build_default_args(
    owner: str = DEFAULT_OWNER,
    retries: int = DEFAULT_RETRIES,
    retry_delay_minutes: int = DEFAULT_RETRY_DELAY_MINUTES,
    depends_on_past: bool = False,
    execution_timeout_minutes: int | None = None,
) -> dict[str, Any]:
    """
    Tạo default_args dùng chung cho DAG.

    execution_timeout_minutes:
    - nếu truyền vào -> add execution_timeout
    - nếu None -> để task tự set riêng ở operator
    """
    default_args: dict[str, Any] = {
        "owner": owner,
        "depends_on_past": depends_on_past,
        "retries": retries,
        "retry_delay": timedelta(minutes=retry_delay_minutes),
        "email_on_failure": False,
        "email_on_retry": False,
        "on_failure_callback": airflow_task_failure_callback,
    }

    if execution_timeout_minutes is not None:
        default_args["execution_timeout"] = timedelta(
            minutes=execution_timeout_minutes
        )

    return default_args


def build_manual_run_params() -> dict[str, Param]:
    """
    Params chuẩn cho DAG manual run / backfill.

    Gợi ý dùng ở DAG:
        params=build_manual_run_params()

    Ý nghĩa:
    - load_date:
        + mặc định None -> task sẽ fallback về ds của Airflow
    - year_months:
        + optional list[str] hoặc chuỗi CSV
        + để dành cho backfill / custom run
    - force_full_refresh:
        + cờ logic tùy DAG downstream có dùng hay không
    """
    return {
        "load_date": Param(
            default=None,
            type=["null", "string"],
            description="Optional. YYYY-MM-DD. Nếu bỏ trống sẽ dùng ds của Airflow.",
        ),
        "year_months": Param(
            default=[],
            type=["array", "string"],
            description=(
                "Optional. Danh sách tháng dạng ['2023-01','2023-02'] "
                "hoặc chuỗi '2023-01,2023-02'."
            ),
        ),
        "force_full_refresh": Param(
            default=False,
            type="boolean",
            description="Optional. Cờ logic cho full refresh/backfill.",
        ),
    }


# =========================================================
# 3) CALLBACKS
# =========================================================
def airflow_task_failure_callback(context: dict[str, Any]) -> None:
    """
    Callback log khi task fail.

    Hiện tại chỉ log rõ:
    - dag_id
    - task_id
    - run_id
    - logical_date
    - try_number
    - exception

    Sau này có thể mở rộng:
    - gửi Slack
    - gửi email
    - gọi webhook
    """
    logger = LoggingMixin().log

    dag_id = context.get("dag").dag_id if context.get("dag") else None
    task_instance = context.get("task_instance")
    task_id = task_instance.task_id if task_instance else None
    run_id = context.get("run_id")
    logical_date = context.get("logical_date")
    exception = context.get("exception")
    try_number = getattr(task_instance, "try_number", None)

    logger.error("=" * 80)
    logger.error("AIRFLOW TASK FAILED")
    logger.error("dag_id=%s", dag_id)
    logger.error("task_id=%s", task_id)
    logger.error("run_id=%s", run_id)
    logger.error("logical_date=%s", logical_date)
    logger.error("try_number=%s", try_number)
    logger.error("exception=%s", exception)
    logger.error("=" * 80)


# =========================================================
# 4) RUNTIME PARAM HELPERS
# =========================================================
def _as_bool(value: Any) -> bool:
    """
    Chuẩn hóa nhiều kiểu input thành bool.
    """
    if isinstance(value, bool):
        return value

    if value is None:
        return False

    if isinstance(value, (int, float)):
        return bool(value)

    if isinstance(value, str):
        normalized = value.strip().lower()
        return normalized in {"1", "true", "yes", "y", "on"}

    return bool(value)


def _deduplicate_keep_order(items: list[str]) -> list[str]:
    """
    Remove duplicate nhưng giữ nguyên thứ tự xuất hiện đầu tiên.
    """
    seen: set[str] = set()
    output: list[str] = []

    for item in items:
        if item not in seen:
            seen.add(item)
            output.append(item)

    return output


def normalize_year_months(value: Any) -> list[str]:
    """
    Chuẩn hóa year_months về list[str].

    Hỗ trợ:
    - None
    - []
    - "2023-01,2023-02"
    - ["2023-01", "2023-02"]
    """
    if value is None:
        return []

    raw_items: list[str]

    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return []
        raw_items = [item.strip() for item in stripped.split(",") if item.strip()]
    elif isinstance(value, (list, tuple, set)):
        raw_items = [str(item).strip() for item in value if str(item).strip()]
    else:
        raise ValueError(
            "year_months must be None, CSV string, list/tuple/set of strings"
        )

    normalized = _deduplicate_keep_order(raw_items)

    invalid_items = [item for item in normalized if not YEAR_MONTH_PATTERN.match(item)]
    if invalid_items:
        raise ValueError(
            f"Invalid year_months values: {invalid_items}. Expected YYYY-MM format."
        )

    return normalized


def _normalize_load_date(value: Any, fallback_ds: str) -> str:
    """
    Chuẩn hóa load_date về YYYY-MM-DD.
    """
    if value is None or str(value).strip() == "":
        value = fallback_ds

    load_date = str(value).strip()

    if not DATE_PATTERN.match(load_date):
        raise ValueError(
            f"Invalid load_date: {load_date}. Expected YYYY-MM-DD format."
        )

    return load_date


def get_runtime_config() -> dict[str, Any]:
    """
    Đọc runtime config từ Airflow context.

    Thứ tự ưu tiên:
    1) dag_run.conf
    2) params
    3) fallback mặc định

    Trả về:
    {
        "load_date": "2026-04-19",
        "year_months": ["2023-01", "2023-02"],
        "force_full_refresh": False,
        "dag_id": "...",
        "task_id": "...",
        "run_id": "...",
        "ds": "...",
    }
    """
    context = get_current_context()

    dag = context["dag"]
    task = context["task"]
    dag_run = context.get("dag_run")

    params = dict(context.get("params") or {})
    conf = dict(dag_run.conf or {}) if dag_run else {}

    ds = str(context["ds"])

    load_date = _normalize_load_date(
        conf.get("load_date", params.get("load_date")),
        fallback_ds=ds,
    )

    year_months = normalize_year_months(
        conf.get("year_months", params.get("year_months"))
    )

    force_full_refresh = _as_bool(
        conf.get("force_full_refresh", params.get("force_full_refresh", False))
    )

    return {
        "load_date": load_date,
        "year_months": year_months,
        "force_full_refresh": force_full_refresh,
        "dag_id": dag.dag_id,
        "task_id": task.task_id,
        "run_id": context.get("run_id"),
        "ds": ds,
    }


def log_runtime_config() -> dict[str, Any]:
    """
    Log runtime config để debug nhanh trong task log.
    """
    runtime = get_runtime_config()
    logger = LoggingMixin().log

    logger.info(
        "Runtime config | dag_id=%s | task_id=%s | run_id=%s | ds=%s | "
        "load_date=%s | year_months=%s | force_full_refresh=%s",
        runtime["dag_id"],
        runtime["task_id"],
        runtime["run_id"],
        runtime["ds"],
        runtime["load_date"],
        ",".join(runtime["year_months"]) if runtime["year_months"] else "",
        runtime["force_full_refresh"],
    )

    return runtime


# =========================================================
# 5) ENV HELPERS
# =========================================================
def build_runtime_env(extra_env: dict[str, Any] | None = None) -> dict[str, str]:
    """
    Tạo env vars cho task runtime.

    Chuẩn chung:
    - LOAD_DATE
    - YEAR_MONTHS
    - FORCE_FULL_REFRESH
    - AIRFLOW_DAG_ID
    - AIRFLOW_TASK_ID
    - AIRFLOW_RUN_ID

    Lưu ý:
    - hiện tại run_ingestion.py đã đọc LOAD_DATE từ env
    - YEAR_MONTHS / FORCE_FULL_REFRESH để dành cho task cần dùng sau này
    """
    runtime = get_runtime_config()

    env = dict(os.environ)

    env["LOAD_DATE"] = runtime["load_date"]
    env["YEAR_MONTHS"] = ",".join(runtime["year_months"])
    env["FORCE_FULL_REFRESH"] = "true" if runtime["force_full_refresh"] else "false"

    env["AIRFLOW_DAG_ID"] = runtime["dag_id"]
    env["AIRFLOW_TASK_ID"] = runtime["task_id"]
    env["AIRFLOW_RUN_ID"] = str(runtime["run_id"])

    if extra_env:
        for key, value in extra_env.items():
            if value is None:
                continue
            env[str(key)] = str(value)

    return env


@contextmanager
def patched_environ(new_env: dict[str, str]):
    """
    Context manager để patch tạm thời os.environ trong lúc chạy task.
    """
    old_env = os.environ.copy()

    try:
        os.environ.clear()
        os.environ.update(new_env)
        yield
    finally:
        os.environ.clear()
        os.environ.update(old_env)


# =========================================================
# 6) DYNAMIC IMPORT / PYTHON ENTRYPOINT EXECUTION
# =========================================================
def import_callable(module_name: str, callable_name: str = "main"):
    """
    Import động 1 callable từ module.

    Ví dụ:
    - module_name="src.ingestion.run_ingestion"
    - callable_name="main"
    """
    module = importlib.import_module(module_name)

    if not hasattr(module, callable_name):
        raise AttributeError(
            f"Module '{module_name}' does not have callable '{callable_name}'"
        )

    fn = getattr(module, callable_name)

    if not callable(fn):
        raise TypeError(
            f"Attribute '{callable_name}' in module '{module_name}' is not callable"
        )

    return fn


def run_python_entrypoint(
    module_name: str,
    callable_name: str = "main",
    callable_kwargs: dict[str, Any] | None = None,
    extra_env: dict[str, Any] | None = None,
    log_runtime: bool = True,
) -> Any:
    """
    Wrapper chung để Airflow gọi 1 Python entrypoint trong project.

    Dùng với PythonOperator:
    - import module động ở runtime
    - inject env tạm thời
    - gọi fn()

    Ưu điểm:
    - DAG không cần import nặng toàn project lúc parse
    - code gọn hơn
    - dễ dùng cho nhiều task cùng kiểu
    """
    logger = LoggingMixin().log

    if log_runtime:
        runtime = log_runtime_config()
    else:
        runtime = get_runtime_config()

    env = build_runtime_env(extra_env=extra_env)
    kwargs = deepcopy(callable_kwargs or {})

    logger.info(
        "Running python entrypoint | module=%s | callable=%s | load_date=%s | year_months=%s",
        module_name,
        callable_name,
        runtime["load_date"],
        ",".join(runtime["year_months"]) if runtime["year_months"] else "",
    )

    with patched_environ(env):
        fn = import_callable(module_name=module_name, callable_name=callable_name)

        if kwargs:
            return fn(**kwargs)

        return fn()


# =========================================================
# 7) OPERATOR FACTORIES
# =========================================================
def build_empty_task(
    task_id: str,
    dag=None,
    trigger_rule: str = "all_success",
    doc_md: str | None = None,
) -> EmptyOperator:
    """
    Tạo EmptyOperator chuẩn.
    """
    return EmptyOperator(
        task_id=task_id,
        trigger_rule=trigger_rule,
        dag=dag,
        doc_md=doc_md,
    )


def build_python_entrypoint_task(
    task_id: str,
    module_name: str,
    callable_name: str = "main",
    dag=None,
    callable_kwargs: dict[str, Any] | None = None,
    extra_env: dict[str, Any] | None = None,
    retries: int = DEFAULT_RETRIES,
    retry_delay_minutes: int = DEFAULT_RETRY_DELAY_MINUTES,
    execution_timeout_minutes: int = DEFAULT_EXECUTION_TIMEOUT_MINUTES,
    pool: str | None = None,
    priority_weight: int = 1,
    trigger_rule: str = "all_success",
    doc_md: str | None = None,
) -> PythonOperator:
    """
    Factory tạo PythonOperator tiêu chuẩn cho task gọi entrypoint trong project.

    Ví dụ dùng ở DAG:
        raw_ingestion = build_python_entrypoint_task(
            task_id="raw_ingestion",
            module_name="src.ingestion.run_ingestion",
            callable_name="main",
            pool="s3_pool",
        )
    """
    return PythonOperator(
        task_id=task_id,
        python_callable=run_python_entrypoint,
        op_kwargs={
            "module_name": module_name,
            "callable_name": callable_name,
            "callable_kwargs": callable_kwargs or {},
            "extra_env": extra_env or {},
            "log_runtime": True,
        },
        retries=retries,
        retry_delay=timedelta(minutes=retry_delay_minutes),
        execution_timeout=timedelta(minutes=execution_timeout_minutes),
        pool=pool,
        priority_weight=priority_weight,
        trigger_rule=trigger_rule,
        dag=dag,
        doc_md=doc_md,
    )


# =========================================================
# 8) TASK DOC HELPERS
# =========================================================
def build_task_doc(
    title: str,
    purpose: str,
    inputs: list[str] | None = None,
    outputs: list[str] | None = None,
) -> str:
    """
    Tạo doc_md ngắn gọn cho task trên UI Airflow.
    """
    inputs = inputs or []
    outputs = outputs or []

    lines: list[str] = [f"### {title}", "", purpose]

    if inputs:
        lines.extend(["", "**Inputs**"])
        lines.extend([f"- {item}" for item in inputs])

    if outputs:
        lines.extend(["", "**Outputs**"])
        lines.extend([f"- {item}" for item in outputs])

    return "\n".join(lines)


# =========================================================
# 9) COMMON DOC STRINGS / SUGGESTED CONSTANTS
# =========================================================
DOC_RAW_INGESTION = build_task_doc(
    title="Raw ingestion",
    purpose="Fetch source data, flatten raw files, upload to S3, and load raw tables in Redshift.",
    inputs=[
        "NYC TLC yellow taxi parquet",
        "Open-Meteo historical weather API",
        "Taxi zone lookup CSV",
    ],
    outputs=[
        "local raw files",
        "S3 raw objects",
        "Redshift raw tables",
    ],
)

DOC_RAW_TO_BRONZE = build_task_doc(
    title="Raw to bronze",
    purpose="Transform raw CSV inputs into bronze parquet datasets.",
)

DOC_BRONZE_TO_SILVER = build_task_doc(
    title="Bronze to silver",
    purpose="Clean bronze datasets and produce silver base tables.",
)

DOC_SILVER_TO_GOLD = build_task_doc(
    title="Silver to gold",
    purpose="Build semantic dimensions and fact tables from silver.",
)

DOC_GOLD_TO_SERVING = build_task_doc(
    title="Gold to serving",
    purpose="Build BI-ready marts from gold semantic layer.",
)

DOC_EXPORT_SERVING = build_task_doc(
    title="Export serving to Redshift",
    purpose="Flatten local serving parquet, upload to S3, COPY into Redshift serving tables, and reconcile row counts.",
)


# =========================================================
# 10) __all__
# =========================================================
__all__ = [
    "PROJECT_TIMEZONE",
    "DEFAULT_OWNER",
    "DEFAULT_RETRIES",
    "DEFAULT_RETRY_DELAY_MINUTES",
    "DEFAULT_EXECUTION_TIMEOUT_MINUTES",
    "DEFAULT_POOL_S3",
    "DEFAULT_POOL_REDSHIFT",
    "build_default_args",
    "build_manual_run_params",
    "airflow_task_failure_callback",
    "normalize_year_months",
    "get_runtime_config",
    "log_runtime_config",
    "build_runtime_env",
    "patched_environ",
    "import_callable",
    "run_python_entrypoint",
    "build_empty_task",
    "build_python_entrypoint_task",
    "build_task_doc",
    "DOC_RAW_INGESTION",
    "DOC_RAW_TO_BRONZE",
    "DOC_BRONZE_TO_SILVER",
    "DOC_SILVER_TO_GOLD",
    "DOC_GOLD_TO_SERVING",
    "DOC_EXPORT_SERVING",
]