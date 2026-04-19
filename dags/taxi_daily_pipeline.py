from __future__ import annotations

# =========================================================
# taxi_daily_pipeline.py
# ---------------------------------------------------------
# Mục tiêu:
# - Orchestrate toàn bộ daily pipeline của Project 2
# - Chuỗi chạy:
#     ingest raw
#     -> validate raw
#     -> raw to bronze
#     -> validate bronze
#     -> bronze to silver
#     -> validate silver
#     -> silver to gold
#     -> validate gold
#     -> gold to serving
#     -> validate serving
#     -> export 4 marts serving vào Redshift
#
# Thiết kế:
# - Dùng helper chung từ dags/helpers/airflow_common.py
# - Dùng PythonOperator
# - Import runtime theo file path candidates để đỡ phụ thuộc
#   quá cứng vào đúng module path vật lý của project
# - Với export_serving.py:
#     + các task export mart tách riêng
#     + mỗi task gọi đúng 1 hàm load_mart_*_to_redshift_serving
# =========================================================

import importlib.util
from datetime import timedelta
from pathlib import Path
from typing import Any

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin

from helpers.airflow_common import (
    DEFAULT_EXECUTION_TIMEOUT_MINUTES,
    DEFAULT_POOL_REDSHIFT,
    DEFAULT_POOL_S3,
    PROJECT_TIMEZONE,
    DOC_BRONZE_TO_SILVER,
    DOC_EXPORT_SERVING,
    DOC_GOLD_TO_SERVING,
    DOC_RAW_INGESTION,
    DOC_RAW_TO_BRONZE,
    DOC_SILVER_TO_GOLD,
    build_default_args,
    build_empty_task,
    build_manual_run_params,
    build_runtime_env,
    log_runtime_config,
    patched_environ,
)

# =========================================================
# 1) DAG METADATA
# =========================================================
DAG_ID = "taxi_daily_pipeline"
DAG_DESCRIPTION = "Daily NYC taxi + weather pipeline from raw ingestion to serving export on Redshift."

PROJECT_ROOT = Path("/opt/airflow/project")

# Daily schedule:
# - 07:00 Asia/Ho_Chi_Minh
# - catchup=False để tránh backfill ngoài ý muốn
SCHEDULE = "0 7 * * *"

START_DATE = pendulum.datetime(2026, 4, 1, tz=PROJECT_TIMEZONE)

TAGS = [
    "taxi",
    "weather",
    "daily",
    "spark",
    "redshift",
    "serving",
    "portfolio",
]


# =========================================================
# 2) FILE CANDIDATES
# ---------------------------------------------------------
# Vì layout project thực tế có thể khác nhau chút giữa local và
# container Airflow, nên mỗi task có nhiều candidate path.
#
# Logic:
# - ưu tiên path "chuẩn" trong project
# - nếu không có thì fallback sang file cùng tên ở root
# =========================================================
RUN_INGESTION_CANDIDATES = [
    "src/ingestion/run_ingestion.py",
]

VALIDATION_RAW_CANDIDATES = [
    "src/validation/validation_raw.py",
]

RAW_TO_BRONZE_CANDIDATES = [
    "spark/raw_to_bronze.py",
]

VALIDATION_BRONZE_CANDIDATES = [
    "src/validation/validation_bronze.py",
]

BRONZE_TO_SILVER_CANDIDATES = [
    "spark/bronze_to_silver.py",
]

VALIDATION_SILVER_CANDIDATES = [
    "src/validation/validation_silver.py",
]

SILVER_TO_GOLD_CANDIDATES = [
    "spark/silver_to_gold.py",
]

VALIDATION_GOLD_CANDIDATES = [
    "src/validation/validation_gold.py",
]

GOLD_TO_SERVING_CANDIDATES = [
    "spark/gold_to_serving.py",
]

VALIDATION_SERVING_CANDIDATES = [
    "src/validation/validation_serving.py",
]

EXPORT_SERVING_CANDIDATES = [
    "spark/export_serving.py",
]


# =========================================================
# 3) HELPER FUNCTIONS
# =========================================================
def _resolve_existing_file(candidate_paths: list[str]) -> Path:
    """
    Resolve file path đầu tiên tồn tại trong candidate list.
    """
    for relative_path in candidate_paths:
        path = PROJECT_ROOT / relative_path
        if path.exists() and path.is_file():
            return path

    raise FileNotFoundError(
        "Cannot resolve any candidate file path. Tried: "
        + ", ".join(str(PROJECT_ROOT / p) for p in candidate_paths)
    )


def _import_module_from_file(file_path: Path):
    """
    Import module động từ file path.
    """
    module_name = f"airflow_runtime_{file_path.stem}_{abs(hash(str(file_path)))}"
    spec = importlib.util.spec_from_file_location(module_name, str(file_path))

    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot create import spec from file: {file_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _import_callable_from_candidates(
    candidate_paths: list[str],
    callable_name: str,
):
    """
    Import 1 callable từ file path candidates.
    """
    file_path = _resolve_existing_file(candidate_paths)
    module = _import_module_from_file(file_path)

    if not hasattr(module, callable_name):
        raise AttributeError(
            f"Callable '{callable_name}' not found in file: {file_path}"
        )

    fn = getattr(module, callable_name)

    if not callable(fn):
        raise TypeError(
            f"Attribute '{callable_name}' in file '{file_path}' is not callable"
        )

    return fn, file_path


def _load_project_config_and_logger(logger_name: str):
    """
    Import load_app_config + get_logger theo fallback giống codebase hiện tại.
    """
    try:
        from src.common.config import load_app_config
        from src.common.logger import get_logger
    except ImportError:  # pragma: no cover
        from src.utils.config import load_app_config  # type: ignore
        from src.utils.logger import get_logger  # type: ignore

    config = load_app_config()
    logger = get_logger(
        name=logger_name,
        log_dir=config["paths"]["log_dir"],
    )
    return config, logger


def _run_file_entrypoint(
    candidate_paths: list[str],
    callable_name: str = "main",
) -> Any:
    """
    Chạy 1 entrypoint kiểu main() từ file Python trong project.
    """
    runtime = log_runtime_config()
    logger = LoggingMixin().log

    env = build_runtime_env()

    fn, file_path = _import_callable_from_candidates(
        candidate_paths=candidate_paths,
        callable_name=callable_name,
    )

    logger.info(
        "Running file entrypoint | file=%s | callable=%s | load_date=%s | year_months=%s",
        file_path,
        callable_name,
        runtime["load_date"],
        ",".join(runtime["year_months"]) if runtime["year_months"] else "",
    )

    with patched_environ(env):
        return fn()


def _run_export_serving_callable(export_callable_name: str) -> None:
    """
    Chạy đúng 1 hàm export mart từ export_serving.py.

    Lý do cần wrapper riêng:
    - các hàm load_mart_*_to_redshift_serving(config, logger)
      không phải signature main()
    - Airflow task nên tách theo từng mart để retry / debug riêng
    """
    runtime = log_runtime_config()
    airflow_logger = LoggingMixin().log

    env = build_runtime_env()

    export_fn, file_path = _import_callable_from_candidates(
        candidate_paths=EXPORT_SERVING_CANDIDATES,
        callable_name=export_callable_name,
    )

    airflow_logger.info(
        "Running serving export callable | file=%s | callable=%s | load_date=%s",
        file_path,
        export_callable_name,
        runtime["load_date"],
    )

    with patched_environ(env):
        config, logger = _load_project_config_and_logger(
            logger_name=f"airflow_{export_callable_name}"
        )
        export_fn(config=config, logger=logger)


def _build_standard_python_task(
    task_id: str,
    python_callable,
    op_kwargs: dict[str, Any] | None = None,
    retries: int = 1,
    retry_delay_minutes: int = 5,
    execution_timeout_minutes: int = DEFAULT_EXECUTION_TIMEOUT_MINUTES,
    pool: str | None = None,
    priority_weight: int = 1,
    doc_md: str | None = None,
    dag: DAG | None = None,
) -> PythonOperator:
    """
    Factory nhỏ để tạo PythonOperator đồng bộ style.
    """
    return PythonOperator(
        task_id=task_id,
        python_callable=python_callable,
        op_kwargs=op_kwargs or {},
        retries=retries,
        retry_delay=timedelta(minutes=retry_delay_minutes),
        execution_timeout=timedelta(minutes=execution_timeout_minutes),
        pool=pool,
        priority_weight=priority_weight,
        doc_md=doc_md,
        dag=dag,
    )


# =========================================================
# 4) DAG DEFINITION
# =========================================================
with DAG(
    dag_id=DAG_ID,
    description=DAG_DESCRIPTION,
    start_date=START_DATE,
    schedule=SCHEDULE,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=8),
    default_args=build_default_args(
        owner="phuchoang",
        retries=1,
        retry_delay_minutes=5,
        depends_on_past=False,
    ),
    params=build_manual_run_params(),
    tags=TAGS,
    render_template_as_native_obj=True,
    doc_md="""
    ## Taxi Daily Pipeline

    Daily orchestration cho Project 2:

    1. Raw ingestion
    2. Validation raw
    3. Raw -> Bronze
    4. Validation bronze
    5. Bronze -> Silver
    6. Validation silver
    7. Silver -> Gold
    8. Validation gold
    9. Gold -> Serving
    10. Validation serving
    11. Export 4 serving marts vào Redshift

    Timezone: **Asia/Ho_Chi_Minh**
    Schedule: **07:00 mỗi ngày**
    """,
) as dag:
    # -----------------------------------------------------
    # START / END
    # -----------------------------------------------------
    start = build_empty_task(
        task_id="start",
        doc_md="### Start\nĐiểm bắt đầu của daily pipeline.",
        dag=dag,
    )

    end = build_empty_task(
        task_id="end",
        doc_md="### End\nPipeline hoàn tất sau khi cả 4 mart đã export thành công.",
        dag=dag,
    )

    # -----------------------------------------------------
    # RAW LAYER
    # -----------------------------------------------------
    raw_ingestion = _build_standard_python_task(
        task_id="raw_ingestion",
        python_callable=_run_file_entrypoint,
        op_kwargs={
            "candidate_paths": RUN_INGESTION_CANDIDATES,
            "callable_name": "main",
        },
        retries=2,
        retry_delay_minutes=5,
        execution_timeout_minutes=120,
        pool=DEFAULT_POOL_S3,
        priority_weight=20,
        doc_md=DOC_RAW_INGESTION,
        dag=dag,
    )

    validation_raw = _build_standard_python_task(
        task_id="validation_raw",
        python_callable=_run_file_entrypoint,
        op_kwargs={
            "candidate_paths": VALIDATION_RAW_CANDIDATES,
            "callable_name": "main",
        },
        retries=0,
        execution_timeout_minutes=60,
        pool=DEFAULT_POOL_REDSHIFT,
        priority_weight=19,
        doc_md=(
            "### Raw validation\n"
            "Kiểm tra raw local files và raw tables trên Redshift."
        ),
        dag=dag,
    )

    # -----------------------------------------------------
    # BRONZE LAYER
    # -----------------------------------------------------
    raw_to_bronze = _build_standard_python_task(
        task_id="raw_to_bronze",
        python_callable=_run_file_entrypoint,
        op_kwargs={
            "candidate_paths": RAW_TO_BRONZE_CANDIDATES,
            "callable_name": "main",
        },
        retries=1,
        execution_timeout_minutes=120,
        priority_weight=18,
        doc_md=DOC_RAW_TO_BRONZE,
        dag=dag,
    )

    validation_bronze = _build_standard_python_task(
        task_id="validation_bronze",
        python_callable=_run_file_entrypoint,
        op_kwargs={
            "candidate_paths": VALIDATION_BRONZE_CANDIDATES,
            "callable_name": "main",
        },
        retries=0,
        execution_timeout_minutes=60,
        priority_weight=17,
        doc_md="### Bronze validation\nKiểm tra schema, row count relation, partition, duplicate cơ bản của bronze.",
        dag=dag,
    )

    # -----------------------------------------------------
    # SILVER LAYER
    # -----------------------------------------------------
    bronze_to_silver = _build_standard_python_task(
        task_id="bronze_to_silver",
        python_callable=_run_file_entrypoint,
        op_kwargs={
            "candidate_paths": BRONZE_TO_SILVER_CANDIDATES,
            "callable_name": "main",
        },
        retries=1,
        execution_timeout_minutes=120,
        priority_weight=16,
        doc_md=DOC_BRONZE_TO_SILVER,
        dag=dag,
    )

    validation_silver = _build_standard_python_task(
        task_id="validation_silver",
        python_callable=_run_file_entrypoint,
        op_kwargs={
            "candidate_paths": VALIDATION_SILVER_CANDIDATES,
            "callable_name": "main",
        },
        retries=0,
        execution_timeout_minutes=60,
        priority_weight=15,
        doc_md="### Silver validation\nKiểm tra grain, time logic, derived columns, partitions của silver.",
        dag=dag,
    )

    # -----------------------------------------------------
    # GOLD LAYER
    # -----------------------------------------------------
    silver_to_gold = _build_standard_python_task(
        task_id="silver_to_gold",
        python_callable=_run_file_entrypoint,
        op_kwargs={
            "candidate_paths": SILVER_TO_GOLD_CANDIDATES,
            "callable_name": "main",
        },
        retries=1,
        execution_timeout_minutes=120,
        priority_weight=14,
        doc_md=DOC_SILVER_TO_GOLD,
        dag=dag,
    )

    validation_gold = _build_standard_python_task(
        task_id="validation_gold",
        python_callable=_run_file_entrypoint,
        op_kwargs={
            "candidate_paths": VALIDATION_GOLD_CANDIDATES,
            "callable_name": "main",
        },
        retries=0,
        execution_timeout_minutes=60,
        priority_weight=13,
        doc_md="### Gold validation\nKiểm tra dimension/fact grains, FK coverage, reconciliation với silver.",
        dag=dag,
    )

    # -----------------------------------------------------
    # SERVING LAYER
    # -----------------------------------------------------
    gold_to_serving = _build_standard_python_task(
        task_id="gold_to_serving",
        python_callable=_run_file_entrypoint,
        op_kwargs={
            "candidate_paths": GOLD_TO_SERVING_CANDIDATES,
            "callable_name": "main",
        },
        retries=1,
        execution_timeout_minutes=120,
        priority_weight=12,
        doc_md=DOC_GOLD_TO_SERVING,
        dag=dag,
    )

    validation_serving = _build_standard_python_task(
        task_id="validation_serving",
        python_callable=_run_file_entrypoint,
        op_kwargs={
            "candidate_paths": VALIDATION_SERVING_CANDIDATES,
            "callable_name": "main",
        },
        retries=0,
        execution_timeout_minutes=60,
        priority_weight=11,
        doc_md="### Serving validation\nKiểm tra marts BI-ready, consistency với dimensions và reconciliation với fact.",
        dag=dag,
    )

    # -----------------------------------------------------
    # EXPORT SERVING -> REDSHIFT
    # -----------------------------------------------------
    export_mart_daily_demand = _build_standard_python_task(
        task_id="export_mart_daily_demand",
        python_callable=_run_export_serving_callable,
        op_kwargs={
            "export_callable_name": "load_mart_daily_demand_to_redshift_serving",
        },
        retries=1,
        retry_delay_minutes=5,
        execution_timeout_minutes=90,
        pool=DEFAULT_POOL_REDSHIFT,
        priority_weight=10,
        doc_md=DOC_EXPORT_SERVING,
        dag=dag,
    )

    export_mart_daily_payment_mix = _build_standard_python_task(
        task_id="export_mart_daily_payment_mix",
        python_callable=_run_export_serving_callable,
        op_kwargs={
            "export_callable_name": "load_mart_daily_payment_mix_to_redshift_serving",
        },
        retries=1,
        retry_delay_minutes=5,
        execution_timeout_minutes=90,
        pool=DEFAULT_POOL_REDSHIFT,
        priority_weight=9,
        doc_md=DOC_EXPORT_SERVING,
        dag=dag,
    )

    export_mart_weather_impact = _build_standard_python_task(
        task_id="export_mart_weather_impact",
        python_callable=_run_export_serving_callable,
        op_kwargs={
            "export_callable_name": "load_mart_weather_impact_to_redshift_serving",
        },
        retries=1,
        retry_delay_minutes=5,
        execution_timeout_minutes=90,
        pool=DEFAULT_POOL_REDSHIFT,
        priority_weight=8,
        doc_md=DOC_EXPORT_SERVING,
        dag=dag,
    )

    export_mart_zone_demand = _build_standard_python_task(
        task_id="export_mart_zone_demand",
        python_callable=_run_export_serving_callable,
        op_kwargs={
            "export_callable_name": "load_mart_zone_demand_to_redshift_serving",
        },
        retries=1,
        retry_delay_minutes=5,
        execution_timeout_minutes=90,
        pool=DEFAULT_POOL_REDSHIFT,
        priority_weight=7,
        doc_md=DOC_EXPORT_SERVING,
        dag=dag,
    )

    # -----------------------------------------------------
    # DEPENDENCIES
    # -----------------------------------------------------
    start >> raw_ingestion
    raw_ingestion >> validation_raw
    validation_raw >> raw_to_bronze
    raw_to_bronze >> validation_bronze
    validation_bronze >> bronze_to_silver
    bronze_to_silver >> validation_silver
    validation_silver >> silver_to_gold
    silver_to_gold >> validation_gold
    validation_gold >> gold_to_serving
    gold_to_serving >> validation_serving

    validation_serving >> [
        export_mart_daily_demand,
        export_mart_daily_payment_mix,
        export_mart_weather_impact,
        export_mart_zone_demand,
    ]

    export_mart_daily_demand >> end
    export_mart_daily_payment_mix >> end
    export_mart_weather_impact >> end
    export_mart_zone_demand >> end