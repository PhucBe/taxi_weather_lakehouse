from __future__ import annotations

# =========================================================
# taxi_backfill_pipeline.py
# ---------------------------------------------------------
# Mục tiêu:
# - Orchestrate manual backfill pipeline của Project 2
# - Flow giống daily pipeline nhưng chỉ chạy khi trigger tay
# - Hỗ trợ truyền params:
#     + load_date
#     + year_months
#     + force_full_refresh
#
# Lưu ý:
# - Pipeline hiện tại chắc chắn đã đọc LOAD_DATE từ env ở run_ingestion.py
# - YEAR_MONTHS / FORCE_FULL_REFRESH được truyền qua env để đồng bộ runtime
# - Nếu muốn backfill theo year_months thật sự ở ingestion/transform,
#   upstream scripts cần hỗ trợ override config từ env hoặc dag_run.conf
# =========================================================

import importlib.util
from datetime import timedelta
from pathlib import Path
from typing import Any

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowFailException
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
    get_runtime_config,
    log_runtime_config,
    normalize_year_months,
    patched_environ,
)

# =========================================================
# 1) DAG METADATA
# =========================================================
DAG_ID = "taxi_backfill_pipeline"
DAG_DESCRIPTION = "Manual backfill pipeline for NYC taxi + weather project from raw ingestion to Redshift serving."

PROJECT_ROOT = Path("/opt/airflow/project")

# Backfill DAG:
# - không schedule
# - chỉ manual trigger từ UI / API
SCHEDULE = None

START_DATE = pendulum.datetime(2026, 4, 1, tz=PROJECT_TIMEZONE)

TAGS = [
    "taxi",
    "weather",
    "backfill",
    "manual",
    "spark",
    "redshift",
    "serving",
    "portfolio",
]


# =========================================================
# 2) FILE CANDIDATES
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
        "Running file entrypoint | file=%s | callable=%s | load_date=%s | year_months=%s | force_full_refresh=%s",
        file_path,
        callable_name,
        runtime["load_date"],
        ",".join(runtime["year_months"]) if runtime["year_months"] else "",
        runtime["force_full_refresh"],
    )

    with patched_environ(env):
        return fn()


def _run_export_serving_callable(export_callable_name: str) -> None:
    """
    Chạy đúng 1 hàm export mart từ export_serving.py.
    """
    runtime = log_runtime_config()
    airflow_logger = LoggingMixin().log

    env = build_runtime_env()

    export_fn, file_path = _import_callable_from_candidates(
        candidate_paths=EXPORT_SERVING_CANDIDATES,
        callable_name=export_callable_name,
    )

    airflow_logger.info(
        "Running serving export callable | file=%s | callable=%s | load_date=%s | year_months=%s",
        file_path,
        export_callable_name,
        runtime["load_date"],
        ",".join(runtime["year_months"]) if runtime["year_months"] else "",
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


def validate_backfill_request() -> None:
    """
    Validate/log request backfill trước khi pipeline chạy.

    Rule mềm:
    - load_date luôn được normalize từ helper
    - year_months có thể rỗng: khi đó pipeline có thể fallback về config prototype
    - nếu user truyền year_months, validate định dạng YYYY-MM

    Rule cứng:
    - nếu year_months truyền vào nhưng sai format -> fail sớm
    """
    logger = LoggingMixin().log
    runtime = get_runtime_config()

    # validate lại để fail sớm và log rõ
    year_months = normalize_year_months(runtime["year_months"])

    logger.info("=" * 80)
    logger.info("VALIDATING BACKFILL REQUEST")
    logger.info("dag_id=%s", runtime["dag_id"])
    logger.info("task_id=%s", runtime["task_id"])
    logger.info("run_id=%s", runtime["run_id"])
    logger.info("load_date=%s", runtime["load_date"])
    logger.info("year_months=%s", ",".join(year_months) if year_months else "")
    logger.info("force_full_refresh=%s", runtime["force_full_refresh"])

    if not year_months:
        logger.warning(
            "No year_months provided in backfill request. "
            "Upstream scripts may fallback to config['prototype']['year_months']."
        )
    else:
        logger.info("Backfill months validated successfully: %s", ",".join(year_months))

    logger.info("=" * 80)


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
    dagrun_timeout=timedelta(hours=12),
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
    ## Taxi Backfill Pipeline

    Manual orchestration cho Project 2:

    1. Validate backfill request
    2. Raw ingestion
    3. Validation raw
    4. Raw -> Bronze
    5. Validation bronze
    6. Bronze -> Silver
    7. Validation silver
    8. Silver -> Gold
    9. Validation gold
    10. Gold -> Serving
    11. Validation serving
    12. Export 4 serving marts vào Redshift

    ### Expected manual params
    - `load_date`: optional, `YYYY-MM-DD`
    - `year_months`: optional, `["2023-01","2023-02"]` hoặc `"2023-01,2023-02"`
    - `force_full_refresh`: optional, boolean

    Timezone: **Asia/Ho_Chi_Minh**
    Schedule: **manual only**
    """,
) as dag:
    # -----------------------------------------------------
    # START / END
    # -----------------------------------------------------
    start = build_empty_task(
        task_id="start",
        doc_md="### Start\nĐiểm bắt đầu của backfill pipeline.",
        dag=dag,
    )

    end = build_empty_task(
        task_id="end",
        doc_md="### End\nBackfill pipeline hoàn tất sau khi cả 4 mart đã export thành công.",
        dag=dag,
    )

    # -----------------------------------------------------
    # REQUEST VALIDATION
    # -----------------------------------------------------
    validate_backfill_request_task = _build_standard_python_task(
        task_id="validate_backfill_request",
        python_callable=validate_backfill_request,
        retries=0,
        execution_timeout_minutes=15,
        priority_weight=30,
        doc_md=(
            "### Validate backfill request\n"
            "Log runtime config và validate format của `load_date` / `year_months`."
        ),
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
        execution_timeout_minutes=180,
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
        execution_timeout_minutes=180,
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
        execution_timeout_minutes=180,
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
        execution_timeout_minutes=180,
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
        execution_timeout_minutes=180,
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
    start >> validate_backfill_request_task
    validate_backfill_request_task >> raw_ingestion
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