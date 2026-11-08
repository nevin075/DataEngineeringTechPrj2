"""
etl_spark_pipeline -- DATA 430 Project 2

Airflow is only the orchestrator: data generation runs a Python script, every heavy
transformation runs as a Spark job submitted with `spark-submit` to the standalone Spark
cluster (spark://spark-master:7077), and the results are bulk-loaded into PostgreSQL.

    generate_data >> file_validation >> [spark_cleanse_customers, spark_cleanse_products]
                  >> spark_cleanse_orders >> spark_analytics >> load_to_postgres
                  >> data_validation >> completion

spark_cleanse_customers and spark_cleanse_products run in parallel. spark_cleanse_orders waits
for both: orders are checked against the cleaned customers, and order_items against the
cleaned products.

Every run writes to its own folders, keyed by the run's logical timestamp ({{ ts_nodash }}):
    data/raw/<run>/  data/cleaned/<run>/  data/rejected/<run>/  data/analytics/<run>/  data/reports/<run>/
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# ----------------------------------------------------------------------------- settings
SCRIPTS_DIR = os.getenv("PIPELINE_SCRIPTS_DIR", "/opt/airflow/scripts")
DATA_DIR = os.getenv("PIPELINE_DATA_DIR", "/opt/airflow/data")
CONFIG_PATH = os.getenv("PIPELINE_CONFIG_PATH", "/opt/airflow/config/spark_config.json")
SCHEMA_PATH = os.getenv("PIPELINE_SCHEMA_PATH", "/opt/airflow/sql/schema.sql")
CONFIG_DIR = os.path.dirname(CONFIG_PATH)
SPARK_MASTER = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
WORKER_CORES = int(os.getenv("SPARK_WORKER_CORES", "2"))
ALERT_EMAIL = os.getenv("ALERT_EMAIL", "").strip()
RUN_ID = "{{ ts_nodash }}"           # rendered by Airflow, e.g. 20261107T110000

if SCRIPTS_DIR not in sys.path:      # lets the PythonOperator callables import our scripts
    sys.path.insert(0, SCRIPTS_DIR)

log = logging.getLogger(__name__)


def spark_submit(app_name: str, script: str, args: str = "", cores: int | None = None) -> str:
    """Build the spark-submit command for a BashOperator.

    * client deploy mode: the driver runs inside the Airflow container, executors on the worker
    * spark.driver.host = this container's IP so executors can call back to the driver
    * BashOperator fails the task when spark-submit exits non-zero (our scripts exit 1 on error)
    """
    cores = cores or WORKER_CORES
    return f"""
set -euo pipefail
echo "[airflow] spark-submit {script} {args} -> {SPARK_MASTER} (run {RUN_ID})"
spark-submit \\
  --master "{SPARK_MASTER}" \\
  --deploy-mode client \\
  --name {app_name}_{RUN_ID} \\
  --driver-memory ${{SPARK_DRIVER_MEMORY:-1g}} \\
  --executor-memory ${{SPARK_EXECUTOR_MEMORY:-1g}} \\
  --total-executor-cores {cores} \\
  --conf spark.driver.host=$(hostname -i | awk '{{print $1}}') \\
  --conf spark.driver.bindAddress=0.0.0.0 \\
  --driver-java-options "-Dlog4j.configurationFile=file:{CONFIG_DIR}/log4j2.properties" \\
  --py-files {SCRIPTS_DIR}/spark_utils.py \\
  {SCRIPTS_DIR}/{script} {args} --run-id {RUN_ID} --data-dir {DATA_DIR} --config {CONFIG_PATH}
echo "[airflow] spark-submit finished OK"
"""


# ----------------------------------------------------------------------------- task callables
def _file_validation(ts_nodash: str, **_):
    from pipeline_checks import validate_raw_files
    return validate_raw_files(os.path.join(DATA_DIR, "raw", ts_nodash))


def _load_to_postgres(ts_nodash: str, **_):
    from load_postgres import load_run
    loaded = load_run(ts_nodash, DATA_DIR, SCHEMA_PATH)
    return {"rows_loaded": sum(loaded.values()), "tables": loaded}


def _data_validation(ts_nodash: str, **_):
    from pipeline_checks import validate_warehouse
    checks = validate_warehouse(ts_nodash, DATA_DIR)
    return {"checks_passed": len(checks), "checks": checks}


def _completion(ts_nodash: str, dag_run, ti, **_):
    from pipeline_checks import record_audit
    load = ti.xcom_pull(task_ids="load_to_postgres") or {}
    validation = ti.xcom_pull(task_ids="data_validation") or {}
    audit = record_audit(dag_id=dag_run.dag_id, run_id=dag_run.run_id, data_run_id=ts_nodash,
                         status="success", start_time=dag_run.start_date,
                         rows_loaded=load.get("rows_loaded"), data_dir=DATA_DIR,
                         extra={"validation_checks_passed": validation.get("checks_passed"),
                                "rows_by_table": load.get("tables")})
    log.info("PIPELINE SUCCEEDED -- run %s | raw %s -> clean %s (rejected %s) | %s rows loaded | %.0fs",
             ts_nodash, audit["records_raw"], audit["records_clean"], audit["records_rejected"],
             audit["rows_loaded"], audit["duration_seconds"] or 0)
    return audit["audit_id"]


# ----------------------------------------------------------------------------- callbacks
def _on_task_failure(context):
    """Final failure of any task (after retries): log + Slack (if configured).
    Email alerts are handled by Airflow itself via email_on_failure."""
    from pipeline_checks import send_alert
    ti = context["task_instance"]
    send_alert(f"Airflow task FAILED | dag={ti.dag_id} task={ti.task_id} run={context['run_id']} "
               f"try={ti.try_number} | {context.get('exception')} | log: {ti.log_url}")


def _on_task_retry(context):
    ti = context["task_instance"]
    log.warning("Task %s failed, retrying (try %s): %s", ti.task_id, ti.try_number, context.get("exception"))


def _on_dag_failure(context):
    """Record the failed run in etl_audit. Must never raise."""
    try:
        from pipeline_checks import record_audit
        dag_run = context["dag_run"]
        failed = [t.task_id for t in dag_run.get_task_instances(state="failed")]
        record_audit(dag_id=dag_run.dag_id, run_id=dag_run.run_id,
                     data_run_id=dag_run.logical_date.strftime("%Y%m%dT%H%M%S"), status="failed",
                     start_time=dag_run.start_date, failed_task=", ".join(failed) or None,
                     error_message=str(context.get("reason") or "task failure"), data_dir=DATA_DIR)
    except Exception as exc:  # noqa: BLE001
        log.error("Could not write failed run to etl_audit: %s", exc)


default_args = {
    "owner": "data430",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": _on_task_failure,
    "on_retry_callback": _on_task_retry,
    "email": [ALERT_EMAIL] if ALERT_EMAIL else [],
    "email_on_failure": bool(ALERT_EMAIL),
    "email_on_retry": False,
}

# ----------------------------------------------------------------------------- DAG
with DAG(
    dag_id="etl_spark_pipeline",
    description="Faker data -> Spark cleansing + analytics -> PostgreSQL",
    doc_md=__doc__,
    schedule="0 6 * * *",                                   # daily at 06:00
    start_date=pendulum.datetime(2026, 11, 1, tz="America/New_York"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    default_args=default_args,
    on_failure_callback=_on_dag_failure,
    params={
        "records": Param(int(os.getenv("PIPELINE_RECORDS", "100000")), type="integer", minimum=100,
                         description="Base records per entity (100000 for the real run, 1000 for a quick test)"),
        "seed": Param(int(os.getenv("PIPELINE_SEED", "42")), type="integer",
                      description="Random seed for the Faker generator"),
    },
    tags=["data430", "spark", "etl"],
) as dag:

    generate_data = BashOperator(
        task_id="generate_data",
        bash_command=(f"python {SCRIPTS_DIR}/generate_data.py --records {{{{ params.records }}}} "
                      f"--seed {{{{ params.seed }}}} --output-dir {DATA_DIR}/raw/{RUN_ID}"),
    )

    file_validation = PythonOperator(task_id="file_validation", python_callable=_file_validation)

    # The two parallel jobs share the worker's cores so they really run side by side
    half = max(1, WORKER_CORES // 2)
    spark_cleanse_customers = BashOperator(
        task_id="spark_cleanse_customers",
        bash_command=spark_submit("cleanse_customers", "spark_cleanse.py", "--entity customers", cores=half),
    )
    spark_cleanse_products = BashOperator(
        task_id="spark_cleanse_products",
        bash_command=spark_submit("cleanse_products", "spark_cleanse.py", "--entity products", cores=half),
    )
    spark_cleanse_orders = BashOperator(
        task_id="spark_cleanse_orders",
        bash_command=spark_submit("cleanse_orders", "spark_cleanse.py", "--entity orders"),
    )
    spark_analytics = BashOperator(
        task_id="spark_analytics",
        bash_command=spark_submit("analytics", "spark_analytics.py"),
    )

    load_to_postgres = PythonOperator(task_id="load_to_postgres", python_callable=_load_to_postgres,
                                      retries=2)
    data_validation = PythonOperator(task_id="data_validation", python_callable=_data_validation)
    completion = PythonOperator(task_id="completion", python_callable=_completion)

    generate_data >> file_validation >> [spark_cleanse_customers, spark_cleanse_products]
    [spark_cleanse_customers, spark_cleanse_products] >> spark_cleanse_orders
    spark_cleanse_orders >> spark_analytics >> load_to_postgres >> data_validation >> completion
