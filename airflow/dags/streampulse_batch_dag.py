"""StreamPulse batch DAG: orchestrates the FINITE steps of the pipeline.

Why only finite tasks: an Airflow task must end. The continuous Spark stream (trigger every 5 s) never
ends, so it is NOT a task here. The DAG runs Spark with trigger availableNow instead: process whatever
is in Kafka now, then stop. The historical-dataset replay is bounded by `limit` too.

Every step is safe to run again (idempotent): replaying the same orders gives the same record_uid, so
PostgreSQL ignores them (see docs/data_quality_rules.md).

Task graph:
  check_dataset_present -> validate_dataset_profile -> ingest_batch_to_kafka -> run_spark_available_now
      -> run_quality_checks_sql -> load_runs_dead_letters_api -> reconcile_counts -> refresh_analytics
  api_health_check (independent, one real request to the exchange-rate API)
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

PROJECT = "/opt/streampulse"
PYTHON = "/home/airflow/project-venv/bin/python"
RUN_ID = "airflow-{{ ts_nodash }}"          # unique per DAG run, e.g. airflow-20261015T180000

default_args = {
    "owner": "streampulse",
    "retries": 2,                            # transient problems (Kafka/DB just starting) get another chance
    "retry_delay": timedelta(seconds=30),
    "execution_timeout": timedelta(minutes=20),
}


def bash(task_id: str, command: str, **kwargs) -> BashOperator:
    """All commands run from the project folder with the project virtualenv."""
    return BashOperator(task_id=task_id, bash_command=f"cd {PROJECT} && {command}", **kwargs)


with DAG(
    dag_id="streampulse_batch",
    description="Finite StreamPulse steps: dataset checks, bounded Kafka replay, Spark availableNow, quality, reconcile",
    default_args=default_args,
    start_date=datetime(2026, 10, 1),
    schedule=None,                           # started manually (or give it a schedule later)
    catchup=False,
    max_active_runs=1,                       # two runs at once would share one Spark checkpoint
    params={"limit": Param(500, type="integer", minimum=1, maximum=100000,
                           description="how many historical orders to replay into Kafka in this run")},
    tags=["streampulse"],
) as dag:

    check_dataset_present = bash("check_dataset_present", f"{PYTHON} scripts/check_dataset_fingerprint.py --files-only")

    validate_dataset_profile = bash("validate_dataset_profile", f"{PYTHON} scripts/check_dataset_fingerprint.py")

    api_health_check = bash(
        "api_health_check",
        # one real request to the exchange-rate API (no cache) proves the integration is alive
        f"{PYTHON} scripts/api_demo_call.py --sample --no-cache",
    )

    ingest_batch_to_kafka = bash(
        "ingest_batch_to_kafka",
        f"{PYTHON} -m src.producer.dataset_producer --limit {{{{ params.limit }}}} --run-id {RUN_ID}",
    )

    run_spark_available_now = bash(
        "run_spark_available_now",
        f"{PYTHON} -m src.streaming.order_stream --stream-name orders --trigger availableNow --postgres",
        execution_timeout=timedelta(minutes=30),
    )

    run_quality_checks_sql = bash("run_quality_checks_sql", f"{PYTHON} scripts/run_quality_checks.py")

    load_runs_dead_letters_api = bash(
        "load_runs_dead_letters_api",
        f"{PYTHON} -m src.warehouse.load_reference --only runs dlq api",
    )

    reconcile_counts = bash("reconcile_counts", f"{PYTHON} scripts/reconcile.py")   # exits 1 on an unexplained mismatch

    refresh_analytics = bash("refresh_analytics", f"{PYTHON} scripts/init_db.py")   # CREATE OR REPLACE views, never drops data

    (check_dataset_present >> validate_dataset_profile >> ingest_batch_to_kafka >> run_spark_available_now
     >> run_quality_checks_sql >> load_runs_dead_letters_api >> reconcile_counts >> refresh_analytics)
    check_dataset_present >> api_health_check
