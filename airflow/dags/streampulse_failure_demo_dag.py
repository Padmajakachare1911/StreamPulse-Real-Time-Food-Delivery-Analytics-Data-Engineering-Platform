"""Retry / failure demonstration (Demo 4): a REAL task with an unavailable dependency.

The task runs the real replay producer against a Kafka address that does not exist. It fails, Airflow
retries it (retries=2, retry_delay=15 s) and finally marks the task failed; the downstream task is then
skipped (state upstream_failed). This is a simulated outage of a real dependency, not a mocked service.
Trigger it manually; it never touches production data (the producer cannot reach any broker).
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT = "/opt/streampulse"
PYTHON = "/home/airflow/project-venv/bin/python"

with DAG(
    dag_id="streampulse_failure_demo",
    description="Shows Airflow retries and failure handling with an unreachable Kafka broker",
    default_args={"owner": "streampulse", "retries": 2, "retry_delay": timedelta(seconds=15),
                  "execution_timeout": timedelta(minutes=5)},
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,
    tags=["streampulse", "demo"],
) as dag:
    ingest_from_unreachable_kafka = BashOperator(
        task_id="ingest_from_unreachable_kafka",
        bash_command=(f"cd {PROJECT} && echo \"attempt {{{{ ti.try_number }}}} of 3\" && "
                      f"{PYTHON} -m src.producer.dataset_producer --limit 5 --bootstrap kafka-does-not-exist:9092 "
                      f"--run-id airflow-failure-demo-{{{{ ts_nodash }}}}"),
    )
    downstream_never_runs = BashOperator(task_id="downstream_never_runs", bash_command="echo 'this should not run'")
    ingest_from_unreachable_kafka >> downstream_never_runs
