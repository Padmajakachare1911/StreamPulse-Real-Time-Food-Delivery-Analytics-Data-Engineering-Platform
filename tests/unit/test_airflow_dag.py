"""DAG import and structure tests (M7). Marker `airflow`: they need the airflow library, so they run
inside the Airflow container:  .\\scripts\\run_tests_airflow.ps1
An import success is NOT the proof that the pipeline works; the real DAG run evidence is in docs/evidence/08_airflow/.
"""
import os
from pathlib import Path

import pytest

pytestmark = pytest.mark.airflow
# The repo has its own airflow/ folder (the DAGs), so check for the REAL library module, not the folder name.
airflow_models = pytest.importorskip("airflow.models", reason="the airflow library is only installed in the Airflow container")

from airflow.models import DagBag  # noqa: E402

DAGS = Path(os.getenv("DAGS_FOLDER", Path(__file__).resolve().parents[2] / "airflow" / "dags"))


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAGS), include_examples=False)


def test_dags_import_without_errors(dagbag):
    assert dagbag.import_errors == {}, dagbag.import_errors


def test_expected_dags_exist(dagbag):
    assert {"streampulse_batch", "streampulse_failure_demo"} <= set(dagbag.dag_ids)


def test_batch_dag_has_the_finite_tasks(dagbag):
    dag = dagbag.get_dag("streampulse_batch")
    assert set(dag.task_ids) == {
        "check_dataset_present", "validate_dataset_profile", "api_health_check", "ingest_batch_to_kafka",
        "run_spark_available_now", "run_quality_checks_sql", "load_runs_dead_letters_api", "reconcile_counts",
        "refresh_analytics"}


def test_batch_dag_dependencies_follow_the_pipeline_order(dagbag):
    dag = dagbag.get_dag("streampulse_batch")
    chain = ["check_dataset_present", "validate_dataset_profile", "ingest_batch_to_kafka", "run_spark_available_now",
             "run_quality_checks_sql", "load_runs_dead_letters_api", "reconcile_counts", "refresh_analytics"]
    for upstream, downstream in zip(chain, chain[1:]):
        assert downstream in dag.get_task(upstream).downstream_task_ids, f"{upstream} -> {downstream}"
    assert "api_health_check" in dag.get_task("check_dataset_present").downstream_task_ids
    assert dag.get_task("api_health_check").downstream_task_ids == set()


def test_every_task_has_retries_delay_and_timeout(dagbag):
    for dag_id in ("streampulse_batch", "streampulse_failure_demo"):
        for task in dagbag.get_dag(dag_id).tasks:
            assert task.retries >= 1, f"{task.task_id} has no retries"
            assert task.retry_delay.total_seconds() > 0
            assert task.execution_timeout is not None, f"{task.task_id} has no timeout"


def test_the_infinite_stream_is_not_a_task(dagbag):
    spark = dagbag.get_dag("streampulse_batch").get_task("run_spark_available_now")
    assert "--trigger availableNow" in spark.bash_command          # finite run
    for task in dagbag.get_dag("streampulse_batch").tasks:
        assert "5 seconds" not in task.bash_command, f"{task.task_id} looks like the continuous stream"


def test_batch_dag_is_safe_to_run_repeatedly(dagbag):
    dag = dagbag.get_dag("streampulse_batch")
    assert dag.max_active_runs == 1            # one run at a time: they share one Spark checkpoint
    assert dag.catchup is False
    assert "limit" in dag.params               # the replay size is bounded by a parameter


def test_no_secret_values_in_dag_files():
    for path in DAGS.glob("*.py"):
        text = path.read_text(encoding="utf-8").lower()
        assert "password=" not in text and "secret=" not in text, f"possible secret in {path.name}"
