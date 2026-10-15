"""End-to-end tests: they run the REAL pipeline (real producer -> real Kafka -> Spark in Docker -> Parquet ->
PostgreSQL) in an ISOLATED topic/database/checkpoint, via scripts/*_experiment(s).py, and the real reconciliation.
Each scenario takes minutes. They are skipped (with a reason) when Docker, Kafka or PostgreSQL are not available.
The scripts print every check as PASS/FAIL and exit non-zero on any FAIL, so exit code 0 means all checks passed.
"""
import shutil
import subprocess
import sys
import uuid
from pathlib import Path

import pytest

from src.warehouse import db

pytestmark = pytest.mark.e2e
ROOT = Path(__file__).resolve().parents[2]


def _infrastructure_ready() -> str | None:
    if shutil.which("docker") is None:
        return "docker is not installed"
    running = subprocess.run(["docker", "ps", "--format", "{{.Names}}"], capture_output=True, text=True).stdout
    if "streampulse-kafka" not in running:
        return "the streampulse-kafka container is not running"
    ok, message = db.ping()
    return None if ok else f"PostgreSQL not reachable: {message}"


@pytest.fixture(scope="module", autouse=True)
def require_infrastructure():
    problem = _infrastructure_ready()
    if problem:
        pytest.skip(problem)


def run_script(args: list[str], timeout: int) -> subprocess.CompletedProcess:
    return subprocess.run([sys.executable, "-u", *args], cwd=ROOT, capture_output=True, text=True, timeout=timeout,
                          env={**__import__("os").environ, "PYTHONUTF8": "1"})


def tag() -> str:
    return "e" + uuid.uuid4().hex[:6]


def test_data_quality_and_idempotent_reingestion_end_to_end():
    result = run_script(["scripts/quality_experiment.py", "--tag", tag()], timeout=900)
    assert result.returncode == 0, result.stdout[-2500:]
    assert "EXPERIMENT PASSED" in result.stdout and "[FAIL]" not in result.stdout


def test_malformed_messages_reach_the_stream_dead_letter_end_to_end():
    result = run_script(["scripts/spark_experiments.py", "malformed", "--tag", tag()], timeout=900)
    assert result.returncode == 0, result.stdout[-2500:]
    assert "EXPERIMENT PASSED" in result.stdout and "[FAIL]" not in result.stdout


def test_stop_mid_backlog_and_restart_recovers_without_duplicates_end_to_end():
    result = run_script(["scripts/spark_experiments.py", "recovery", "--tag", tag()], timeout=1500)
    assert result.returncode == 0, result.stdout[-3000:]
    assert "EXPERIMENT PASSED" in result.stdout and "[FAIL]" not in result.stdout


def test_production_reconciliation_has_no_unexplained_differences():
    result = run_script(["scripts/reconcile.py"], timeout=600)
    assert result.returncode == 0, result.stdout[-3000:]
    assert "RESULT: all differences are zero or explained" in result.stdout
