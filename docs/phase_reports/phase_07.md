Phase: 7 - Airflow orchestration
Completed: Airflow 2.10.5 (LocalExecutor, metadata in a separate "airflow" database inside the existing PostgreSQL), custom image with Java and an isolated project virtualenv; DAG streampulse_batch (9 finite tasks); DAG streampulse_failure_demo; DAG structure tests; real successful run; retry/failure demonstration; UI screenshots.
Files changed: airflow/dags/*.py, docker/airflow/Dockerfile, docker-compose.yml (airflow service), scripts/{airflow_up,airflow_down,airflow,airflow_run_dag,run_tests_airflow,check_dataset_fingerprint,run_quality_checks}, tests/unit/test_airflow_dag.py, pytest.ini, .env.example, docs/evidence/08_airflow/*
Commands executed: scripts\airflow_up.ps1; scripts\run_tests_airflow.ps1; scripts\airflow_run_dag.ps1 -Dag streampulse_batch -Conf '{"limit": 300}'; ... -Dag streampulse_failure_demo; python scripts\capture_screenshots.py airflow ...
Verification (docs/evidence/08_airflow/):
  - DAG tests: 8 passed (imports, task set, dependency order, retries/delay/timeout on every task, infinite stream is not a task, max_active_runs=1, no secrets in DAG files).
  - Real run manual__20261015T181422: SUCCESS, 9/9 tasks. Spark batch 11 processed 590 messages (295 from the first failed run's ingestion + 295 from this run): 0 inserted, 295 retry duplicates; quality checks OK; reconcile "all differences are zero or explained".
  - Failure demo: 3 attempts, KafkaTimeoutError each, task failed, downstream upstream_failed, DAG failed (intended).
Failures met and fixed (history kept):
  1. Run manual__20261015T180545 FAILED: Spark task "Permission denied" writing data/checkpoints/orders (the Spark container created it as root; Airflow ran as uid 50000). Fix: Airflow container runs as root (docker-compose.yml, with HOME for the connector cache). Trade-off documented: acceptable for a demo; production would share a uid or use volumes.
  2. My first orchestration script polled `airflow dags state` (needs an execution date) so it never saw the state: replaced by list-runs JSON. 3. PowerShell 5.1 strips quotes in --conf JSON: escaped, and the script now aborts if the trigger fails.
Tests: DAG tests 8 passed; real DAG runs as above.
Git: see commit "feat(airflow): ..." (hash in docs/task_tracker.md)
Blocked items: none. Limitation: runs as root inside the container; the DAG has no schedule (manual trigger).
Next phase: 8 - dashboard (done, see phase_08.md)
