Phase: 9 - Automated testing
Completed: coverage gaps closed (Parquet persistence, stored API provenance, end-to-end suite, safe-stop classifier, dashboard source guard); all suites run separately with saved logs (command, exit code, counts, skip reasons).
Files changed: tests/unit/{test_parquet,test_stop_classification,test_dashboard_source,test_reconcile}.py, tests/integration/test_api_stored_results.py, tests/e2e/test_pipeline_e2e.py, src/streaming/order_stream.py (safe stop), scripts/spark_experiments.py (stop-signature classifier), src/dashboard/app.py, docs/evidence/10_automated_tests/*, docs/test_cases.md, docs/known_limitations.md
Commands executed (logs under docs/evidence/10_automated_tests/):
  pytest -m unit -v -rs            -> 65 passed, 1 skipped (Airflow module; reason printed), exit 0
  pytest -m integration -v -rs     -> 18 passed, 0 skipped (live API, PostgreSQL, real Kafka), exit 0
  scripts\run_tests_docker.ps1     -> 5 passed (Spark)
  scripts\run_tests_airflow.ps1    -> 8 passed (Airflow container)
  pytest -m e2e -v -rs             -> FIRST run: 3 passed, 1 FAILED; FINAL run: 4 passed
Executed vs skipped vs mocked: the API client unit tests use a mocked HTTP session (logic only, never counted as API proof); the live API is exercised by tests/integration/test_api_real.py (passed, not skipped) and the stored provenance by test_api_stored_results.py; the only skip is the Airflow module on the host, whose 8 tests passed inside the Airflow container.
Failure found and fixed (kept as history): the e2e recovery test failed once with InterruptedException in a Spark count. Cause: the graceful stop had a race (query.stop() while a batch was starting). Data stayed correct (17 of 18 checks passed). Spark has no public stop-between-batches call, so the stop was redesigned: "in batch" is set before "stopping" is read; a batch that starts after a stop request raises StopRequested before doing any work; a running batch finishes; query.stop() only when idle. The experiment checker accepts only that signature (5 unit tests prove it still flags a real InterruptedException). Verified by 3 consecutive 18/18 runs and the final e2e pass.
Defect found while viewing screenshots: Streamlit magic printed internal documentation on 3 dashboard pages; fixed + AST guard test.
Git: see commit "test: ..." (hash in docs/task_tracker.md)
Blocked items: none. Limitation: the final full re-run of every suite after the last code change is part of Phase 11.
Next phase: 10 - manual demonstrations (dry-run tested), then 11 final audit
