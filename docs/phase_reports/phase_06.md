Phase: 6 - PostgreSQL warehouse and reconciliation
Completed: repeatable schema + 11 analytics views checked on real data; idempotent upsert proven (tests and production re-ingestion); reference tables, run summaries, dead letters and API cache loaded; reconciliation rewritten with explicit run-level vs source-level accounting; 14-vs-15 rejected discrepancy investigated and explained; non-zero exit on unexplained mismatch demonstrated.
Files changed: scripts/reconcile.py (rewritten), scripts/explain_rejected_count.py, sql/analytics_samples.sql, src/warehouse/load_reference.py (topic/run filters, numpy cast fix), tests/unit/test_reconcile.py, docs/data_quality_report.md, docs/evidence/06_parquet/*, docs/evidence/07_postgresql/*
Commands executed:
  python scripts\reconcile.py   (production, 3 runs)  /  --topic recovery_test_r2 ...  /  --runs rec-A-r2 ... (deliberate mismatch)
  python scripts\explain_rejected_count.py
  python -m src.warehouse.load_reference   (twice)
  python -m src.producer.dataset_producer --limit 200 --run-id reingest-200 ; scripts\run_stream.ps1 -Trigger availableNow
  psql: quality_checks.sql, analytics_samples.sql ; pg_dump --schema-only ; parquet_readback.py ; parquet_readback_spark.py
Verification (docs/evidence/06_parquet, 07_postgresql):
  - Warehouse: 99,427 orders = distinct record_uid = distinct order_id = distinct business_hash; quality checks: 0 problem rows.
  - Production re-ingestion: 199 messages re-sent, 0 inserted, 199 retry_duplicates, count unchanged, stream resumed at stored offsets.
  - Reconciliation (3 runs): Kafka 99,725 = sum of run publishes (298 repeats); Parquet 99,626 rows / 99,427 distinct; PostgreSQL 99,427; every row difference 0.
  - 14 vs 15: the 100-order run and the full run both rejected record_uid 92fc65a4...; run-level lines 1 + 14 (+1 re-ingestion) vs 14 unique. Accounting changed (unique counts by record_uid over the union of covered rows); the expected number was NOT adjusted.
  - Mismatch test: declaring only rec-A-r2 for a topic that holds 3 runs -> 3 UNEXPLAINED rows, exit code 1.
Defects found and fixed: (1) loaders would have imported experiment runs/dead letters into the production warehouse -> filtered by topic/run ids; (2) np.float64 not accepted by psycopg2 in the geolocation load -> explicit casts; failed attempt kept as history.
Tests: 53 host unit tests (incl. 6 new reconcile tests), 7 warehouse integration tests passing; see Phase 9 for the full suite logs.
Git: see the Phase 6 commit in docs/task_tracker.md
Blocked items: none.
Limitations: for limited runs the "rows read" count comes from the run summary (a limited run's requested limit is not stored); API enrichment match rate is 100 % for these dates (no unmatched weekend gap > 7 days occurred).
Next phase: 7 - Airflow
