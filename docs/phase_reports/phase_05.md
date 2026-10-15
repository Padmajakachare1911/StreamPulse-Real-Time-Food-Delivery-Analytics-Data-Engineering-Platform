Phase: 5 - Data quality, deduplication and dead-letter handling
Completed: rules re-checked against the real data (docs/data_quality_rules.md: every rule has a measured real-data count); duplicate classification (retry vs source vs re-ingestion) proven on real PostgreSQL; invalid-row dead-letter proven end to end on the real orders schema; idempotent re-ingestion proven; Parquet-vs-PostgreSQL semantics documented.
Defect found and fixed: a discarded source duplicate that was ingested again would have been logged a second time in core.duplicate_records (its record_uid is not in core.orders). The writer now treats a record_uid already in core.duplicate_records as a retry, and a unique index (record_uid, duplicate_type) protects the table.
Files changed: src/warehouse/load.py, sql/schema.sql, tests/integration/test_warehouse.py (+1 test), scripts/quality_experiment.py, scripts/spark_experiments.py (publish --input), data/test_inputs/demo3_records.csv, docs/data_quality_rules.md, docs/evidence/05_data_quality/*
Commands executed:
  python -m pytest tests/unit/test_validators.py tests/unit/test_mapping.py tests/unit/test_producer.py -v
  python -m pytest tests/integration/test_warehouse.py -v
  python scripts/quality_experiment.py --tag q1
Verification (docs/evidence/05_data_quality/):
  - Demo 3 file (real orders schema; 1 valid, 1 delivered-before-purchase, 1 exact copy): producer read 3 = 2 valid + 1 rejected; dead-letter record has dlq_id, stage=ingest, reason_code=RANGE_ERROR, rule delivered_not_before_purchase, record_uid, ingest_run_id, raw_payload, rejected_at.
  - Spark+PostgreSQL: Parquet 2 rows, PostgreSQL 1 row, copy logged once as source_duplicate.
  - Re-ingestion (new run id, same file): same record_uid, PostgreSQL still 1 row, second batch retry_duplicates=2 inserted=0, duplicate_records still 1, Parquet 4 rows / 2 distinct, dead letters 2 rows (one per run) for 1 distinct record, loading twice adds nothing.
  - Malformed / schema-invalid messages have distinct reason codes (MALFORMED_JSON vs SCHEMA_MISMATCH): docs/evidence/04_spark/malformed_message_test.txt.
Tests: 32 unit passed, 7 warehouse integration passed, experiment 15/15.
Git: see commit "test(quality): ..." in git log (hash recorded in docs/task_tracker.md)
Blocked items: none. Limitation: the real orders file has no source duplicates (order_id is unique), so source-duplicate behavior is shown with the isolated Demo 3 file, not with real duplicates.
Next phase: 6 - PostgreSQL warehouse and reconciliation
