Phase: 4 - Spark Structured Streaming
Completed: Spark job (foreachBatch: parse, validate, in-batch dedup, per-batch Parquet folder, PostgreSQL upsert, dead-letter, batch logs, graceful stop); runs in Docker because host Spark needs winutils; first full pass over the production topic; isolated mid-backlog recovery experiment; isolated malformed-message experiment.
Files changed: src/streaming/{order_stream,transforms}.py, src/producer/dataset_producer.py (--skip), docker/spark/Dockerfile, docker-compose.yml, scripts/{run_stream,stop_stream,run_tests_docker,parquet_readback_spark,spark_experiments}.*, tests/unit/{test_spark_transforms,test_producer}.py, .gitignore (data/test_runs/), docs/evidence/04_spark/*
Commands executed:
  scripts\run_tests_docker.ps1
  scripts\run_stream.ps1 (production stream, checkpoint data/checkpoints/orders, never reset)
  docker compose --profile spark run --rm --no-deps -T spark python scripts/parquet_readback_spark.py
  python scripts\spark_experiments.py recovery --tag r1   (attempt 1)
  python scripts\spark_experiments.py recovery --tag r2   (final)
  python scripts\spark_experiments.py malformed --tag m1
  python scripts\reconcile.py
Verification:
  - Production: 99,526 Kafka messages consumed in 10 batches; Parquet 99,427 rows; PostgreSQL 99,427 rows = distinct record_uid = distinct order_id; reconciliation differences all 0.
  - Recovery (r2): 6k real orders published, Spark stopped after batches 0-1 (4,995 unread). While down: 2,000 new real orders + 99 repeated record_uids published. Restart with same checkpoint: first batch started at offset 1000 (= stored offset), batch ids 0..16 contiguous, sum(input)=8,094=topic total, Parquet 8,094 rows / 7,995 distinct, PostgreSQL 7,995 rows, retry_duplicates=99 detected across batches. 18/18 checks.
  - Dead letter: 3 crafted bad messages -> stage=stream records with MALFORMED_JSON (1) and SCHEMA_MISMATCH (2); 5 real valid orders in Parquet. 7/7 checks.
Findings and fixes during this phase (all honest history):
  1. Unit test caught that malformed JSON was labelled SCHEMA_MISMATCH (from_json returns a struct of nulls, not null). Code fixed to check JSON validity separately; test left unchanged.
  2. First stop/restart script waited on stdout while logs go to stderr, so it never stopped mid-backlog. Replaced by scripts/spark_experiments.py (reads merged output, bounded timeouts, fails on any unmet condition).
  3. Recovery attempt 1 passed but its log showed a py4j error because stop interrupted an in-flight batch (the batch was re-run correctly after restart). Stop now waits for the in-flight batch (max 120 s); attempt 2 has no errors in either run.
Tests: host unit 47 passed; Spark transform tests 5 passed (Docker); warehouse integration 6 passed; experiments 18/18 and 7/7.
Git: implementation 4c17922, read-back check 792df69, experiments commit "test(spark): ..." (see git log); push status recorded in the final command output.
Blocked items: none.
Remaining gaps (documented, not hidden):
  - Parquet is a landing layer: cross-batch retry duplicates stay in Parquet (distinct record_uid is the unique count); PostgreSQL is deduplicated.
  - The graceful stop has a small race (a batch can start just after the check); the idempotent design makes that safe, as attempt 1 showed.
  - Stages 1/3 of the reconciliation accept rows_read from the run summary for limited runs.
  - PySpark warns it does not fully support pandas >= 3.0 (job does not use pandas).
Next phase: 5 - Data quality and idempotency proof (not started until this gate is accepted).
