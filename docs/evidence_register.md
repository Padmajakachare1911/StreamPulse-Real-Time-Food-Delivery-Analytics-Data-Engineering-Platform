# Evidence Register

Every row points to a real saved file. Status uses the same vocabulary as `docs/task_tracker.md` (+ `PENDING_HUMAN`).

| Task ID | Task | Evidence location | Actual result | Status |
|---|---|---|---|---|
| ENV-01 | Verify Python/Java/Docker versions | `docs/evidence/00_environment/env_check.txt` | Python 3.11.9, OpenJDK 17.0.20.1, Docker 29.8.2, Compose v5.6.0, PySpark 4.2.0, kafka-python 3.0.11; Kafka port 9092 reachable | VERIFIED_WITH_EVIDENCE |
| KFK-00 | Kafka produce+consume smoke test | `docs/evidence/00_environment/kafka_smoke_test.txt` | 1 message produced and consumed on topic `smoke_test`; `food_orders` has 3 partitions, RF 1 | VERIFIED_WITH_EVIDENCE |
| SPK-00 | Existing Spark stream baseline | `docs/evidence/00_environment/spark_baseline_existing_stream.txt` | `SparkContext` failed on host: HADOOP_HOME/winutils missing; no Parquet written | VERIFIED_WITH_EVIDENCE (failure recorded) |
| GIT-00 | `.gitignore` audit | `docs/evidence/00_environment/gitignore_audit.txt` | `.env`, `.venv`, raw data, Parquet, checkpoints, DLQ ignored; `.env.example` and `.gitkeep` not ignored | VERIFIED_WITH_EVIDENCE |
| DS-01 | Dataset row counts + SHA-256 + profile | `docs/evidence/01_dataset/dataset_row_count.txt`, `dataset_profile.txt`, `dataset_profile.json` | orders 99,441 rows / 8 cols; order_id unique; all timestamps parse; 9 files hashed | VERIFIED_WITH_EVIDENCE |
| DS-02 | Order join + mapping over all orders | `docs/evidence/01_dataset/join_and_mapping_check.txt` | 99,441 in, 99,441 out; 775 without items, 1 without payments; 0 mapping rejections | VERIFIED_WITH_EVIDENCE |
| DS-03 | Dataset license | `docs/evidence/01_dataset/license_check.txt` | CC BY-NC-SA 4.0 (search-result excerpt of Kaggle page, not a screenshot) | TESTED |
| API-01 | Real API request (one week) | `docs/evidence/02_api/api_real_request.txt` | HTTP 200, 5 daily BRL->USD rates, retrieved 2026-10-15 | VERIFIED_WITH_EVIDENCE |
| API-02 | Real API request (full order span) | `docs/evidence/02_api/api_full_range_request.txt` | HTTP 200, 544 rows, 2016-09-04..2018-10-17 | VERIFIED_WITH_EVIDENCE |
| API-03 | Client retry/backoff against unreachable host | `docs/evidence/02_api/api_retry_backoff_simulated.txt` | 3 attempts, waits 1s then 2s, clean error (simulated failure of the real client, not a mock API) | VERIFIED_WITH_EVIDENCE |
| KFK-01 | Topic description | `docs/evidence/03_kafka/kafka_topic_description.txt` | `ecommerce_orders`: 3 partitions, RF 1 | VERIFIED_WITH_EVIDENCE |
| KFK-02 | Producer dry run (full file) | `docs/evidence/03_kafka/producer_dry_run_full.txt`, `dead_letter_sample_dryrun.txt` | 99,441 read, 99,427 valid, 14 rejected (INCONSISTENT), nothing published; 14 dead-letter lines | VERIFIED_WITH_EVIDENCE |
| KFK-03 | Limited run (100) | `docs/evidence/03_kafka/producer_limited_100.txt`, `partition_counts_after_limited_run.txt` | 99 published + 1 rejected; broker offsets 31/42/26 = callbacks 31/42/26 | VERIFIED_WITH_EVIDENCE |
| KFK-04 | Full run | `docs/evidence/03_kafka/producer_full_run.txt`, `partition_counts_before_full_run.txt`, `partition_counts_after_full_run.txt` | published 99,427 = valid; failed 0; broker delta 33,199 + 33,255 + 32,973 = 99,427 | VERIFIED_WITH_EVIDENCE |
| KFK-05 | Key choice | `docs/evidence/03_kafka/key_options_computed_offline.txt` | order_id 33.4/33.4/33.2 % vs customer_state 22.2/66.9/10.9 % (offline computation); order_id chosen; measured on broker in KFK-04 | VERIFIED_WITH_EVIDENCE |
| KFK-06 | Sample message consumed | `docs/evidence/03_kafka/consume_sample_messages.txt` | envelope with payload, raw, record_uid, api_enrichment | VERIFIED_WITH_EVIDENCE |
| SPK-01 | Spark transform tests (in Docker) | `docs/evidence/04_spark/spark_transform_tests_docker.txt` | 5 passed; one test caught a real bug (malformed JSON was labelled SCHEMA_MISMATCH) which was fixed in the code, not the test | VERIFIED_WITH_EVIDENCE |
| SPK-02 | Spark Parquet read-back | `docs/evidence/04_spark/parquet_readback_spark.txt` | 99,427 rows = 99,427 distinct record_uid; each of 10 batch folders equals its batch log | VERIFIED_WITH_EVIDENCE |
| SPK-03 | Mid-backlog recovery (final) | `docs/evidence/04_spark/recovery_test.txt`, `docs/evidence/04_spark/recovery_checkpoint_and_offsets.txt`, `docs/evidence/04_spark/recovery_postgres_counts.txt` | 18/18 checks: stopped after batches 0-1, 4,995 unread; restart began at offset 1000; consumed 8,094 = topic total; PostgreSQL 7,995 unique; 99 cross-batch duplicates counted as retry_duplicates | VERIFIED_WITH_EVIDENCE |
| SPK-04 | Recovery attempt 1 (stop while a batch was running) | `docs/evidence/04_spark/recovery_test_attempt1_stop_during_batch.txt` | 16/16 checks passed, but the log showed a py4j error at stop; the stop was then changed to wait for the in-flight batch | VERIFIED_WITH_EVIDENCE (kept as history) |
| SPK-05 | Stream dead-letter with real Spark | `docs/evidence/04_spark/malformed_message_test.txt` | 7/7: 1 MALFORMED_JSON + 2 SCHEMA_MISMATCH reached data/dead_letter stream sink; only 5 real orders in Parquet | VERIFIED_WITH_EVIDENCE |
| SPK-06 | Production checkpoint untouched | `docs/evidence/04_spark/production_checkpoint_untouched.txt` | data/checkpoints/orders: 10 commits, same offsets, same file times as the original run | VERIFIED_WITH_EVIDENCE |
| REC-01 | Count reconciliation, production topic | `docs/evidence/04_spark/reconciliation_production.txt` | all 12 stage rows have difference 0 (stages 1 and 3 include the 100-order test run) | VERIFIED_WITH_EVIDENCE |
| DQ-01 | Quality rules + mapping checks | `docs/evidence/05_data_quality/pytest_validators_and_mapping.txt` | 32 passed (every reject rule fires; empty delivery date for non-delivered orders is valid; dead-letter format) | VERIFIED_WITH_EVIDENCE |
| DQ-02 | Invalid row, source duplicate and re-ingestion on the Demo 3 file | `docs/evidence/05_data_quality/quality_idempotency_test.txt` | 15/15 PASS: 3 read = 2 valid + 1 rejected; dead letter RANGE_ERROR with rule name; PostgreSQL 1 row after two ingestions; duplicate logged once; Parquet 4 rows / 2 distinct | VERIFIED_WITH_EVIDENCE |
| DQ-03 | Duplicate classification in PostgreSQL | `docs/evidence/05_data_quality/pytest_duplicates_postgres.txt` | 7 passed on real PostgreSQL (retry, source, in-batch, counts add up, re-ingested discarded copy) | VERIFIED_WITH_EVIDENCE |
| DEMO-1..4 | Four manual demos | `docs/evidence/11_manual_demos/` | to be performed by the student | PENDING_HUMAN |
