# Task Tracker

Statuses: `NOT_STARTED | IN_PROGRESS | BLOCKED | IMPLEMENTED_NOT_TESTED | TESTED | VERIFIED_WITH_EVIDENCE` (+ `PENDING_HUMAN` for manual tasks).
`VERIFIED_WITH_EVIDENCE` is only used when a saved evidence file exists (see `docs/evidence_register.md`).

| ID | Phase | Module | Task | Status | Evidence | Blocker / Notes | Commit |
|---|---|---|---|---|---|---|---|
| P0-01 | 0 | M0 | Git status & branch recorded | VERIFIED_WITH_EVIDENCE | `docs/evidence/00_environment/env_check.txt` | branch main; no commits before this work | c3fc6ee |
| P0-02 | 0 | M0 | Environment versions check | VERIFIED_WITH_EVIDENCE | `docs/evidence/00_environment/env_check.txt` | `scripts/check_env.ps1` | c3fc6ee |
| P0-03 | 0 | M0 | Kafka health + topic metadata + produce/consume | VERIFIED_WITH_EVIDENCE | `docs/evidence/00_environment/kafka_smoke_test.txt` |  | c3fc6ee |
| P0-04 | 0 | M4 | Spark↔connector compatibility check | VERIFIED_WITH_EVIDENCE | `docs/evidence/04_spark/spark_transform_tests_docker.txt`, `docs/evidence/04_spark/recovery_test.txt` | Resolved by running Spark 4.2.0 (Scala 2.13) + spark-sql-kafka-0-10_2.13:4.2.0 in Docker: reads Kafka, writes Parquet | PHASE4 |
| P0-05 | 0 | M4 | winutils/HADOOP_HOME impact assessment | VERIFIED_WITH_EVIDENCE | `docs/evidence/00_environment/spark_baseline_existing_stream.txt` | Finding: blocks SparkContext init; see baseline_audit section 4 | c3fc6ee |
| P0-06 | 0 | M0 | `.gitignore` audit | VERIFIED_WITH_EVIDENCE | `docs/evidence/00_environment/gitignore_audit.txt` |  | c3fc6ee |
| P0-07 | 0 | — | baseline_audit / plan / tracker / register written | VERIFIED_WITH_EVIDENCE | `docs/baseline_audit.md` |  | c3fc6ee |
| P1-01 | 1 | M1 | Dataset located & hashed | VERIFIED_WITH_EVIDENCE | `docs/evidence/01_dataset/dataset_row_count.txt` | 9 files hashed in dataset_profile.txt | 802c4a4 |
| P1-02 | 1 | M1 | Profiling (rows/cols/nulls/dupes) | VERIFIED_WITH_EVIDENCE | `docs/evidence/01_dataset/dataset_profile.txt` | rows, nulls, duplicates, orphans, consistency | 802c4a4 |
| P1-03 | 1 | M1 | Field mapping config | VERIFIED_WITH_EVIDENCE | `docs/evidence/01_dataset/join_and_mapping_check.txt` | 8 unit tests pass; 0 of 99,441 rows rejected by mapping | 802c4a4 |
| P1-04 | 1 | M11 | data_source / dictionary / DQ report | VERIFIED_WITH_EVIDENCE | `docs/data_quality_report.md`; `docs/evidence/01_dataset/license_check.txt` | license from Kaggle page excerpt; student should re-confirm on the page | 802c4a4 |
| P2-01 | 2 | M3 | API chosen & justified | VERIFIED_WITH_EVIDENCE | `docs/api_integration.md` | Frankfurter (ECB); no professor API given; reasoning documented | a9f479d |
| P2-02 | 2 | M3 | API client + unit tests | VERIFIED_WITH_EVIDENCE | `docs/evidence/02_api/api_retry_backoff_simulated.txt` | 15 unit tests (mocked HTTP) pass; retry/backoff 1s,2s shown against a real unreachable host | a9f479d |
| P2-03 | 2 | M3 | Real request succeeded & stored | VERIFIED_WITH_EVIDENCE | `docs/evidence/02_api/api_real_request.txt`, `api_full_range_request.txt` | HTTP 200 (5 rows; 544 rows full span). DB storage in core.api_cache is done in Phase 6 (P6-01) | a9f479d |
| P3-01 | 3 | M2 | Replay producer (limit/dry-run/full) | VERIFIED_WITH_EVIDENCE | `docs/evidence/03_kafka/producer_dry_run_full.txt`, `producer_limited_100.txt` | limit/dry-run/full work; 9 producer unit tests pass | PHASE3 |
| P3-02 | 3 | M2 | Partition key + measured distribution | VERIFIED_WITH_EVIDENCE | `docs/evidence/03_kafka/partition_counts_after_full_run.txt`, `key_options_computed_offline.txt` | key=order_id; measured 33,199/33,255/32,973 | PHASE3 |
| P3-03 | 3 | M2 | Full ingestion run (~100k) | VERIFIED_WITH_EVIDENCE | `docs/evidence/03_kafka/producer_full_run.txt` | 99,441 read = 99,427 valid + 14 rejected; 99,427 published, 0 failed | PHASE3 |
| P4-01 | 4 | M4 | Kafka→Spark→Parquet verified | VERIFIED_WITH_EVIDENCE | `docs/evidence/04_spark/parquet_readback_spark.txt`, `docs/evidence/04_spark/reconciliation_production.txt` | Kafka->Spark->Parquet->PostgreSQL: 99,526 messages consumed, 99,427 unique rows in Parquet and PostgreSQL; Spark read-back matches all 10 batch logs | 792df69 |
| P4-02 | 4 | M4 | Checkpoint restart verified | VERIFIED_WITH_EVIDENCE | `docs/evidence/04_spark/recovery_test.txt`, `docs/evidence/04_spark/malformed_message_test.txt` | Isolated experiment: stop after 2 committed batches, publish 2,000 new + 99 duplicate records while down, restart: resumed at offset 1000, 18/18 checks pass; stream dead-letter 7/7. Attempt 1 (stop during a batch) also passed, kept as history | PHASE4 |
| P5-01 | 5 | M5 | Rule engine + DLQ | VERIFIED_WITH_EVIDENCE | `docs/evidence/05_data_quality/quality_idempotency_test.txt`, `docs/evidence/05_data_quality/pytest_validators_and_mapping.txt` | 9 rules + mapping checks, 32 unit tests pass; Demo 3 file: invalid row -> dead letter (RANGE_ERROR, rule named); stream dead letter in docs/evidence/04_spark/malformed_message_test.txt | PHASE5 |
| P5-02 | 5 | M5 | Duplicate/idempotency proof | VERIFIED_WITH_EVIDENCE | `docs/evidence/05_data_quality/quality_idempotency_test.txt`, `docs/evidence/05_data_quality/pytest_duplicates_postgres.txt` | retry vs source duplicate classified; re-ingestion keeps PostgreSQL at 1 row; 15/15 experiment checks, 7 warehouse tests | PHASE5 |
| P6-01 | 6 | M6 | Schema + upsert + views | NOT_STARTED | | | |
| P6-02 | 6 | M10 | Reconciliation across layers | NOT_STARTED | | | |
| P7-01 | 7 | M7 | DAG real run | NOT_STARTED | | | |
| P8-01 | 8 | M8 | Dashboard on real data | NOT_STARTED | | | |
| P9-01 | 9 | M9 | Unit / integration / e2e runs | NOT_STARTED | | | |
| P10-01 | 10 | — | Demo 1–4 guides dry-run tested | NOT_STARTED | | | |
| P10-02 | 10 | — | Human demo runs | PENDING_HUMAN | | student performs | |
| P11-01 | 11 | — | Final audit & secrets scan | NOT_STARTED | | | |
