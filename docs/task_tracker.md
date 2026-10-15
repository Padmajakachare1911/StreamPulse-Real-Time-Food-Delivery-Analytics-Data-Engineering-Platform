# Task Tracker

Statuses: `NOT_STARTED | IN_PROGRESS | BLOCKED | IMPLEMENTED_NOT_TESTED | TESTED | VERIFIED_WITH_EVIDENCE` (+ `PENDING_HUMAN` for manual tasks).
`VERIFIED_WITH_EVIDENCE` is only used when a saved evidence file exists (see `docs/evidence_register.md`).

| ID | Phase | Module | Task | Status | Evidence | Blocker / Notes | Commit |
|---|---|---|---|---|---|---|---|
| P0-01 | 0 | M0 | Git status & branch recorded | VERIFIED_WITH_EVIDENCE | `docs/evidence/00_environment/env_check.txt` | branch `main`, no commits before this work | |
| P0-02 | 0 | M0 | Environment versions check | VERIFIED_WITH_EVIDENCE | `docs/evidence/00_environment/env_check.txt` | `scripts/check_env.ps1` | |
| P0-03 | 0 | M0 | Kafka health + topic metadata + produce/consume | VERIFIED_WITH_EVIDENCE | `docs/evidence/00_environment/kafka_smoke_test.txt` | | |
| P0-04 | 0 | M4 | Spark↔connector compatibility check | BLOCKED | `docs/evidence/00_environment/spark_baseline_existing_stream.txt` | Host Spark fails (winutils). Unblock: run Spark in Docker (Phase 4); dependent: P4-01 | |
| P0-05 | 0 | M4 | winutils/HADOOP_HOME impact assessment | VERIFIED_WITH_EVIDENCE | `docs/evidence/00_environment/spark_baseline_existing_stream.txt` | Finding: it blocks SparkContext init; see `docs/baseline_audit.md` §4 | |
| P0-06 | 0 | M0 | `.gitignore` audit | VERIFIED_WITH_EVIDENCE | `docs/evidence/00_environment/gitignore_audit.txt` | | |
| P0-07 | 0 | — | baseline_audit / plan / tracker / register written | VERIFIED_WITH_EVIDENCE | `docs/baseline_audit.md` | | |
| P1-01 | 1 | M1 | Dataset located & hashed | NOT_STARTED | | files present in `data/raw/` | |
| P1-02 | 1 | M1 | Profiling (rows/cols/nulls/dupes) | NOT_STARTED | | | |
| P1-03 | 1 | M1 | Field mapping config | NOT_STARTED | | | |
| P1-04 | 1 | M11 | data_source / dictionary / DQ report | NOT_STARTED | | | |
| P2-01 | 2 | M3 | API chosen & justified | NOT_STARTED | | no professor API given | |
| P2-02 | 2 | M3 | API client + unit tests | NOT_STARTED | | | |
| P2-03 | 2 | M3 | Real request succeeded & stored | NOT_STARTED | | | |
| P3-01 | 3 | M2 | Replay producer (limit/dry-run/full) | NOT_STARTED | | | |
| P3-02 | 3 | M2 | Partition key + measured distribution | NOT_STARTED | | | |
| P3-03 | 3 | M2 | Full ingestion run (~100k) | NOT_STARTED | | | |
| P4-01 | 4 | M4 | Kafka→Spark→Parquet verified | NOT_STARTED | | | |
| P4-02 | 4 | M4 | Checkpoint restart verified | NOT_STARTED | | | |
| P5-01 | 5 | M5 | Rule engine + DLQ | NOT_STARTED | | | |
| P5-02 | 5 | M5 | Duplicate/idempotency proof | NOT_STARTED | | | |
| P6-01 | 6 | M6 | Schema + upsert + views | NOT_STARTED | | | |
| P6-02 | 6 | M10 | Reconciliation across layers | NOT_STARTED | | | |
| P7-01 | 7 | M7 | DAG real run | NOT_STARTED | | | |
| P8-01 | 8 | M8 | Dashboard on real data | NOT_STARTED | | | |
| P9-01 | 9 | M9 | Unit / integration / e2e runs | NOT_STARTED | | | |
| P10-01 | 10 | — | Demo 1–4 guides dry-run tested | NOT_STARTED | | | |
| P10-02 | 10 | — | Human demo runs | PENDING_HUMAN | | student performs | |
| P11-01 | 11 | — | Final audit & secrets scan | NOT_STARTED | | | |
