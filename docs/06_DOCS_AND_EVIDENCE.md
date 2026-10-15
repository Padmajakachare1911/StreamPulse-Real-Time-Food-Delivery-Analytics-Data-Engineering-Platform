# 06 — Documentation, Evidence, Tracker Templates, Viva

## 1. Documentation deliverables
Update an existing file instead of creating a duplicate.

| File | Contents | Phase |
|---|---|---|
| `README.md` | What/why, architecture summary, quick start, link map | 0→11 |
| `docs/baseline_audit.md` | Phase 0 findings | 0 |
| `docs/implementation_plan.md` | Phases, decisions, deviations | 0→ |
| `docs/task_tracker.md` | Status per task (template below) | 0→ |
| `docs/architecture.md` | Diagram matching **implemented** connections only; deviations noted | 11 |
| `docs/setup_guide.md` | venv, deps, Java, Docker, Kafka, dataset placement, API secrets, connector compatibility, Postgres, Airflow, dashboard, tests, shutdown, troubleshooting | 0→ |
| `docs/data_source.md` | Filename, URL, owner, license, attribution, nature of data | 1 |
| `docs/data_dictionary.md` | Real schema, types, meaning, mapping | 1 |
| `docs/data_quality_report.md` | Baseline profiling results | 1 |
| `docs/data_quality_rules.md` | Rules + justification | 5 |
| `docs/api_integration.md` | Provider, docs, endpoint, params, auth, real sample, HTTP status/timestamp from real run, mapping, limits, reproduction, consumers | 2 |
| `docs/manual_demo_guide.md` | 4 demos in full | 10 |
| `docs/demo_cheatsheet.md` | Copy-paste PowerShell by terminal | 10 |
| `docs/test_cases.md` | Test IDs/inputs/expected/actual/pass-fail | 9–10 |
| `docs/evidence_register.md` | Register table | continuous |
| `docs/viva_questions.md` | Simple, accurate answers | 11 |

**Cheatsheet must include, per command:** terminal name/number; app (PowerShell/VS Code/Docker/browser); working directory; exact command; whether the terminal stays open; what success looks like; independent verification; failure fix; safe stop.
Required command groups: env verification · start Docker · verify Kafka · limited & full ingestion · real API call · start Spark · check Parquet · query PostgreSQL · run Airflow · start Streamlit · run tests · capture evidence · stop services.

---

## 2. Evidence directory layout
```
docs/evidence/
  00_environment/   01_dataset/   02_api/   03_kafka/   04_spark/
  05_data_quality/  06_parquet/   07_postgresql/   08_airflow/
  09_dashboard/     10_automated_tests/
  11_manual_demos/  (demo_1_dataset_ingestion, demo_2_real_api, demo_3_data_quality, demo_4_recovery)
docs/evidence_register.md
```
**Evidence file header (every file):**
```
Task ID: <ID>
Date/time (UTC and local): …
Command: …
Working directory: …
Git commit: <hash>
--- output ---
```
Recommended filenames: `kafka_topic_description.txt`, `dataset_row_count.txt`, `api_real_request.txt`, `spark_batch_summary.log`, `parquet_readback.txt`, `postgres_reconciliation.txt`, `airflow_dag_success.png`, `demo_1_full_pipeline.png`.
**Never** include passwords, API keys, tokens. Run the secret scrubber (M10) before committing evidence. Don't fabricate screenshots/logs.

---

## 3. Evidence register template (`docs/evidence_register.md`)
| Task ID | Task | Evidence location | Actual result | Status |
|---|---|---|---|---|
| ENV-01 | Verify Python/Java/Docker versions | `docs/evidence/00_environment/env_check.txt` | _filled from real run_ | NOT_STARTED |
| KFK-01 | Kafka topic metadata | `docs/evidence/03_kafka/kafka_topic_description.txt` | | NOT_STARTED |
| … | … | … | … | … |

Extra status for human tasks: `PENDING_HUMAN`.

---

## 4. Task tracker template (`docs/task_tracker.md`)
Statuses: `NOT_STARTED | IN_PROGRESS | BLOCKED | IMPLEMENTED_NOT_TESTED | TESTED | VERIFIED_WITH_EVIDENCE`

| ID | Phase | Module | Task | Status | Evidence | Blocker / Notes | Commit |
|---|---|---|---|---|---|---|---|
| P0-01 | 0 | M0 | Git status & branch recorded | NOT_STARTED | | | |
| P0-02 | 0 | M0 | Environment versions check | NOT_STARTED | | | |
| P0-03 | 0 | M0 | Kafka health + topic metadata | NOT_STARTED | | | |
| P0-04 | 0 | M4 | Spark↔connector compatibility check | NOT_STARTED | | | |
| P0-05 | 0 | M4 | winutils/HADOOP_HOME impact assessment | NOT_STARTED | | | |
| P0-06 | 0 | M0 | `.gitignore` audit | NOT_STARTED | | | |
| P1-01 | 1 | M1 | Dataset located & hashed | NOT_STARTED | | needs Kaggle file | |
| P1-02 | 1 | M1 | Profiling (rows/cols/nulls/dupes) | NOT_STARTED | | | |
| P1-03 | 1 | M1 | Field mapping config | NOT_STARTED | | | |
| P1-04 | 1 | M11 | data_source / dictionary / DQ report | NOT_STARTED | | | |
| P2-01 | 2 | M3 | API chosen & justified | NOT_STARTED | | check professor's choice | |
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
| P10-02 | 10 | — | Human demo runs | PENDING_HUMAN | | | |
| P11-01 | 11 | — | Final audit & secrets scan | NOT_STARTED | | | |

---

## 5. Viva topics (`docs/viva_questions.md`) — simple, accurate answers required
1. What StreamPulse does · 2. Why a Kaggle dataset · 3. Why replay historical data into Kafka · 4. Why replay ≠ live feed · 5. The actual API and its role · 6. Why Kafka · 7. Topics, partitions, offsets, keys, consumers · 8. Spark Structured Streaming (micro-batch, triggers) · 9. Checkpoints and recovery · 10. Parquet vs PostgreSQL · 11. SQL analytics · 12. ETL vs ELT (and which this is) · 13. Data quality + dead-letter · 14. Deduplication + idempotency · 15. Airflow + DAGs (and why the stream isn't a DAG task) · 16. Streamlit · 17. Lineage and provenance · 18. Count reconciliation · 19. Limitations + future work (e.g., real live source, Schema Registry, Kubernetes, monitoring, Delta/Iceberg).

**Format for each answer:** *Short answer (1–2 sentences) → Simple analogy → Where it is in our project (file/command) → Honest limitation.*

**Model honest answers to prepare**
- "Is this real-time?" → "It's a streaming architecture processing a historical dataset replayed at a controlled rate. The API provides real external reference data. We don't claim live customer orders."
- "Is it exactly-once?" → "No. At-least-once delivery with idempotent database writes, so repeated records don't create duplicates in the warehouse."

---

## 6. Final submission checklist (Phase 11)
- [ ] `git status` clean; all verified work pushed (command output saved)
- [ ] `.env` and secrets absent from repo and evidence (scan done)
- [ ] Reconciliation table filled with real numbers for the full run
- [ ] API provenance visible in DB and dashboard
- [ ] Tracker statuses honest; every `VERIFIED_WITH_EVIDENCE` has a file path
- [ ] Architecture diagram matches the code
- [ ] 4 demos dry-run tested; human steps marked `PENDING_HUMAN` until performed
- [ ] Dataset placement instructions for evaluator in `README.md`
- [ ] Final report: verified vs pending vs blocked
