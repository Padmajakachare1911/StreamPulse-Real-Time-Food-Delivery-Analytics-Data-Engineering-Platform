# 03 — Module Specifications

Each module lists: **Purpose · Paths · Interfaces · Requirements · Tests · Evidence · Definition of Done (DoD)**. Paths adapt to what exists in the repo; inspect first.

Module map:
| ID | Module | Phase |
|---|---|---|
| M0 | Environment & Config | 0 |
| M1 | Dataset Profiling & Mapping | 1 |
| M2 | Kafka Replay Producer | 3 |
| M3 | Real API Client & Enrichment | 2 |
| M4 | Spark Structured Streaming | 4 |
| M5 | Data Quality, Dedup, Dead-Letter | 5 |
| M6 | Parquet + PostgreSQL Warehouse | 6 |
| M7 | Airflow Orchestration | 7 |
| M8 | Streamlit Dashboard | 8 |
| M9 | Testing | 9 |
| M10 | Reconciliation & Evidence Automation | 3–11 |
| M11 | Documentation & Viva | continuous |

---

## M0 — Environment & Config
**Purpose:** reproducible environment, single config source.
**Paths:** `src/common/config.py`, `.env`, `.env.example`, `requirements.txt`, `docker-compose.yml`, `scripts/check_env.ps1`
**Requirements**
- `config.py` loads from env (via `python-dotenv` or similar) with defaults: `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`, `DATASET_PATH`, `PARQUET_OUTPUT_DIR`, `CHECKPOINT_DIR`, `DEAD_LETTER_DIR`, `POSTGRES_*`, `API_BASE_URL`, `API_KEY` (if any), `REPLAY_RATE`, `BATCH_SIZE`.
- `.env.example` has placeholders only.
- `scripts/check_env.ps1` prints versions (Python, Java, Docker, compose), container status, topic metadata, and port reachability → writes to evidence.
- Pin dependency versions; note PySpark 4.2.0 ↔ Scala 2.13 connector.
**Tests:** config loads defaults/overrides; missing required var raises clear error.
**Evidence:** `docs/evidence/00_environment/env_check.txt`
**DoD:** fresh `.venv` + `pip install -r requirements.txt` + `check_env.ps1` all succeed.

---

## M1 — Dataset Profiling & Mapping
**Purpose:** understand the real dataset; define the mapping to canonical fields.
**Paths:** `scripts/profile_dataset.py`, `config/field_mapping.yaml`, `src/common/mapping.py`, `data/raw/`
**Requirements**
- Profile: rows, columns, dtypes, null counts, duplicates (full-row and candidate-key), min/max, timestamp parse rate, categorical value counts, sample rows. Output text + JSON.
- SHA-256 of raw file recorded.
- Mapping config declares per field: `source_column`, `canonical_name`, `type`, `required`, `rules`. **No invented columns.**
- `mapping.py` applies mapping + type conversion, returns `(mapped_record | None, errors[])`.
- Original columns preserved in `raw`.
**Tests:** mapping happy path; missing required; bad type; extra columns preserved.
**Evidence:** `docs/evidence/01_dataset/dataset_profile.txt`, `dataset_row_count.txt`
**DoD:** docs `data_source.md`, `data_dictionary.md`, `data_quality_report.md` written from real numbers.

---

## M2 — Kafka Replay Producer
**Purpose:** replay the real dataset into Kafka as *historical dataset replay*.
**Paths:** `src/producer/dataset_producer.py` (keep/retire `order_producer.py` after audit; don't delete without reason), `scripts/kafka_*.ps1`
**CLI:** `--input`, `--bootstrap`, `--topic`, `--limit N`, `--dry-run`, `--full`, `--rate rec/s`, `--batch-size`, `--run-id`, `--key-field`
**Requirements**
- Stream-read the file (chunked) — don't assume size.
- Validate per record (M5 rules); invalid → DLQ file/topic, counted.
- Wrap in envelope (see `04_DATA_CONTRACTS.md`): `record_uid`, `source_file_sha256`, `source_row_number`, `ingest_run_id`, `source_category=kafka_replayed`.
- Key = deliberate choice based on actual data; document ordering semantics.
- Producer config: `acks=all`, `enable.idempotence=true` (if client supports), retries with backoff, `linger`/batch for throughput, `flush()` + safe shutdown on Ctrl+C.
- Delivery callbacks count success/failure.
- Progress log every N records; final summary JSON in `data/processed/ingestion_runs/`.
- Rerun strategy: `record_uid` is deterministic (file hash + row number) so reruns produce the same IDs → downstream upsert dedups.
**Scripts (PowerShell):** `kafka_start.ps1`, `kafka_stop.ps1`, `kafka_list_topics.ps1`, `kafka_describe_topic.ps1`, `kafka_consume_n.ps1`, `kafka_partition_counts.ps1`.
**Tests:** serialization; key selection; invalid routed to DLQ; dry-run publishes nothing; failure counted.
**Evidence:** `docs/evidence/03_kafka/` — topic description, partition offsets (measured), limited run log, full run log.
**DoD:** read == valid + rejected; published == valid (or explained); partition distribution measured, not assumed.

---

## M3 — Real API Client & Enrichment
**Purpose:** satisfy the "actual API" requirement truthfully.
**Paths:** `src/api/client.py`, `src/api/enrich.py`, `scripts/api_demo_call.py`, `tests/test_api_client.py`
**Selection:** see Phase 2 in `02_WORKFLOW.md`. Choose a design:
- *Enrichment:* dedupe lookup keys (e.g., unique location/city/coordinate), call API once per unique key, cache results, join to records with provenance.
- *Additional source:* fetch API records on a schedule, ingest as `api_sourced`.
**Client requirements:** configurable base URL/endpoint; auth via env; connect+read timeouts; status validation; retry with exponential backoff on 429/5xx/timeouts; honor `Retry-After`; respect provider rate limit (e.g., throttle to documented req/s; set a proper `User-Agent` if required by ToS); response schema validation; empty/malformed handling; sanitized logging (no secrets); persistent cache (SQLite/JSON/Postgres table).
**Provenance fields:** `api_source_name`, `api_endpoint`, `api_retrieved_at`, `api_http_status`, `api_resource_id` (if any).
**Tests (unit, mocked):** success, 4xx, 429 with retry, 5xx retry then fail, timeout, malformed JSON, schema mismatch, cache hit. **Plus** a real-call integration test marked `@pytest.mark.integration` (skipped if offline).
**Evidence:** `docs/evidence/02_api/api_real_request.txt` (command, timestamp, HTTP status, sanitized response), stored result rows.
**DoD:** real request succeeded **and** data stored/processed; `docs/api_integration.md` complete. Mocks are never evidence.

---

## M4 — Spark Structured Streaming
**Purpose:** consume from Kafka, parse, transform, write Parquet + PostgreSQL.
**Paths:** `src/streaming/order_stream.py`, `src/streaming/transforms.py`, `scripts/run_stream.ps1`
**Requirements**
- Packages: `org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0` (match installed Spark/Scala; verify).
- Read: `startingOffsets` configurable (`earliest` for demos); `maxOffsetsPerTrigger` configurable.
- Parse: `from_json` with schema generated from mapping config; keep `kafka_topic/partition/offset/timestamp` columns.
- Malformed JSON / null parse result → dead-letter sink (with reason).
- `foreachBatch(process_batch)`: (1) split valid/invalid, (2) in-batch dedup on `record_uid`, (3) write Parquet (append, coalesced to avoid tiny files), (4) upsert into PostgreSQL via batched JDBC or `psycopg2.extras.execute_values` into a staging table + `INSERT … ON CONFLICT`, (5) log `batch_id`, input/valid/rejected/dup/written counts.
- Checkpoint dir separate from output dir; from config.
- Trigger: `processingTime` (e.g., 5s) or `availableNow` for finite demo — document choice.
- Safe stop: `stop_stream.ps1` / Ctrl+C handler / sentinel file.
- Windows: investigate `winutils`/`HADOOP_HOME` — only fix if it breaks writes. Consider Docker-run Spark only if Windows path proves unreliable (document trade-off).
**Tests:** transformations on a static DataFrame; parse malformed; dedup; `process_batch` with a fake batch.
**Evidence:** `docs/evidence/04_spark/` — run logs with batch summaries, `parquet_readback.txt`, checkpoint directory listing, restart log.
**DoD:** Kafka→Spark→Parquet verified with read-back; restart doesn't duplicate.

---

## M5 — Data Quality, Dedup, Dead-Letter
**Purpose:** predictable handling of bad/repeated data.
**Paths:** `src/processing/validators.py`, `config/quality_rules.yaml`, `src/processing/deadletter.py`, `docs/data_quality_rules.md`
**Rules (derive from real data; examples only if the columns exist):** required fields present; numeric parse; non-negative amounts/quantities where meaningful; timestamp parse + sane range; allowed categorical values (only if the real set is closed); impossible combos.
**Duplicate taxonomy**
| Type | Detection | Handling |
|---|---|---|
| Delivery retry | same `record_uid` | upsert no-op; count as `retry_duplicate` |
| Source duplicate | different `record_uid`, same `business_hash`/natural key | keep first (or per documented policy), count as `source_duplicate`, optionally log to `duplicates` table |
| Re-ingestion | same file hash, new `ingest_run_id` | same `record_uid` → idempotent |
**Dead-letter record:** `{dlq_id, record_uid?, stage (ingest|stream|db), reason_code, reason_detail, raw_payload, ingest_run_id, rejected_at}` → `data/dead_letter/*.jsonl` and `dead_letter_records` table.
**Tests:** each rule pass/fail; duplicate classification; DLQ format; rule justifications documented.
**Evidence:** `docs/evidence/05_data_quality/` — rejection output with reason, duplicate proof (before/after counts).
**DoD:** invalid + duplicate demos use real schema, with observable results; re-run proven safe.

---

## M6 — Parquet + PostgreSQL Warehouse
**Purpose:** durable storage + analytics.
**Paths:** `sql/schema.sql`, `sql/analytics.sql`, `sql/quality_checks.sql`, `sql/reconciliation.sql`, `src/warehouse/db.py`, `src/warehouse/load.py`, `scripts/init_db.ps1`
**Parquet:** `data/processed/parquet/` ; types explicit; partition only if justified by real data (e.g., by date if timestamps exist and cardinality is reasonable); avoid tiny files; checkpoints elsewhere.
**PostgreSQL (Docker Compose service):** schemas `staging`, `core`, `analytics` (or simpler if appropriate). Tables derived from the **real** schema:
- `core.food_orders` (PK `record_uid`; typed columns from mapping; provenance columns)
- `core.dead_letter_records`
- `core.ingestion_runs` (run counts)
- `core.api_cache` / `core.api_enriched_*` (from M3)
- Views in `analytics.*` for KPIs actually supported by the data — **no manufactured metrics** (revenue only if an amount column exists).
**Requirements:** parameterized queries; transactions; `ON CONFLICT (record_uid) DO NOTHING/UPDATE` (document choice); indexes only where justified; repeatable `init_db` (idempotent `CREATE … IF NOT EXISTS`); connection pooling not required — batch writes.
**Verification SQL for professor:** total rows; distinct `record_uid`; duplicates; rejected; time range; analytic samples; layer reconciliation.
**Tests:** schema creation; upsert idempotency (insert twice → one row); analytics query on fixture; reconciliation function.
**Evidence:** `docs/evidence/06_parquet/`, `07_postgresql/` (`postgres_reconciliation.txt`, schema dump, query outputs).
**DoD:** re-ingestion doesn't change unique count; reconciliation table filled with real numbers.

---

## M7 — Airflow Orchestration
**Purpose:** schedule/monitor **finite** tasks only (never the infinite stream as a normal task).
**Paths:** `airflow/dags/streampulse_batch_dag.py`, `airflow/docker-compose` (or section in main compose), `scripts/airflow_*.ps1`
**Candidate tasks:** `check_dataset_present` → `validate_dataset_profile` → `ingest_batch_to_kafka` (limited/`availableNow`) → `run_spark_available_now` (finite) → `run_quality_checks_sql` → `reconcile_counts` → `refresh_analytics` → (`api_enrichment_refresh` if safe).
**Requirements:** explicit dependencies; `retries` + `retry_delay`; env-based config; idempotent tasks; useful logs; failure visible (e.g., a deliberate failing task variant or intentionally unavailable dependency to show retry); documented start/stop.
**Tests:** DAG import test (no import errors); task-dependency assertion.
**Evidence:** `docs/evidence/08_airflow/` — DAG success screenshot (`airflow_dag_success.png`, `PENDING_HUMAN` if a screenshot), task logs, retry demonstration.
**DoD:** a real DAG run completed; not marked done on import alone.

---

## M8 — Streamlit Dashboard
**Purpose:** truthful, simple demo UI.
**Paths:** `src/dashboard/app.py`, `src/dashboard/queries.py`, `scripts/run_dashboard.ps1`
**Sections:** overview + honest architecture note ("historical replay"); dataset info; ingestion counts; valid/rejected/duplicate; API status + `api_sourced/enriched` counts; KPIs supported by schema; charts/trends; DQ summary; recent records; reconciliation table; data source + last-refresh timestamps.
**Status indicators:** computed from real checks (Kafka metadata call, DB ping, API last-success timestamp, file existence). On failure show red/"unavailable" with error. Never hard-code green.
**Tests:** query functions on fixture DB; status-check functions return failure when service is down.
**Evidence:** `docs/evidence/09_dashboard/` screenshots (`PENDING_HUMAN` for live captures).
**DoD:** every number traces to PostgreSQL/Parquet; service-down state verified.

---

## M9 — Testing
**Layout:** `tests/unit/`, `tests/integration/`, `tests/e2e/`; markers `unit`, `integration`, `e2e`, `manual`.
**Coverage required:** config; mapping; validation; producer serialization/errors; API client (mocked) + real-call integration; dedup/idempotency; Spark transforms; DLQ; PostgreSQL persistence; analytics queries; reconciliation.
**Commands:**
```powershell
pytest -m unit -v
pytest -m integration -v
pytest -m e2e -v
```
Save output to `docs/evidence/10_automated_tests/pytest_<type>_<date>.txt` with exact command and result.
**Rules:** don't edit tests to pass; mocks don't count as real integration proof.
**DoD:** all pass or failures honestly documented with cause.

---

## M10 — Reconciliation & Evidence Automation
**Paths:** `scripts/reconcile.py`, `scripts/collect_evidence.ps1`
**Reconciliation stages:** (1) source rows read → (2) valid → (3) rejected → (4) published to Kafka → (5) consumed by Spark → (6) written to Parquet → (7) inserted/merged into PostgreSQL → (8) unique analytics-ready records.
Output table: `Stage | Expected | Actual | Difference | Explanation`. Expected values are *computed* from source + rules, never guessed. Save to `docs/evidence/…/reconciliation_<runid>.txt`.
**Evidence collector:** runs a fixed set of commands, writes timestamped outputs, **scrubs secrets** (regex for keys/passwords/tokens).
**DoD:** reconciliation script exit-code non-zero when unexplained mismatch.

---

## M11 — Documentation & Viva
See `06_DOCS_AND_EVIDENCE.md`. Update existing docs rather than duplicating. Keep language beginner-friendly but technically correct.
