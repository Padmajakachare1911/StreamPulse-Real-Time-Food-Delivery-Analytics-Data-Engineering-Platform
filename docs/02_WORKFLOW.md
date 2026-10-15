# 02 — Workflow

## 1. Operating loop (every task)
```
Inspect → Plan (1–3 lines) → Implement → Run → Capture evidence → Update tracker/register → Commit (if logical unit done)
```
Verify each phase before starting dependent work. Don't batch-build everything unverified.

## 2. Phase map and dependencies
```
P0 Audit ─► P1 Dataset ─┬─► P3 Kafka ingestion ─► P4 Spark ─► P5 DQ/Idempotency ─► P6 PostgreSQL ─► P7 Airflow
                        │                                                              │
P2 API selection ───────┘                                                              ▼
                                                                                P8 Dashboard ─► P9 Testing ─► P10 Demos ─► P11 Final audit
```
P2 (API) can run in parallel with P1/P3 once the schema is known. If P1 is blocked (no dataset), build the framework for P3–P8 against a *config-driven mapping* and mark data-dependent tasks `BLOCKED`.

---

## 3. Phases: goals, steps, exit gates

### Phase 0 — Audit and planning
**Steps**
1. `git status`, `git branch --show-current`, `git remote -v` (record output).
2. Inventory repo tree, `requirements.txt`, `docker-compose.yml`, `.env.example`, `.gitignore`, `sql/`, `tests/`, `airflow/`, docs.
3. Environment checks: `python --version`, `java -version`, `docker --version`, `docker compose version`, `pip freeze` (PySpark version), WSL status.
4. Docker/Kafka: container `streampulse-kafka` health, topic `food_orders` metadata (partitions, RF), produce 1 test message, consume it.
5. Spark: confirm PySpark 4.2.0 ↔ Kafka connector `spark-sql-kafka-0-10_2.13:4.2.0` ↔ Scala 2.13 compatibility. Run existing `order_stream.py` against Kafka; check whether Parquet is actually written.
6. Investigate `HADOOP_HOME`/`winutils.exe` warning: does it block Parquet/checkpoint writes? Record finding; fix only if it breaks something.
7. `.gitignore` audit: `.env`, `.venv`, logs, `data/raw/*`, `data/processed/*`, checkpoints, `__pycache__`, `*.parquet`, credentials.
8. Write `docs/baseline_audit.md` (what exists / works / unverified / broken / duplicated).
9. Create `docs/implementation_plan.md`, `docs/task_tracker.md`, `docs/evidence_register.md` from templates in `06_DOCS_AND_EVIDENCE.md`.
10. Determine: is the dataset present? has the professor specified an API? List exact missing inputs.

**Exit gate:** baseline audit + tracker + register exist; Kafka/Spark/winutils status known with evidence in `docs/evidence/00_environment/`.

---

### Phase 1 — Dataset onboarding and schema
**Steps**
1. Locate dataset under `data/raw/`. If absent → mark `BLOCKED`, document filename/location needed, continue with framework work.
2. Compute and record SHA-256 of the raw file (used for run provenance). Never modify the file.
3. Profile: format, rows, columns, dtypes, nulls, duplicates, invalid values, min/max, timestamp fields, ID-like columns, categorical cardinalities, sample rows.
4. Decide domain fit: true food-delivery orders vs related data; snapshot vs transactions vs simulated.
5. Record source URL, owner, license, attribution.
6. Build **mapping config** (`config/field_mapping.yaml`): source column → canonical field, type, required?, validation rule. Preserve original columns in `raw` payload.
7. Write `docs/data_source.md`, `docs/data_dictionary.md`, `docs/data_quality_report.md`.
8. Build profiling script `scripts/profile_dataset.py` that outputs a reproducible text/JSON report to `docs/evidence/01_dataset/`.

**Exit gate:** real row/column counts recorded; mapping config reviewed; DQ baseline report with evidence.

---

### Phase 2 — Real API selection and integration
**Steps**
1. Ask/check whether the professor named an API. If yes → use it.
2. If not: research 2–3 candidates relevant to the *actual schema* (e.g., geocoding/reverse-geocoding via OpenStreetMap Nominatim, weather by lat/long such as Open-Meteo, restaurant reference data if a free API exists). Evaluate: relevance to real columns, auth, quota, rate limits, ToS, licensing.
3. Present recommendation + rationale + role (additional source vs enrichment). **Do not silently pick an unrelated API.**
4. Implement `src/common/api_client.py` (or `src/api/`) per Module M3.
5. Make a **real request** from the machine; save sanitized request/response + HTTP status + timestamp to `docs/evidence/02_api/api_real_request.txt`.
6. Integrate output: cache table/file + provenance fields; enrich records or ingest as source.
7. Write `docs/api_integration.md`.

**Exit gate:** at least one real successful request recorded **and** its data processed/stored. If blocked, record the real failure; no faking.

---

### Phase 3 — Kafka ingestion
**Steps**
1. Verify compose starts Kafka reliably; add healthcheck; ensure topic auto-create or init script.
2. Implement replay producer (Module M2) with `--limit`, `--dry-run`, `--rate`, `--batch-size`, `--full`.
3. Choose partition key from *actual data* (e.g., an entity ID with real ordering needs); document trade-off.
4. Scripts for topic list/describe/consume N/partition distribution (`scripts/kafka_*.ps1`).
5. Count tracking → `data/processed/ingestion_runs/<run_id>.json`.
6. Run limited (e.g., 100) then full (~100,000) ingestion; save evidence.

**Exit gate:** published count == valid count (or differences explained); partition distribution **measured** and saved.

---

### Phase 4 — Spark Structured Streaming
**Steps**
1. Refactor `src/streaming/order_stream.py` (preserve working bits) per Module M4.
2. Parse envelope safely; route malformed to DLQ; durable checkpoint path from config.
3. `foreachBatch` for Parquet + PostgreSQL writes; log batch counts.
4. Run end-to-end on the limited batch; **read Parquet back** and compare counts.
5. Demonstrate stop/restart from checkpoint.

**Exit gate:** Kafka→Spark→Parquet verified with read-back evidence.

---

### Phase 5 — Data quality and idempotency
Implement rule engine (Module M5), DLQ, duplicate tracking, retry-vs-source-duplicate distinction. **Prove** behavior with an isolated test input (invalid record, duplicate record, re-run).
**Exit gate:** observable rejection output + proven idempotent re-run.

### Phase 6 — PostgreSQL warehouse
Schema, upsert, analytics views, quality + reconciliation SQL (Module M6). **Exit gate:** reconciliation query output saved; re-running ingestion doesn't change unique counts.

### Phase 7 — Airflow orchestration
Decide which tasks are finite and orchestrate only those (Module M7). **Exit gate:** a real DAG run (success) with logs; retry/failure demonstrated if practical.

### Phase 8 — Streamlit dashboard
Read from PostgreSQL/Parquet; truthful status checks (Module M8). **Exit gate:** screenshots of real data; service-down state displays correctly.

### Phase 9 — Automated testing
Run unit / integration / e2e suites (Module M9); save pytest output. **Exit gate:** all pass, or failures documented honestly.

### Phase 10 — Four manual demos
Write and *dry-run-test* every command in `05_DEMO_SPECS.md`; fill `docs/manual_demo_guide.md`, `docs/demo_cheatsheet.md`, `docs/test_cases.md`. Mark human parts `PENDING_HUMAN`.

### Phase 11 — Final audit
Reconcile counts across all layers, check provenance, secrets scan, docs completeness, `git status` clean, submission instructions, list of verified vs pending.

---

## 4. Phase report (output after every phase, concise)
```
Phase: <number and name>
Completed: <tasks actually completed>
Files changed: <exact paths>
Commands executed: <exact commands>
Verification: <actual results + evidence paths>
Tests: <passed / failed / not run>
Git: <commit hash, pushed? (verified by command output)>
Blocked items: <exact cause + required action>
Next phase: <what's next>
```

## 5. Blocked-task protocol
1. Mark `BLOCKED` in tracker. 2. Record exact error/missing prerequisite. 3. Save sanitized logs. 4. State root cause (or "under investigation"). 5. List precise unblock action. 6. List dependent tasks. 7. **Continue with independent tasks.** 8. Never silently skip. 9. Never substitute fake data/mock API. 10. Retry only after a meaningful corrective change.

## 6. Git protocol
- Before changes: `git status`, `git branch --show-current`, `git remote -v`.
- Commit logical units; check `git diff --staged` first; clear messages (e.g., `feat(producer): add dataset replay producer with dry-run`).
- Never commit: `.env`, `.venv`, checkpoints, large datasets, local DB files, caches, secrets.
- Push when credentials work; if push fails keep local commits and report exact error.
- Don't claim push success without actual command output.
- No `reset --hard`, force-push, or destructive cleanup without explicit user approval.
- Suggested commit sequence: audit docs → dataset profiling + mapping → API client → producer → Spark → DQ → warehouse → Airflow → dashboard → tests → demo docs → final audit.

## 7. Suggested commit/branch convention
Work on the current branch unless the user asks otherwise (record the branch name in Phase 0). Prefix commits: `feat:`, `fix:`, `docs:`, `test:`, `chore:`.
