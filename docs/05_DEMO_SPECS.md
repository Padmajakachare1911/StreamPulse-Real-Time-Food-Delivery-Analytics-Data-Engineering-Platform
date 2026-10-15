# 05 — The Four Manual Demonstrations (Spec for Agent)

The student will personally run these in front of the professor. The agent must **prepare, dry-run, and document** them — never replace them with automated tests or claim they were performed.

Outputs the agent must produce:
- `docs/manual_demo_guide.md` (full narrative per demo)
- `docs/demo_cheatsheet.md` (copy-paste PowerShell, terminal by terminal)
- `docs/test_cases.md` (IDs, inputs, expected vs actual, pass/fail)
- `docs/evidence/11_manual_demos/demo_{1..4}_*/` (folders + placeholders marked `PENDING_HUMAN`)

## Required template per demo (all fields mandatory)
```
### DEMO N — <title>
Purpose:
What it proves:
What it does NOT prove:
Prerequisites:
Terminal assignments: (Terminal 1 = …, Terminal 2 = …, Browser = …; which stay open)
Working directory for each command:
Commands (PowerShell, copy-paste):
Controlled sample input (file path + contents):
What to observe:
Expected output (labelled EXPECTED — not observed):
ACTUAL observed output: ______ (student fills in)
Verification commands (independent check):
SQL queries:
Screenshot/log filenames to save:
Pass / fail criteria:
Common errors + fixes:
Safe cleanup / reset steps:
30-second explanation to say to the professor:
```
Every command must be **tested by the agent** before being labelled verified. Untested ⇒ label `UNTESTED`.

## Terminal conventions (use consistently)
| Terminal | Role | Stays open? |
|---|---|---|
| T1 | Infrastructure (docker compose up/logs) | yes |
| T2 | Spark streaming job | yes during run |
| T3 | Producer / API / scripts / verification | no |
| T4 | Airflow / dashboard (as needed) | yes |
| Browser | Streamlit, Airflow UI | — |

---

## DEMO 1 — Kaggle Dataset Ingestion and End-to-End Flow
**Goal:** actual dataset records travel Kafka → Spark → Parquet → PostgreSQL → dashboard.
**Steps to document**
1. Show dataset path/filename (`data/raw/…`), actual row & column counts (`scripts/profile_dataset.py --summary`), schema, 5 sample rows.
2. Start services (compose), verify Kafka + topic: `kafka_describe_topic.ps1`.
3. Start Spark stream (T2).
4. Run **limited** ingestion (e.g., `--limit 200`) in T3 → show published counts.
5. Show partition offsets / distribution (`kafka_partition_counts.ps1`) — measured.
6. Show Spark batch logs; list Parquet files; run `scripts/parquet_readback.py` for count + sample.
7. Run PostgreSQL queries: total, distinct `record_uid`, sample rows.
8. Open Streamlit; show numbers match.
9. Show **count-reconciliation table** (`scripts/reconcile.py`): `Stage | Expected | Actual | Difference | Explanation` — expected computed from source + rules.
10. Provide separate evidence of the **full ~100,000 run** (pre-captured log + reconciliation), not run live unless time allows.
**Pass:** reconciliation differences are zero or fully explained; dashboard counts == SQL counts.
**Framing line:** "This is a historical dataset replayed through a streaming pipeline, not live orders."

## DEMO 2 — Real API Integration
**Goal:** prove a genuine request to the external API and use of the response.
**Steps to document**
1. Name provider, official docs URL, endpoint, parameters, auth method (key never displayed — show `.env.example` and say key is in `.env`).
2. Run `python scripts/api_demo_call.py --sample` (real request) → prints timestamp, URL (secrets masked), HTTP status, sanitized response excerpt.
3. Show the mapping/enrichment step output (`src/api/enrich.py` on a few real keys).
4. Show persisted result: SQL on `core.api_cache` / enriched table with provenance columns.
5. Show dashboard API section (last successful call timestamp, enriched count, match rate).
6. (Optional safe failure) run with an invalid host/timeouts flag `--simulate-failure` to show retry/backoff logging — clearly labelled as simulated failure of a *real* client, not a mock API.
**Pass:** HTTP 2xx observed live, record persisted with provenance; no secrets visible.
**Does not prove:** that the API contains food-delivery orders (public APIs don't) — it provides reference/enrichment data.

## DEMO 3 — Invalid Data, Data Quality, Duplicate Handling
**Goal:** bad and repeated records are handled predictably.
**Steps to document**
1. Show rules (`config/quality_rules.yaml`, `docs/data_quality_rules.md`) and the real schema.
2. Prepare an **isolated test file** `data/test_inputs/demo3_records.csv` in the *real schema*: 1 valid row (dedicated test IDs), 1 invalid row (specific, named violation, e.g., required field blank or non-numeric in a numeric field — chosen from real columns), 1 exact duplicate of the valid row. Original Kaggle file untouched.
3. Run ingestion on that file with a distinct `--run-id demo3`.
4. Show rejected output: DLQ JSONL + `core.dead_letter_records` row with `reason_code`.
5. Show duplicate handling: SQL proving only one row exists for the valid `record_uid`; counter `retry_duplicate`/`source_duplicate`.
6. **Re-run the same file** → show unique count unchanged (idempotency) — and explain the distinction between retry (same `record_uid`) and source duplicate (same `business_hash`, different `record_uid`).
7. Dashboard quality section reflects the counts.
**Cleanup:** `scripts/demo3_cleanup.ps1` deletes only rows with demo test IDs (documented, parameterized, no blanket deletes).
**Pass:** observed rejection with reason; unique count stable after re-run.

## DEMO 4 — Recovery, Checkpoints, Airflow, Reconciliation
**Goal:** recovery behavior and orchestration work as implemented.
**Steps to document**
1. Show current counts (SQL + Parquet) and the checkpoint path/listing.
2. Start Spark stream; ingest more records (limited batch A).
3. **Controlled stop** of Spark (`stop_stream.ps1`) mid-way; publish batch B to Kafka while stream is down.
4. **Restart** stream with same checkpoint → show it resumes from stored offsets and processes only batch B (log lines show offsets).
5. Verify no duplication: SQL distinct vs total; reconciliation.
6. Trigger Airflow DAG (UI or `airflow dags trigger`); show task graph, logs, success.
7. (If practical) show a task retry using a deliberately unavailable dependency.
8. Final reconciliation + dashboard refresh.
**Explain limits:** checkpoint = Spark's progress record; Kafka offsets = broker positions; DB upsert = duplicate protection. Together at-least-once + idempotent sink. **Not** claimed as exactly-once everywhere.
**Do not** delete checkpoints/data except via documented safe reset (`scripts/reset_demo_state.ps1`, with confirmation prompt, scope limited to demo outputs).

---

## Evidence Placeholders (create now, mark `PENDING_HUMAN`)
```
docs/evidence/11_manual_demos/
  demo_1_dataset_ingestion/  (demo_1_full_pipeline.png, dataset_row_count.txt, reconciliation.txt)
  demo_2_real_api/           (api_real_request.txt, demo_2_api_sql.png)
  demo_3_data_quality/       (rejected_record.txt, duplicate_proof.txt, demo_3_dashboard.png)
  demo_4_recovery/           (restart_log.txt, airflow_dag_success.png, final_reconciliation.txt)
```
Each placeholder: a `README.md` listing exactly what the student must capture and the command that produces it.

## Dry-run checklist before declaring demos "ready"
- [ ] Each command run from a clean PowerShell on Windows
- [ ] Working directory stated for each command
- [ ] Services started in documented order; shutdown order documented
- [ ] Expected output captured from a real dry-run (labelled as dry-run result)
- [ ] Troubleshooting entries based on errors actually encountered
- [ ] Time budget per demo noted (suggest ≤ 5–7 min each)
