# Manual Demonstration Guide (the student runs these live)

**Status of this document:** the commands were dry-run by the agent (results in `docs/evidence/11_manual_demos/dry_runs/`); the live demonstrations in front of the professor are **PENDING_HUMAN**. "EXPECTED" text below comes from those dry runs; "ACTUAL observed output" is left for you to fill in during the real demo.

**Framing line for every demo:** *"This is a historical dataset replayed through a streaming pipeline, not live orders."*

All commands are PowerShell, run from the repository root:
`C:\Users\Padmaja Kachare\Documents\StreamPulse-Real-Time-Food-Delivery-Analytics-Data-Engineering-Platform`
Shorthand used below: `$py = ".\.venv\Scripts\python.exe"` (define it once in every terminal).

## Terminals
| Terminal | Role | Stays open? |
|---|---|---|
| T1 | Infrastructure (Docker, Kafka, PostgreSQL) | yes |
| T2 | Spark streaming job | yes while it runs |
| T3 | Producer, API calls, SQL, verification | no |
| T4 | Dashboard / Airflow | yes |
| Browser | http://localhost:8501 (dashboard), http://localhost:8080 (Airflow) | – |

## Before you start (once, ~2 minutes)
```powershell
# T1
$py = ".\.venv\Scripts\python.exe"
.\scripts\kafka_start.ps1                 # success: "Kafka health: healthy"
.\scripts\init_db.ps1                     # success: "schema OK"
$py scripts\check_dataset_fingerprint.py  # success: "All 9 files match the fingerprints..."
```
Services use ports 9092, 5432, 8501, 8080. Total time budget per demo: 5–7 minutes.

---
## DEMO 1 — Real dataset ingestion and end-to-end flow
**Purpose:** show that actual dataset records travel Kafka → Spark → Parquet → PostgreSQL → dashboard.
**What it proves:** the pipeline moves real Olist orders through every layer and the counts reconcile.
**What it does NOT prove:** that the data is live (it is historical) or that the system is production-ready (single node).
**Prerequisites:** "Before you start" done; Docker running; Spark image built (`docker compose --profile spark build spark`).
**Terminals:** T1 infrastructure (already up) · T2 Spark · T3 commands · Browser dashboard.

**Steps (all from the repository root):**
1. T3 — dataset and counts:
   ```powershell
   $py scripts\profile_dataset.py --summary            # 9 files; orders 99,441 rows x 8 columns
   Get-Content data\raw\olist_orders_dataset.csv -TotalCount 6      # header + 5 sample rows
   ```
2. T3 — Kafka topic: `.\scripts\kafka_describe_topic.ps1` (3 partitions)
3. T2 — start Spark: `.\scripts\run_stream.ps1` (leave it running)
4. T3 — limited ingestion: `$py -m src.producer.dataset_producer --limit 200 --run-id demo1`
5. T3 — measured partition distribution: `.\scripts\kafka_partition_counts.ps1`
6. T2/T3 — Spark batch log lines `BATCH_SUMMARY`; then `$py scripts\parquet_readback.py`
7. T3 — PostgreSQL:
   ```powershell
   docker exec streampulse-postgres psql -U streampulse -d streampulse -c "SELECT count(*), count(DISTINCT record_uid) FROM core.orders"
   docker exec streampulse-postgres psql -U streampulse -d streampulse -c "SELECT order_id, order_status, purchase_ts, customer_state, payments_value_total, payments_value_usd FROM core.orders ORDER BY processed_at DESC LIMIT 5"
   ```
8. T4 — `.\scripts\run_dashboard.ps1`, browser → http://localhost:8501, pages "Overview and status", "Ingestion and reconciliation", "Business KPIs"
9. T3 — reconciliation: `$py scripts\reconcile.py`
10. The full ~100,000 run is shown from pre-captured evidence: `docs/evidence/03_kafka/producer_full_run.txt` and `docs/evidence/07_postgresql/reconciliation_production_final.txt` (do not re-run live unless time allows).

**EXPECTED (from the dry run, labelled EXPECTED — not observed):** step 4 reads 200 = 199 valid + 1 rejected and publishes 199; PostgreSQL stays at 99,427 unique orders because these orders already exist (they are counted as `retry_duplicates`); `reconcile.py` ends with `RESULT: all differences are zero or explained`.
**ACTUAL observed output:** ______ (student fills in)
**Verification (independent):** the dashboard "Ingestion and reconciliation" table equals the SQL counts.
**SQL:** see step 7; also `sql\quality_checks.sql` (all `problem_rows` = 0).
**Save:** `demo_1_full_pipeline.png` (browser), `dataset_row_count.txt`, `reconciliation.txt` into `docs/evidence/11_manual_demos/demo_1_dataset_ingestion/`.
**Pass:** reconciliation differences are zero or explained; dashboard counts equal SQL counts.
**Common errors:** *Kafka not reachable* → `.\scripts\kafka_start.ps1`; *Spark exits immediately* → Docker not running; *`POSTGRES_PASSWORD` error* → fill `.env`.
**Cleanup:** Ctrl+C in T2 (or `.\scripts\stop_stream.ps1`). Nothing to delete: the replay is idempotent.
**30-second explanation:** "We replay 100,000 real historical orders into Kafka at a controlled speed. Spark reads them in micro-batches, validates them, appends them to Parquet and upserts them into PostgreSQL. The dashboard and the reconciliation script show that every layer has the same unique orders. It is a historical replay, not live data."

---
## DEMO 2 — Real API integration
**Purpose:** prove a genuine request to the external API and the use of its response.
**What it proves:** a real HTTP 200 from Frankfurter (ECB rates), stored with provenance and used to give each order a USD value.
**What it does NOT prove:** that the API contains orders (public APIs do not); it provides reference data.
**Prerequisites:** internet access; `.env.example` shows there is no API key (the provider needs none).
**Terminals:** T3 + Browser.
**Commands:**
```powershell
$py scripts\api_demo_call.py --sample --no-cache            # real request: timestamp, URL, HTTP status, excerpt
$py scripts\api_demo_call.py --simulate-failure             # OPTIONAL: real client against an unreachable host: retry/backoff 1 s, 2 s
# The one-week sample above is only displayed. What is PERSISTED is the full-range response that the producer fetched
# (and cached) during ingestion; this loads those cached responses into PostgreSQL (safe to repeat):
$py -m src.warehouse.load_reference --only api
docker exec streampulse-postgres psql -U streampulse -d streampulse -c "SELECT api_source_name, http_status, retrieved_at FROM core.api_cache ORDER BY retrieved_at DESC LIMIT 3"
docker exec streampulse-postgres psql -U streampulse -d streampulse -c "SELECT order_id, purchase_ts, payments_value_total, fx_rate_brl_usd, fx_match_type, payments_value_usd, api_source_name, api_http_status FROM core.orders LIMIT 5"
docker exec streampulse-postgres psql -U streampulse -d streampulse -c "SELECT * FROM analytics.api_enrichment_summary"
```
Dashboard page "External API" (match rate, last retrieval, rate chart).
**EXPECTED:** `HTTP : 200`, 5 rows, a Saturday date resolved to the previous business day; the failure option ends with `API failed after 3 attempts: ConnectionError`; `match_rate_percent` 100.00.
**ACTUAL observed output:** ______
**Verification:** the same URL in a browser: `https://api.frankfurter.dev/v2/providers/ecb/rates?base=BRL&quotes=USD&from=2017-10-02&to=2017-10-08`.
**Save:** `api_real_request.txt`, `demo_2_api_sql.png` into `docs/evidence/11_manual_demos/demo_2_real_api/`.
**Pass:** HTTP 2xx observed live, rows persisted with provenance, no secrets visible.
**Common errors:** *no internet* → the real call fails honestly (do not fake it); *proxy* → run from another network.
**Cleanup:** none.
**30-second explanation:** "Each order has a BRL payment value and a purchase date. We ask the European Central Bank's rates (through Frankfurter, no key) once for the whole period, cache it, and attach the rate of the purchase day, so every order also has an estimated USD value. The API gives reference data, not orders."

---
## DEMO 3 — Invalid data, data quality and duplicate handling
**Purpose:** show that bad and repeated records are handled predictably.
**Runs in an ISOLATED environment** (own topic `demo3_orders`, database `streampulse_demo3`, folder `data\test_runs\demo3`): the real warehouse is not touched.
**Controlled input:** `data\test_inputs\demo3_records.csv` (real orders schema; real Kaggle files untouched):
- row 1 `demo3_order_valid_001` — valid
- row 2 `demo3_order_invalid_002` — delivered date (2017-09-01) **before** the purchase date (2017-10-03) → must be rejected
- row 3 — exact copy of row 1 → source duplicate
**Terminals:** T1 · T2 Spark · T3 commands · Browser.
**Commands:**
```powershell
Get-Content config\quality_rules.yaml                                   # 1. the rules
Get-Content data\test_inputs\demo3_records.csv                          #    the controlled input
.\scripts\demo_setup.ps1 -Demo 3                                        # isolated topic + database
$py -m src.producer.dataset_producer --input data\test_inputs\demo3_records.csv --topic demo3_orders --limit 10 --run-id demo3-run1
Get-Content data\dead_letter\ingest\demo3-run1.jsonl                    # 4. rejected record with reason_code
.\scripts\run_stream.ps1 -Demo 3 -Trigger availableNow                  # T2: Spark into the demo database
$q = "SELECT order_id, record_uid FROM core.orders; SELECT duplicate_type, record_uid, kept_record_uid FROM core.duplicate_records;"
docker exec streampulse-postgres psql -U streampulse -d streampulse_demo3 -c "SELECT count(*) AS orders FROM core.orders" -c "SELECT duplicate_type, count(*) FROM core.duplicate_records GROUP BY 1"
# 6. re-run the SAME file (idempotency)
$py -m src.producer.dataset_producer --input data\test_inputs\demo3_records.csv --topic demo3_orders --limit 10 --run-id demo3-run2
.\scripts\run_stream.ps1 -Demo 3 -Trigger availableNow
docker exec streampulse-postgres psql -U streampulse -d streampulse_demo3 -c "SELECT count(*) AS orders FROM core.orders"
$py scripts\parquet_readback.py --dir data\test_runs\demo3\parquet     # landing layer keeps repeats
.\scripts\demo_load.ps1 -Demo 3                                          # loads the demo's run summaries and dead letters into the demo database
.\scripts\run_dashboard.ps1 -Demo 3                                     # T4: "Data quality" page shows the demo database
```
**EXPECTED:** run 1: read 3, valid 2, rejected 1, published 2; dead letter `RANGE_ERROR` naming `delivered_not_before_purchase`; PostgreSQL 1 order and 1 `source_duplicate`. After the re-run: still 1 order; Parquet 4 rows (2 distinct); the second Spark batch reports `retry_duplicates: 2`.
**ACTUAL observed output:** ______
**Explain:** *retry duplicate* = same `record_uid` (same file and row, replayed); *source duplicate* = different `record_uid`, same `order_id`.
**Save:** `rejected_record.txt`, `duplicate_proof.txt`, `demo_3_dashboard.png` into `docs/evidence/11_manual_demos/demo_3_data_quality/`.
**Pass:** rejection with a reason is observed; the unique count is stable after the re-run.
**Common errors:** *topic already exists* is fine; *database does not exist* → run `demo_setup.ps1 -Demo 3` first.
**Cleanup (safe, scoped):** `.\scripts\demo3_cleanup.ps1` (asks for YES; removes only the demo topic/database/folders).
**30-second explanation:** "I feed three controlled rows in the real schema. The impossible one goes to the dead-letter queue with a reason. The exact copy is recognised as a source duplicate, only one row reaches the warehouse, and re-running the same file changes nothing."

---
## DEMO 4 — Recovery, checkpoints, Airflow and reconciliation
**Purpose:** show recovery behaviour and orchestration.
**Isolated environment** for the recovery part (topic `demo4_orders`, database `streampulse_demo4`); Airflow uses the real pipeline.
**Terminals:** T1 · T2 Spark · T3 commands · T4 Airflow · Browser.
**Commands (recovery):**
```powershell
.\scripts\demo_setup.ps1 -Demo 4
$py -m src.producer.dataset_producer --topic demo4_orders --skip 0 --limit 3000 --run-id demo4-A
.\scripts\run_stream.ps1 -Demo 4 -MaxOffsetsPerTrigger 500 -Trigger "3 seconds"     # T2: leave running ~15 s, then in T3:
.\scripts\stop_stream.ps1 -Demo 4                                                    # controlled stop (waits for the current batch)
Get-ChildItem data\test_runs\demo4\checkpoints\demo4\commits                         # which batches were committed
$py -m src.producer.dataset_producer --topic demo4_orders --skip 3000 --limit 1000 --run-id demo4-B   # while Spark is DOWN
$py -m src.producer.dataset_producer --topic demo4_orders --skip 0 --limit 100 --run-id demo4-dup      # repeats of processed records
.\scripts\run_stream.ps1 -Demo 4 -MaxOffsetsPerTrigger 500 -Trigger availableNow     # restart with the SAME checkpoint
docker exec streampulse-postgres psql -U streampulse -d streampulse_demo4 -c "SELECT count(*), count(DISTINCT record_uid) FROM core.orders"
$py scripts\reconcile.py --topic demo4_orders --stream demo4 --parquet-dir data\test_runs\demo4\parquet --database streampulse_demo4 --runs demo4-A demo4-B demo4-dup
```
**Commands (Airflow):**
```powershell
.\scripts\airflow_up.ps1                                  # T4; UI http://localhost:8080, user admin, password in .env (AIRFLOW_ADMIN_PASSWORD)
.\scripts\airflow_run_dag.ps1 -Dag streampulse_batch -Conf '{"limit": 300}'          # triggers, waits, prints every task state (or press Trigger DAG in the UI)
.\scripts\airflow_run_dag.ps1 -Dag streampulse_failure_demo -TimeoutMinutes 10       # OPTIONAL: retries with an unreachable Kafka; ends "failed" ON PURPOSE
.\scripts\airflow.ps1 dags pause streampulse_failure_demo                            # afterwards, so it never runs by accident
```
**EXPECTED:** after the stop, some batch ids are committed and the rest of the offsets are pending; the restart's first batch starts at the stored offset; PostgreSQL shows the number of unique orders (duplicates are not counted twice); reconcile prints `RESULT: all differences are zero or explained`; the DAG run ends `success` with all 9 tasks green (about 2 minutes in the dry run); the failure DAG shows 3 attempts, then `failed`.
**What you will see at the Spark stop (NORMAL, explain it):** `Stop requested`, possibly a few `ERROR` lines and a traceback ending in `StopRequested: stop requested: batch N was not started and will run after the restart`, then `STOPPED_AT_BATCH_BOUNDARY`. This is the deliberate safe stop: a batch about to start is refused *before* it does any work, so nothing is half-written, and it runs after the restart. A real problem would not contain `StopRequested`.
**ACTUAL observed output:** ______
**Explain the limits:** the checkpoint is Spark's progress record; Kafka offsets are broker positions; the database upsert is the duplicate protection. Together: **at-least-once delivery + idempotent sink**, **not** exactly-once everywhere.
**Save:** `restart_log.txt`, `airflow_dag_success.png`, `final_reconciliation.txt` into `docs/evidence/11_manual_demos/demo_4_recovery/`.
**Pass:** restart resumes from the stored offset without duplicates; the DAG run succeeds; reconciliation is clean.
**Common errors:** *Airflow login fails* → password is `AIRFLOW_ADMIN_PASSWORD` in `.env`; *DAG task "Permission denied"* → see `docs/setup_guide.md` troubleshooting.
**Cleanup (safe, scoped):** `.\scripts\reset_demo_state.ps1 -Demo 4`. **Never** delete `data\checkpoints\orders`.
**30-second explanation:** "I stop Spark in the middle of a backlog, publish more records and some repeats while it is down, and restart with the same checkpoint. It continues exactly where it stopped, processes only the new offsets, and the database ends with the right unique count. Airflow then runs the same finite steps as a scheduled workflow with retries."

---
## Dry-run checklist
Each command was run from a clean PowerShell on Windows; working directory stated above; start order T1 → T2 → T3 → T4/Browser; shutdown order: stop stream, `.\scripts\airflow_down.ps1`, close the dashboard, `docker compose stop`. Results and the errors met while testing: `docs/evidence/11_manual_demos/dry_runs/` and `docs/test_cases.md`.
