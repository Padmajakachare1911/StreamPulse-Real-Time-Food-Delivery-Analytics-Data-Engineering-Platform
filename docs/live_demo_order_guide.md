# Live Demonstration: place an order in Streamlit and follow it through the pipeline

This is the end-to-end demonstration of the original requirement:
**a user places an order in Streamlit and visibly traces it through Kafka, Spark Structured Streaming, Parquet and PostgreSQL, with the processing status on the dashboard and Airflow demonstrated for orchestration.**

Every output below was produced by a real run on 2026-10-16 (evidence files in `docs/evidence/12_demo_orders/`). Order ids and offsets differ on every run; the shape of the output does not.

## 0. What a demo order is (and is not)

| | Historical Olist order | Demo order (placed in the form) |
|---|---|---|
| Created by | `dataset_producer.py` replaying the CSV files | the Streamlit page "Place a demo order" |
| `order_id` | 32 hex characters from the dataset | starts with `demo-` |
| `source_category` | `kafka_replayed` | `user_demo` |
| `ingest_run_id` | `full-run-01`, `airflow-...`, ... | starts with `demo-order-` |
| Same schema, Kafka topic, Spark job, Parquet, `core.orders`? | yes | **yes (nothing is bypassed)** |
| In the 11 historical KPI views? | yes | **no** (`analytics.historical_orders` excludes `user_demo`) |
| In `scripts/reconcile.py`? | expected values come from the CSV source | listed as **explained** differences from the demo submission log |

The Olist data is never modified. Demo orders are marked in every layer, so they can never be mistaken for dataset records. No personal data is collected: the customer id is a random `demo-customer-...` value.

## 1. Preparation (T1 to T4 = four PowerShell terminals, repository root)

```powershell
$py = ".\.venv\Scripts\python.exe"
.\scripts\kafka_start.ps1          # T1  expected: Kafka health: healthy
.\scripts\init_db.ps1              # T1  expected: schema OK  (also re-applies the views; repeatable)
.\scripts\run_stream.ps1           # T2  Spark, a micro-batch every 5 s. Leave it open.
.\scripts\run_dashboard.ps1        # T3  expected: You can now view your Streamlit app ... http://localhost:8501
```
Open http://localhost:8501 and choose **Place a demo order** in the sidebar.

## 2. Place the order (Streamlit)

Fill the form (state, city, zip prefix, number of items, price, freight, payment type, status) and press **Place demo order**.

Expected on the page, immediately (screenshot `docs/evidence/12_demo_orders/ui_run1_02_submitted.png`):

```
Published to Kafka: partition 2, offset 33364. Order id: demo-dd14ee722ecf446ea4c3473fcc189671
```

What happened: the page called `submit_demo_order()` in `src/demo/demo_order.py`, which (1) checks the form, (2) builds one row in the same shape as the Olist rows, (3) validates it with the real quality rules (`classify_record`), (4) asks the real Frankfurter API for the BRL to USD rate, (5) wraps it in the standard envelope with `source_category = user_demo`, (6) publishes it with the same producer settings (`acks=all`, idempotence, gzip, key = `order_id`), (7) logs partition and offset in `data/processed/demo_orders/submissions.jsonl`.

## 3. Watch the trace (same page, "Trace an order")

The page re-checks every 3 seconds. Each line is a **real read of that layer**; a stage is ticked only when the order was found there (screenshot `ui_run1_03_trace_complete.png`):

```
✅ 1 Submitted: published to topic 'ecommerce_orders' at partition 2, offset 33364
✅ 2 Kafka: message read back from partition 2, offset 33364 (key demo-dd14ee722..., source_category user_demo)
✅ 3 Spark and Parquet: 1 Parquet row(s), batch_id 15; Spark batch 15 read 1 message(s), wrote 1 row(s) in 4.68 s
✅ 4 PostgreSQL: stored: status approved, source_category user_demo, 1 row for this order
Complete: the same order was found in Kafka, Spark/Parquet and PostgreSQL. Submit to PostgreSQL took 6.5 s.
```

| Stage | How it is checked | What it proves |
|---|---|---|
| 1 Submitted | the submission log | the dashboard published it; gives partition and offset |
| 2 Kafka | a consumer seeks to that partition/offset and compares `order_id` and `record_uid` | the broker really holds this message |
| 3 Spark and Parquet | a Parquet filter on `order_id` gives the row and its `batch_id`; the matching `BATCH_SUMMARY` line comes from `data/processed/spark_batches/orders.jsonl` | Spark consumed it, in which micro-batch, and how many rows that batch handled |
| 4 PostgreSQL | `SELECT ... FROM core.orders WHERE order_id = ...` | the warehouse row exists, with the same Kafka partition/offset |

The "Technical details" expander shows the facts of each stage as JSON. The table "Demo orders stored in PostgreSQL" at the bottom lists the stored demo orders.

If Spark is **not** running, stages 3 and 4 stay `⏳` with the text *"not in Parquet yet: Spark has not processed it. Is the stream running?"* (screenshot `airflow_a_spark_down_03_trace_TIMEOUT.png`). The page never shows a stage as done unless it was found.

## 4. Verify independently (do not trust the page alone)

```powershell
$id = "demo-dd14ee722ecf446ea4c3473fcc189671"        # the order id from the page
# Terminal view of the same trace (exit code 0 only when all four stages are done)
$py scripts\trace_order.py $id --verbose
# Kafka itself (the broker's own tool): read that partition/offset
docker exec streampulse-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic ecommerce_orders --partition 2 --offset 33364 --max-messages 1 --timeout-ms 15000
# Parquet files
$py scripts\parquet_readback.py --order-id $id
# PostgreSQL
docker exec streampulse-postgres psql -U streampulse -d streampulse -x -c "select order_id, source_category, order_status, kafka_partition, kafka_offset, payments_value_usd from core.orders where order_id='$id'"
```
Expected: Kafka prints one JSON envelope with `"source_category": "user_demo"` and the same `order_id`; Parquet prints one row with `batch_id 15`; PostgreSQL prints one row with `source_category = user_demo`, `kafka_partition = 2`, `kafka_offset = 33364`. Files: `independent_check_*.txt`.

## 5. Duplicate handling (the "Send the identical message again" button)

Press the button under the trace. The page says the identical message was sent again (offset 33365). Within seconds:

```
Spark batch 16:  input_rows 1  postgres {"attempted": 1, "inserted": 0, "retry_duplicates": 1}
PostgreSQL: still ONE row for the order
Parquet:    TWO rows (batch_id 15 and 16)  <- the landing layer is append-only by design
```
This is the at-least-once delivery + idempotent sink design shown on one real order (same `record_uid`).

## 6. Airflow orchestration

Airflow runs the finite steps and its Spark task uses `availableNow`, so it processes whatever is waiting in Kafka, **including demo orders**. Demonstration:

1. Stop the continuous stream (it and the DAG's Spark task must never share one checkpoint): `.\scripts\stop_stream.ps1` (waits for the running batch), and check that T2 ended.
2. Place two demo orders in Streamlit. Their trace shows stages 1-2 done and 3-4 waiting.
3. Run the DAG:
```powershell
.\scripts\airflow_up.ps1                                                       # expected: Airflow is healthy: metadatabase=healthy scheduler=healthy
.\scripts\airflow_run_dag.ps1 -Dag streampulse_batch -Conf '{"limit": 1}'      # about 2 minutes; expected: FINAL STATE: success, 9 tasks success
```
4. Refresh the trace: both orders are now complete. In the real run `run_spark_available_now` logged one batch of 3 messages (2 demo orders + 1 historical repeat from `limit: 1`): `inserted 2, retry_duplicates 1`. `reconcile_counts` and `run_quality_checks_sql` succeeded inside the container. Evidence: `airflow_dag_run_processes_waiting_demo_orders.txt`, `airflow_streampulse_batch_grid.png`, `airflow_c_after_dag_order1_trace.png`.
5. Restart the stream: `.\scripts\run_stream.ps1`.

`airflow_run_dag.ps1` now refuses to start (exit code 3, with the fix) if the scheduler is not healthy. In the real run the scheduler had stopped silently earlier and a run stayed `queued` for 10 minutes until the container was restarted (`docker compose --profile airflow restart airflow`); that history is kept in the evidence file.

## 7. Prove the numbers still add up

```powershell
$py scripts\reconcile.py            # expected last line: RESULT: all differences are zero or explained
$py scripts\run_quality_checks.py   # expected: RESULT: all quality checks passed
```
Demo messages appear as explicit explained differences, for example:
```
4 messages in Kafka topic     100,514   100,714   +200  [EXPLAINED] ... +1 user-submitted demo message(s); +199 message(s) of run id 'demo1', executed again ...
8c demo orders in PostgreSQL (user_demo)      1   1   +0  [OK]
```
The historical numbers do not move: `analytics.revenue_summary` still reports 99,427 orders and 16,006,745 BRL.

## 8. If something goes wrong

| Symptom | Cause | Fix |
|---|---|---|
| "The order was not published: Kafka did not accept the message" | Kafka is not running | `.\scripts\kafka_start.ps1` |
| Stage 3 stays ⏳ | the Spark stream is not running | `.\scripts\run_stream.ps1` (or run the Airflow DAG) |
| Stage 3 stays ⏳ although Spark runs | Spark is busy with a large backlog | wait; the trace updates itself |
| "rejected by the quality rules" | the form values break a rule (the page shows which) | change the values |
| Exchange-rate API unreachable | no internet | the order still flows, without USD values (`api_enriched = false`), like the historical replay |
| DAG run stays `queued` | Airflow scheduler stopped | `airflow_run_dag.ps1` now stops with exit code 3; restart: `docker compose --profile airflow restart airflow` |

## 9. Automated tests of this feature

| Test | What it covers | Result |
|---|---|---|
| `tests/unit/test_demo_order.py` (25) | order building, marking, identity, validation, historical rules unchanged, publish/resend with a fake producer, trace decisions, reconcile accounting | see `docs/evidence/10_automated_tests/demo_ext_unit.txt` |
| `tests/unit/test_run_id_reuse.py` (4) | a reused run id never overwrites a summary; reconcile detects the excess | same file |
| `tests/integration/test_dashboard_queries.py` | demo orders listed separately, excluded from historical KPIs, `source` column | `demo_ext_integration.txt` |
| `tests/e2e` `test_user_submitted_demo_orders_flow_...` | real Kafka, Spark in Docker, Parquet, PostgreSQL, real API, real reconcile; 19 checks | `demo_ext_e2e.txt`, `12_demo_orders/e2e_experiment_demo_orders_isolated.txt` |

## 10. Honest limits of this feature

- The form creates **demo orders**, not customer orders; there is no login, cart or payment. The data model is the existing Olist order schema (no product line items: item count and totals only).
- Demo orders live in the same warehouse table as the historical data (that is the point), so `core.orders` has one more row per demo order; only the historical KPI views exclude them.
- The dashboard does not start or stop Spark; it reports honestly whether the order has been processed.
- Latency (about 5 to 10 s in the live run) is dominated by Spark's 5-second trigger and batch start-up, not by Kafka.
- A demo order is stamped with the machine's current time; the exchange rate uses the previous business day when the ECB has not yet published today's rate (`fx_match_type = previous_business_day`).
