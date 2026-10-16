# Known Limitations, Open Items and Pending-Human Checklist

Written for the final audit. Nothing here is hidden elsewhere; each item says whether it is **resolved**, **accepted** (with the reason) or **pending (human)**.

## 1. Findings carried over from Phase 4 (kept, with their current status)
| Finding | Status |
|---|---|
| Parquet is an append-only landing layer, so its duplicate semantics differ from the deduplicated PostgreSQL layer | **Accepted by design**, documented in `docs/data_quality_rules.md` and shown live on the dashboard; reconciliation counts the difference (`7b retry duplicates`) |
| The graceful stop had a small race: `query.stop()` could interrupt a batch that started at the very instant of the stop | **Resolved.** Found again by an intermittent e2e failure (`10_automated_tests/pytest_e2e_20261016_FAILED_stop_race.txt`). Spark offers no public "stop between batches" call, so the stop was redesigned: a batch that starts after a stop request raises `StopRequested` before doing any work; a running batch finishes; `query.stop()` is used only when the stream is idle. 3 consecutive stress runs and the final e2e run passed (`04_spark/recovery_stop_stress_run*.txt`). Residual: Spark's own planning phase could still be interrupted, which does no work and is re-planned after restart (safe) |
| The limited-run reconciliation mixed overlapping runs; 14 vs 15 rejected orders | **Resolved.** Run-level and source-level accounting are separate; 15 = 14 unique + 1 record rejected again by an overlapping run (`07_postgresql/rejected_14_vs_15_explained.txt`) |
| PySpark 4.2.0 warns that pandas ≥ 3.0 is not fully supported | **Accepted, verified.** PySpark's metadata declares `pandas>=2.2.0` (no upper bound); the warning is about pandas-conversion APIs, which the project never uses; every Spark run, test and DAG worked on pandas 3.0.6. No change made |
| First recovery attempt (stop during a batch) | **Kept as history**: `04_spark/recovery_test_attempt1_stop_during_batch.txt` |
| Production checkpoint `data/checkpoints/orders` | **Preserved.** Only advanced by normal processing (re-ingestion and Airflow runs); never reset |

## 2. Architecture limits (accepted for a coursework demonstration)
- Single Kafka broker, replication factor 1; single PostgreSQL instance; Spark in local mode inside one container.
- Historical **replay**, not a live source. The API supplies reference data (exchange rates), not orders.
- JSON messages without a schema registry; no monitoring/alerting stack.
- Delivery guarantee is **at-least-once with an idempotent sink**, not exactly-once.
- Timestamps in the dataset have no time zone and are treated as local Brazilian time.
- USD values are estimates (BRL × ECB reference rate of the purchase day; weekends use the previous business day, 24,112 orders).

## 3. Operational notes
- The Airflow container runs as **root** so its tasks can write the Spark checkpoint and Parquet files that the Spark container creates as root (first DAG run failed without this). A real deployment would use one shared user or separate volumes.
- Kafka's default retention is 7 days: replayed messages disappear from the topic after that; replaying again is safe (idempotent).
- The dashboard's live reconciliation compares layer counts; the full source-derived reconciliation is `scripts/reconcile.py`.
- For limited runs, "rows read" in the reconciliation is taken from the run summary (the requested limit is not stored); valid/rejected are always re-derived from the source.
- `docs/inputs/api_info.md` does not exist: the professor named no API, so Frankfurter was chosen and justified (`docs/api_integration.md`).

## 4. Pending human actions (nothing below was performed by the agent)
| # | What | Why it is human | Where |
|---|---|---|---|
| 1 | Run the **four manual demos** live and fill in the "ACTUAL observed output" fields | the professor must see the student perform them | `docs/manual_demo_guide.md`, `docs/demo_cheatsheet.md` |
| 2 | Capture the demo evidence files / screenshots (all marked `PENDING_HUMAN`) | live capture in front of the professor | `docs/evidence/11_manual_demos/*/README.md` |
| 3 | **Confirm the project retitling** ("Real-Time Order Delivery Analytics … (Olist e-commerce dataset)" instead of "Food Delivery") with the professor | the dataset is e-commerce, not food delivery | `README.md`, `docs/implementation_plan.md` |
| 4 | Re-confirm the dataset license on the Kaggle page (CC BY-NC-SA 4.0 was read from a search-result excerpt, not a screenshot) | needs a browser check | `docs/evidence/01_dataset/license_check.txt` |
| 5 | Ask the professor whether a specific API was expected | none was named | `docs/api_integration.md` |
| 6 | Place the Kaggle files in `data/raw/` and create `.env` on any new machine | files and passwords are never committed | `docs/setup_guide.md` |

## 5. Added with the demo-order extension (2026-10-16)
| Item | Status |
|---|---|
| Demo orders are new, user-submitted test orders (no login, cart or payment; item count and totals only, no product lines). They share `core.orders` with the Olist data and are excluded only from the historical KPI views | **Accepted by design** (`docs/live_demo_order_guide.md` section 10) |
| The date rule `purchase_date_in_range` ends in 2018. Demo orders are validated with a demo-only copy of the rules whose window ends today; the historical rules are unchanged (unit test `test_historical_rules_are_unchanged...`) | **Accepted, tested** |
| The dashboard cannot start Spark; the trace reports "waiting" until the stream or the Airflow DAG has processed the order | **Accepted** (honest status) |
| **Run-id reuse hid 199 published messages.** `demo1` (Demo 1 command) was executed twice; the second execution overwrote the first run summary, so `reconcile.py` showed +199 unexplained messages (found when the demo order was reconciled). The producer now adds a suffix (`demo1-2`) instead of overwriting, and `reconcile.py` detects and explains run-id reuse from Parquet (`docs/evidence/12_demo_orders/reconcile_production_with_demo_orders.txt`). The lost first summary was not recreated | **Resolved for the future; past execution explained, not erased** |
| **The Airflow scheduler had stopped silently** (last heartbeat 2026-10-15 18:23 UTC, no error in its log); a DAG run stayed `queued`. Restarted with `docker compose --profile airflow restart airflow`. `airflow_run_dag.ps1` now fails fast (exit code 3) when the scheduler is not healthy. Root cause of the stop is unknown | **Mitigated, cause open** |
| Stopping the production stream for the Airflow demonstration was done with the project's safe stop and the stream was restarted afterwards | **Done, documented** |
