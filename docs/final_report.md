# StreamPulse: Final Report (2026-10-16)

**Real-Time Order Delivery Analytics & Data Engineering Platform (Olist e-commerce dataset)**
Every statement below points to a saved file under `docs/evidence/` (registered in `docs/evidence_register.md`). Nothing was simulated: the dataset, the Kafka broker, Spark, PostgreSQL, the exchange-rate API, Airflow and the dashboard are real and were executed.

## 1. Result in one paragraph
The pipeline `Olist CSV → Python replay producer (join, validate, enrich) → Kafka → Spark Structured Streaming (Docker) → Parquet + PostgreSQL → Streamlit`, with a real external API and Airflow orchestration, works end to end. The full real dataset (99,441 orders) was replayed: 99,427 valid orders are in the warehouse, 14 contradictory orders are preserved in the dead-letter queue, and every layer reconciles. It is a **historical dataset replay through a streaming pipeline, not live orders**, with **at-least-once delivery and an idempotent sink**, not exactly-once.

## 2. Phase status
| Phase | Status | Key evidence |
|---|---|---|
| 0 Audit | VERIFIED_WITH_EVIDENCE | `00_environment/`; winutils finding: host Spark cannot start, Spark runs in Docker |
| 1 Dataset | VERIFIED_WITH_EVIDENCE | `01_dataset/` (9 files hashed, profile, join check, license excerpt) |
| 2 Real API | VERIFIED_WITH_EVIDENCE | `02_api/` (HTTP 200, 544 rates, retry/backoff against an unreachable host) |
| 3 Kafka ingestion | VERIFIED_WITH_EVIDENCE | `03_kafka/` (99,441 read = 99,427 valid + 14 rejected; measured 33,199/33,255/32,973 per partition) |
| 4 Spark | VERIFIED_WITH_EVIDENCE | `04_spark/` (read-back, mid-backlog recovery 18/18, malformed messages 7/7, safe-stop stress 3×18/18) |
| 5 Data quality | VERIFIED_WITH_EVIDENCE | `05_data_quality/` (invalid row, source duplicate, idempotent re-ingestion 15/15) |
| 6 Warehouse + reconciliation | VERIFIED_WITH_EVIDENCE | `06_parquet/`, `07_postgresql/` |
| 7 Airflow | VERIFIED_WITH_EVIDENCE | `08_airflow/` (real DAG run SUCCESS 9/9; first run FAILED and kept; retry/failure demo) |
| 8 Dashboard | VERIFIED_WITH_EVIDENCE | `09_dashboard/` (real screenshots, real outage states, 19 tests) |
| 9 Automated tests | VERIFIED_WITH_EVIDENCE | `10_automated_tests/` |
| 10 Demos | VERIFIED_WITH_EVIDENCE for the preparation (dry runs); **PENDING_HUMAN** for the live demonstrations | `11_manual_demos/dry_runs/` |
| 11 Final audit | VERIFIED_WITH_EVIDENCE | `10_automated_tests/final_*.txt` |

## 3. Final test results (after the last code change; `docs/evidence/10_automated_tests/final_*.txt`)
| Suite | Command | Result |
|---|---|---|
| Unit | `pytest -m unit` | **73 passed, 1 skipped** (the skip: Airflow DAG module needs the Airflow container; reason printed) |
| Integration (real services) | `pytest -m integration` | **18 passed** (live API call, real PostgreSQL, real Kafka, stored API provenance) |
| Spark (in Docker) | `scripts\run_tests_docker.ps1` | **5 passed** |
| Airflow DAG (in container) | `scripts\run_tests_airflow.ps1` | **8 passed** |
| End-to-end (real pipeline, isolated) | `pytest -m e2e` | **4 passed** (3.5 min) |
| Reconciliation | `scripts\reconcile.py` | all differences zero or explained |
| Secret scan | `scripts\secret_scan.py` | clean |
| Evidence paths | `scripts\check_evidence_paths.py` | every cited path exists |
| Fresh environment | new venv from `requirements.txt` | pip exit 0, imports OK, 73 passed |
Mocks are never counted as proof: the API client's mocked tests check logic only; the live API is exercised by an integration test that ran (not skipped).

## 4. Key numbers (all measured)
| Item | Value |
|---|---|
| Orders in the real file / valid / rejected | 99,441 / 99,427 / 14 (8 "delivered" without delivery date; 6 with a delivery date but status canceled) |
| Kafka partition spread (key `order_id`) | 33.4 % / 33.4 % / 33.2 % (a `customer_state` key would have been 66.9 % in one partition) |
| Warehouse | 99,427 rows = distinct `record_uid` = distinct `order_id` |
| Parquet landing layer | 100,120 rows, 99,427 distinct (repeats from re-ingestion are kept there by design) |
| Revenue (payments_value_total) | 16,006,744.62 BRL (estimated 4,768,687.37 USD), late deliveries 8.11 % |
| API match rate | 100 % (24,112 orders matched on the previous business day: weekends) |

## 5. Failures found and fixed (kept as history, not hidden)
| What | Cause | Fix |
|---|---|---|
| Host Spark would not start | `winutils.exe` missing on Windows | Spark runs in Docker |
| Malformed JSON mislabelled | `from_json` returns a struct of nulls | JSON validity checked separately (test unchanged) |
| Geolocation loader crash | numpy float not accepted by psycopg2 | explicit casts |
| First Airflow run FAILED | checkpoint written as root, Airflow ran as another user | Airflow container runs as root |
| Dashboard showed Kafka down while it ran | unsupported client option; my first "down" tests passed for the wrong reason | option removed; positive real-broker test |
| Dashboard charts plotted NUMERIC as text | `Decimal` values | converted to float |
| **Dashboard printed Streamlit's own documentation on 3 pages** | a conditional expression as a bare statement (found only by viewing a screenshot) | if/else helper + AST guard test; all pages recaptured and viewed |
| **Intermittent e2e failure** (recovery test) | the graceful stop raced with a batch that started at that instant (`InterruptedException`) | stop redesigned: batches starting after a stop request raise `StopRequested` before doing any work; verified by 3 consecutive 18/18 runs and the final e2e |
| Fresh-venv test failed twice | very deep folder: Windows path-length limit (not a requirements problem) | proven at a short path; troubleshooting entry |
| 14 vs 15 rejected orders | overlapping runs: one record was rejected by both | run-level and source-level accounting separated (expected number not adjusted) |

## 6. Commits on `main` (all pushed)
`c3fc6ee` audit · `802c4a4` dataset · `a9f479d` API · `aa395f5` producer · `4c17922` Spark/warehouse code · `792df69` read-back · `6448549` recovery + dead-letter proofs · `d4ca4e3` data quality · `db53e65` reconciliation + loaders · `fe64df1` Airflow + dashboard · `6b89d38` safe stop, e2e, demos · final audit commits (see `git log`). Hashes per task: `docs/task_tracker.md`.

## 7. Blocked
**Nothing is blocked.** No external dependency prevented any requirement.

## 8. What only the human can do (PENDING_HUMAN)
1. Run the **four manual demos** in front of the professor and fill in the "ACTUAL observed output" fields (`docs/manual_demo_guide.md`, `docs/demo_cheatsheet.md`; every command was dry-run tested).
2. Capture the demo evidence files and screenshots (`docs/evidence/11_manual_demos/*/README.md` list exactly what and how).
3. **Confirm the retitling** with the professor (the dataset is e-commerce, not food delivery).
4. Re-confirm the **dataset license** on the Kaggle page (CC BY-NC-SA 4.0 was read from a search-result excerpt).
5. Ask whether the professor expected a **specific API** (none was named; Frankfurter was chosen, `docs/api_integration.md`).
6. On a new machine: place the Kaggle files in `data/raw/` and create `.env` (`docs/setup_guide.md`).

## 9. Honest limitations
Single-node Kafka/PostgreSQL/Spark; historical replay, not live data; the API supplies reference data, not orders; USD values are estimates; the Airflow container runs as root (trade-off documented); timestamps have no time zone. Full list: `docs/known_limitations.md`.
