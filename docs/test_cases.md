# Test Cases

IDs, inputs, expected result, **actual result (from real runs)** and pass/fail. Evidence paths are under `docs/evidence/`.
Automated suites are in `tests/` (markers: `unit`, `integration`, `e2e`, `spark`, `airflow`). Manual demo cases (MD-*) are **PENDING_HUMAN**.

## A. Automated suites (FINAL full run on 2026-10-16, after the last code change: `docs/evidence/10_automated_tests/final_*.txt`)
| Suite | Command | Actual result | Evidence |
|---|---|---|---|
| Unit (host) | `pytest -m unit` | **73 passed, 1 skipped** (the skip is the Airflow DAG module, which needs the Airflow container; its 8 tests pass there) | `10_automated_tests/final_unit.txt` |
| Integration (real PostgreSQL, Kafka, live API) | `pytest -m integration` | **18 passed** (the 1 skip shown by pytest is the same Airflow module, not an integration test) | `10_automated_tests/final_integration.txt` |
| Spark (in Docker) | `scripts\run_tests_docker.ps1` | **5 passed** | `10_automated_tests/final_spark_docker.txt` |
| Airflow DAG (in container) | `scripts\run_tests_airflow.ps1` | **8 passed** | `10_automated_tests/final_airflow_container.txt` |
| End-to-end (real pipeline, isolated) | `pytest -m e2e` | **final: 4 passed** (quality + idempotency, malformed messages, stop-mid-backlog recovery, production reconciliation). The first run had 1 intermittent failure (F-07), fixed and re-run | `10_automated_tests/final_e2e.txt` (also `pytest_e2e_final_20261016.txt`); failed first run kept: `pytest_e2e_20261016_FAILED_stop_race.txt` |

## B. Functional test cases
| ID | Area | Input | Expected | Actual | Result | Evidence / test |
|---|---|---|---|---|---|---|
| TC-01 | Config | unset required variable; default; override | clear error; default; override wins | as expected | PASS | `tests/unit/test_mapping.py` |
| TC-02 | Mapping | real-shaped row; empty optional date; blank required; bad number; bad timestamp | typed payload; null; `MISSING_REQUIRED`; `TYPE_ERROR` | as expected; 0 of 99,441 real rows rejected by mapping | PASS | `01_dataset/join_and_mapping_check.txt` |
| TC-03 | Profiling | 9 real files | counts, hashes, key uniqueness | orders 99,441 rows, order_id unique | PASS | `01_dataset/dataset_profile.txt` |
| TC-04 | Producer | valid row; invalid row; dry-run; broker down; duplicate; same file twice; `--skip` | published; dead-letter; nothing published; failure counted; counted not dropped; same `record_uid`; slice | as expected (10 tests) | PASS | `tests/unit/test_producer.py` |
| TC-05 | Producer, real | full file | 99,441 read = 99,427 valid + 14 rejected; 99,427 published, 0 failed | as expected; broker delta 33,199/33,255/32,973 | PASS | `03_kafka/producer_full_run.txt` |
| TC-06 | API client (mocked HTTP: logic only) | 4xx, 429+Retry-After, 5xx, timeout, bad JSON, schema errors, cache | retry/validate | 15 passed | PASS (not API proof) | `tests/unit/test_api_client.py` |
| TC-07 | API, real | live request | HTTP 200, valid rates | HTTP 200, 5 rows (544 for the full span) | PASS | `02_api/api_real_request.txt`, `tests/integration/test_api_real.py` |
| TC-08 | API results stored | warehouse | provenance on every cached response and enriched order | all 99,427 orders enriched with HTTP 200 provenance | PASS | `tests/integration/test_api_stored_results.py` |
| TC-09 | Quality rules | each reject rule; empty delivery date for non-delivered order | rejected / valid | as expected | PASS | `05_data_quality/pytest_validators_and_mapping.txt` |
| TC-10 | Dead letter + duplicates + re-ingestion (real schema) | Demo 3 file twice | invalid row rejected with reason; one warehouse row; no change on re-run | 15/15 checks | PASS | `05_data_quality/quality_idempotency_test.txt` |
| TC-11 | Malformed messages in Spark | bad JSON; missing fields | `MALFORMED_JSON`; `SCHEMA_MISMATCH` in `dead_letter/stream` | 7/7 checks | PASS | `04_spark/malformed_message_test.txt` |
| TC-12 | Spark transforms | static DataFrames | parse, flatten, dedup | 5 passed | PASS | `10_automated_tests/pytest_spark_in_docker_20261016.txt` |
| TC-13 | Checkpoint recovery | stop mid-backlog, publish new + repeated records while down, restart | resumes at stored offset; no duplicates | 18/18 (final), see C for repeatability | PASS | `04_spark/recovery_test.txt` |
| TC-14 | Parquet | batch folders | counts per layout | read-back 99,626 rows / 99,427 distinct; each batch folder equals its log | PASS | `06_parquet/`, `tests/unit/test_parquet.py` |
| TC-15 | PostgreSQL upsert | same `record_uid` twice; source duplicate; in-batch duplicate; re-ingested discarded copy | one row; classified; logged once | 7 passed | PASS | `05_data_quality/pytest_duplicates_postgres.txt` |
| TC-16 | Analytics views | real warehouse | KPIs consistent with profile | delivered 96,470 = 96,478 - 8 rejected | PASS | `07_postgresql/analytics_views_output.txt` |
| TC-17 | Reconciliation | production topic | all differences 0 or explained | all 0 | PASS | `07_postgresql/reconciliation_production_final.txt` |
| TC-18 | Reconciliation fails on mismatch | declare 1 of 3 runs | exit code 1, `UNEXPLAINED` | exit 1, 3 rows UNEXPLAINED | PASS | `07_postgresql/reconciliation_mismatch_exit_code.txt` |
| TC-19 | Production idempotency | re-send first 200 orders | 0 inserted, 199 retries, count unchanged | as expected | PASS | `07_postgresql/reingestion_production.txt` |
| TC-20 | Airflow DAG real run | trigger with limit 300 | 9/9 tasks success | success in ~2 min | PASS | `08_airflow/dag_run2_streampulse_batch.txt` |
| TC-21 | Airflow retry/failure | unreachable Kafka | 3 attempts, task failed, DAG failed | as expected | PASS (intended failure) | `08_airflow/dag_failure_demo_run.txt` |
| TC-22 | Dashboard status checks | real broker; unreachable broker; services stopped | ok; down; red/unknown/unavailable | as expected (17 tests + screenshots) | PASS | `09_dashboard/` |
| TC-23 | Secrets | scan committable files | no secret values | clean | PASS | `scripts/secret_scan.py` |

## C. Failures found during testing (kept as history)
| ID | What failed | Cause | Fix | Evidence |
|---|---|---|---|---|
| F-01 | Host Spark did not start | `winutils.exe` missing on Windows | Spark runs in Docker | `00_environment/spark_baseline_existing_stream.txt` |
| F-02 | Malformed JSON labelled `SCHEMA_MISMATCH` | `from_json` returns a struct of nulls | JSON validity checked separately (test unchanged) | `04_spark/spark_transform_tests_docker.txt` |
| F-03 | Loader crashed on `np.float64` | psycopg2 cannot render numpy floats | explicit casts | `07_postgresql/load_reference_attempt1_FAILED_numpy_float.txt` |
| F-04 | First Airflow run failed | checkpoint written as root, Airflow ran as another user | Airflow container runs as root | `08_airflow/dag_run1_FAILED_*` |
| F-05 | Dashboard showed Kafka down while it ran | unsupported client option reported as "down" (and my first "down" tests passed for the wrong reason) | option removed; positive real-broker test added | `09_dashboard/`, phase_08.md |
| F-06 | Charts plotted NUMERIC as text | `Decimal` values | converted to float | phase_08.md |
| F-07 | **Intermittent e2e failure of the recovery test** | `query.stop()` raced with a batch that started the instant the stop was requested (`InterruptedException`); data stayed correct (17/18 checks) | stop redesigned: batches that start after a stop request raise `StopRequested` before doing any work; checker accepts only that signature | `10_automated_tests/pytest_e2e_20261016_FAILED_stop_race.txt` (failed run); fix verified by 3 consecutive 18/18 runs `04_spark/recovery_stop_stress_run{1,2,3}.txt` and `pytest_e2e_final_20261016.txt` |
| F-08 | **Dashboard printed Streamlit's internal documentation on 3 pages** (found by viewing a Demo 3 screenshot; the production screenshot taken earlier had the same defect and was never inspected) | `st.dataframe(...) if cond else st.write(...)` as a bare statement: Streamlit "magic" displays the returned object | replaced by an if/else helper; AST regression test `tests/unit/test_dashboard_source.py`; all 7 pages recaptured and viewed | `09_dashboard/dashboard_04_data_quality.png`, phase_08.md |

## D. Manual demo test cases (PENDING_HUMAN)
| ID | Demo | Pass criterion | Result |
|---|---|---|---|
| MD-1 | Demo 1 end-to-end flow | reconciliation zero/explained; dashboard counts equal SQL | PENDING_HUMAN |
| MD-2 | Demo 2 real API | HTTP 2xx live, row stored with provenance, no secrets visible | PENDING_HUMAN |
| MD-3 | Demo 3 data quality | rejection with reason; unique count stable after re-run | PENDING_HUMAN |
| MD-4 | Demo 4 recovery + Airflow | restart resumes from the stored offset without duplicates; DAG success; clean reconciliation | PENDING_HUMAN |
