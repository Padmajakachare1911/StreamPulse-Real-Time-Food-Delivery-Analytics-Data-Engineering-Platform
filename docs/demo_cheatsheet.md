# Demo Cheat Sheet (copy-paste PowerShell)

Working directory for **every** command: the repository root
`C:\Users\Padmaja Kachare\Documents\StreamPulse-Real-Time-Food-Delivery-Analytics-Data-Engineering-Platform`
In every terminal first run: `$py = ".\.venv\Scripts\python.exe"`
App = PowerShell unless stated. **Dry-run** column: `VERIFIED` = the agent ran it (evidence in `docs/evidence/11_manual_demos/dry_runs/`); `UNTESTED` = not run.
Terminals: **T1** infrastructure (stays open) · **T2** Spark (stays open while running) · **T3** commands (closes) · **T4** dashboard/Airflow (stays open) · **Browser**.

## 1. Environment verification
| T | Command | Success looks like | Independent check | If it fails | Safe stop | Dry-run |
|---|---|---|---|---|---|---|
| T3 | `.\scripts\check_env.ps1` | versions printed; port 9092 `reachable = True` | `docker ps` | start Docker Desktop | – | VERIFIED |
| T3 | `$py scripts\check_dataset_fingerprint.py` | `All 9 files match the fingerprints...` | `Get-ChildItem data\raw` | place the 9 CSV files in `data\raw` | – | VERIFIED |

## 2. Start Docker services / verify Kafka
| T | Command | Success looks like | Independent check | If it fails | Safe stop | Dry-run |
|---|---|---|---|---|---|---|
| T1 | `.\scripts\kafka_start.ps1` | `Kafka health: healthy` | `docker ps` shows `(healthy)` | `docker logs streampulse-kafka` | `.\scripts\kafka_stop.ps1` | VERIFIED |
| T3 | `.\scripts\init_db.ps1` | `schema OK` | `docker exec streampulse-postgres psql -U streampulse -d streampulse -c "\dt core.*"` | check `POSTGRES_PASSWORD` in `.env` | `docker compose stop postgres` | VERIFIED |
| T3 | `.\scripts\kafka_list_topics.ps1` | lists `ecommerce_orders` | – | rerun `kafka_start.ps1` | – | VERIFIED |
| T3 | `.\scripts\kafka_describe_topic.ps1` | `PartitionCount: 3  ReplicationFactor: 1` | – | – | – | VERIFIED |

## 3. Ingestion (limited and full)
| T | Command | Success looks like | Independent check | If it fails | Safe stop | Dry-run |
|---|---|---|---|---|---|---|
| T3 | `$py -m src.producer.dataset_producer --limit 100 --dry-run` | JSON summary, `rows_published: 0` | – | read the error line | – | VERIFIED |
| T3 | `$py -m src.producer.dataset_producer --limit 200 --run-id demo1` | `rows_read 200`, `rows_valid 199`, `rows_rejected 1`, `rows_publish_failed 0` | `.\scripts\kafka_partition_counts.ps1` total grows by 199 | Kafka not running → `kafka_start.ps1` | Ctrl+C (flushes and writes the summary) | VERIFIED |
| T3 | `$py -m src.producer.dataset_producer --full` (~100 s; evidence of a full run is pre-captured) | `rows_read 99441`, `rows_valid 99427`, `rows_rejected 14` | `.\scripts\kafka_partition_counts.ps1` | – | Ctrl+C | VERIFIED (earlier run) |
| T3 | `.\scripts\kafka_consume_n.ps1 -Count 1 -Partition 0` | one JSON envelope with `payload`, `raw`, `record_uid` | – | – | – | VERIFIED |
| T3 | `.\scripts\kafka_partition_counts.ps1` | messages per partition (measured) | – | – | – | VERIFIED |

## 4. Real API call
| T | Command | Success looks like | Independent check | If it fails | Safe stop | Dry-run |
|---|---|---|---|---|---|---|
| T3 | `$py scripts\api_demo_call.py --sample --no-cache` | `HTTP : 200`, `Rows : 5` | open the printed URL in a browser | no internet: the failure is real, say so | – | VERIFIED |
| T3 | `$py scripts\api_demo_call.py --simulate-failure` | `API failed after 3 attempts: ConnectionError` (retry waits 1 s, 2 s) | – | – | – | VERIFIED |

## 5. Spark
| T | Command | Success looks like | Independent check | If it fails | Safe stop | Dry-run |
|---|---|---|---|---|---|---|
| T2 | `.\scripts\run_stream.ps1 -Trigger availableNow` | `BATCH_SUMMARY {...}` lines then `Stream stopped.` | `$py scripts\parquet_readback.py` | Docker not running; Spark image missing → `docker compose --profile spark build spark` | – (exits by itself) | VERIFIED |
| T2 | `.\scripts\run_stream.ps1` (continuous) | a `BATCH_SUMMARY` whenever new data arrives | – | – | `.\scripts\stop_stream.ps1` (waits for the batch) or Ctrl+C | VERIFIED |

## 6. Parquet and PostgreSQL
| T | Command | Success looks like | Independent check | If it fails | Dry-run |
|---|---|---|---|---|---|
| T3 | `$py scripts\parquet_readback.py` | row count, distinct `record_uid`, schema, 3 sample rows | `.\scripts\run_tests_docker.ps1` / Spark read-back | no files → run Spark first | VERIFIED |
| T3 | `docker exec streampulse-postgres psql -U streampulse -d streampulse -c "SELECT count(*), count(DISTINCT record_uid) FROM core.orders"` | `99427 \| 99427` | `sql\quality_checks.sql` (all problem rows 0) | password/port | VERIFIED |
| T3 | `docker cp sql\analytics_samples.sql streampulse-postgres:/tmp/a.sql; docker exec streampulse-postgres psql -U streampulse -d streampulse -f /tmp/a.sql` | KPI tables | dashboard "Business KPIs" | – | VERIFIED |
| T3 | `$py scripts\reconcile.py` | `RESULT: all differences are zero or explained` | exit code 0 (`$LASTEXITCODE`) | a `[UNEXPLAINED]` row: read its note | VERIFIED |

## 7. Airflow and Streamlit
| T | Command | Success looks like | Independent check | If it fails | Safe stop | Dry-run |
|---|---|---|---|---|---|---|
| T4 | `.\scripts\airflow_up.ps1` | `Airflow is healthy` | browser http://localhost:8080 (user `admin`) | `docker logs streampulse-airflow` | `.\scripts\airflow_down.ps1` | VERIFIED |
| T3 | `.\scripts\airflow_run_dag.ps1 -Dag streampulse_batch -Conf '{"limit": 300}'` | `FINAL STATE: success`, 9 tasks success (~2 min) | Airflow UI grid all green | task log: `airflow\logs\...` | – | VERIFIED |
| T3 | `.\scripts\airflow_run_dag.ps1 -Dag streampulse_failure_demo` | 3 attempts, `FINAL STATE: failed` (**intended**) | UI shows the red task and orange downstream | – | `.\scripts\airflow.ps1 dags pause streampulse_failure_demo` | VERIFIED |
| T4 | `.\scripts\run_dashboard.ps1` | `You can now view your Streamlit app` | browser http://localhost:8501, status lights green | port 8501 busy → close the other dashboard | Ctrl+C | VERIFIED |

## 8. Tests
| T | Command | Success looks like | Dry-run |
|---|---|---|---|
| T3 | `$py -m pytest -m unit -q` | `73 passed, 1 skipped` (the skip is the Airflow module: it runs in its container) | VERIFIED |
| T3 | `$py -m pytest -m integration -q` | `18 passed` (needs Kafka, PostgreSQL, internet) | VERIFIED |
| T3 | `.\scripts\run_tests_docker.ps1` | `5 passed` (Spark) | VERIFIED |
| T3 | `.\scripts\run_tests_airflow.ps1` | `8 passed` (Airflow must be running) | VERIFIED |
| T3 | `$py -m pytest -m e2e -q` (~3.5 min) | `4 passed` (runs the real pipeline in isolation) | VERIFIED |

## 9. Capture evidence
| T | Command | Result |
|---|---|---|
| T3 | `.\scripts\save_evidence.ps1 -TaskId DEMO1 -OutFile docs\evidence\11_manual_demos\demo_1_dataset_ingestion\reconciliation.txt -Command '$py scripts\reconcile.py'` | file with header (task, time, command, git commit) and the real output |
| T3 | `$py scripts\capture_screenshots.py dashboard --out docs\evidence\11_manual_demos\demo_1_dataset_ingestion --prefix demo_1` | real screenshots of the running dashboard (or take them by hand) |
| T3 | `$py scripts\secret_scan.py` | `RESULT: clean` before committing evidence |

## 10. Isolated demos 3 and 4
| T | Command | Dry-run |
|---|---|---|
| T3 | `.\scripts\demo_setup.ps1 -Demo 3` (or `4`) | VERIFIED |
| T2 | `.\scripts\run_stream.ps1 -Demo 3 -Trigger availableNow` | VERIFIED |
| T3 | `.\scripts\stop_stream.ps1 -Demo 4` | VERIFIED |
| T4 | `.\scripts\run_dashboard.ps1 -Demo 3` | VERIFIED |
| T3 | `.\scripts\reset_demo_state.ps1 -Demo 3` (asks `YES`; never touches `data\checkpoints\orders`) | VERIFIED |

## 11. Stop services (in this order)
```powershell
.\scripts\stop_stream.ps1        # T2 stream (if running)
.\scripts\airflow_down.ps1       # Airflow
# Ctrl+C in T4                    # dashboard
docker compose stop              # Kafka + PostgreSQL (data stays in Docker volumes)
```
**Never** run `docker compose down -v` or delete `data\checkpoints\` during a demo.
