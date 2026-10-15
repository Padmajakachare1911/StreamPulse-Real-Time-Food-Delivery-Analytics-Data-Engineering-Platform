# Setup Guide (Windows, PowerShell)

Tested on: Windows 11, Python 3.11.9, OpenJDK 17, Docker Desktop (WSL2 backend, 8 GB memory), PowerShell 5.1.
Every command below is run **from the repository root** unless it says otherwise.

## 1. Prerequisites
| Tool | Check | Notes |
|---|---|---|
| Python 3.11 | `python --version` | virtual environment `.venv` |
| Java 17 | `java -version` | only needed by tools on the host; Spark itself runs in Docker |
| Docker Desktop | `docker --version`, `docker compose version` | must be running; give it ≥ 6 GB memory |
| Git | `git --version` | |
| Internet | | first build downloads images and the Spark Kafka connector; the exchange-rate API is a live service |

## 2. Get the data
Download the Olist dataset from https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce and place the 9 CSV files in `data/raw/` (see `data/raw/README.md`). Do not open and re-save them in Excel. The files are git-ignored.

```powershell
.\.venv\Scripts\python.exe scripts\check_dataset_fingerprint.py --files-only
```

## 3. Python environment and settings
```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
Copy-Item .env.example .env
```
Edit `.env` (it is git-ignored and never committed):
- `POSTGRES_PASSWORD` – choose a password (used by PostgreSQL, the code and Airflow).
- `AIRFLOW_ADMIN_PASSWORD`, `AIRFLOW_SECRET_KEY` – only needed for Airflow.
- Everything else has working defaults. The exchange-rate API needs **no key** (`API_KEY` stays empty).

Check the environment: `.\scripts\check_env.ps1`

## 4. Start the infrastructure
```powershell
.\scripts\kafka_start.ps1        # Kafka + creates topic ecommerce_orders (3 partitions)
.\scripts\init_db.ps1            # starts PostgreSQL if needed and creates tables + analytics views (repeatable)
```
Build the Spark image once (about 5–10 minutes the first time):
```powershell
docker compose --profile spark build spark
```

## 5. Run the pipeline
| Step | Command | Terminal |
|---|---|---|
| Profile the dataset (optional) | `.\.venv\Scripts\python.exe scripts\profile_dataset.py --summary` | T3 |
| Dry run (nothing published) | `.\.venv\Scripts\python.exe -m src.producer.dataset_producer --limit 100 --dry-run` | T3 |
| Replay 500 orders | `.\.venv\Scripts\python.exe -m src.producer.dataset_producer --limit 500` | T3 |
| Replay everything (~100 s) | `.\.venv\Scripts\python.exe -m src.producer.dataset_producer --full` | T3 |
| Spark, finite | `.\scripts\run_stream.ps1 -Trigger availableNow` | T2 |
| Spark, continuous | `.\scripts\run_stream.ps1` (stop: Ctrl+C or `.\scripts\stop_stream.ps1`) | T2, stays open |
| Reference tables, run summaries, dead letters, API cache | `.\.venv\Scripts\python.exe -m src.warehouse.load_reference` | T3 |
| Reconciliation | `.\.venv\Scripts\python.exe scripts\reconcile.py` | T3 |
| Dashboard | `.\scripts\run_dashboard.ps1` → http://localhost:8501 | T4, stays open |
| Airflow | `.\scripts\airflow_up.ps1` → http://localhost:8080 (user `admin`) | T4 |

Real API call: `.\.venv\Scripts\python.exe scripts\api_demo_call.py --sample`

## 6. Tests
```powershell
.\.venv\Scripts\python.exe -m pytest -m unit                 # fast, no services
.\.venv\Scripts\python.exe -m pytest -m integration          # needs PostgreSQL, Kafka and internet (real API)
.\scripts\run_tests_docker.ps1                               # Spark transform tests (inside the Spark container)
.\scripts\run_tests_airflow.ps1                              # DAG tests (inside the Airflow container, Airflow must be running)
```
Markers: `unit`, `integration`, `e2e`, `spark`, `airflow`, `manual`.

## 7. Shut down (nothing is deleted)
```powershell
.\scripts\stop_stream.ps1          # if the stream is running
.\scripts\airflow_down.ps1
docker compose stop                # stops Kafka and PostgreSQL; data stays in Docker volumes
```
**Never** delete `data/checkpoints/` or the Docker volumes unless you intend to reprocess from scratch. Reset scripts for the demos only touch demo output and ask for confirmation.

## 8. Ports
| Service | Port |
|---|---|
| Kafka (host) | 9092 (containers use `kafka:29092`) |
| PostgreSQL | 5432 |
| Airflow UI | 8080 |
| Streamlit dashboard | 8501 |

## 9. Version compatibility
PySpark 4.2.0 (Scala 2.13) ↔ `org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0` (same version). Java 17. kafka-python 3.0.11. PySpark declares `pandas>=2.2.0`; it prints a notice for pandas 3.x, but the project never uses Spark↔pandas conversion and all runs worked with pandas 3.0.6.

## 10. Troubleshooting (problems that really happened)
| Symptom | Cause | Fix |
|---|---|---|
| Spark on the host: `HADOOP_HOME and hadoop.home.dir are unset` | Windows needs `winutils.exe` | run Spark through `scripts\run_stream.ps1` (Docker) |
| Docker build: `openjdk-17-jre-headless has no installation candidate` | newer Debian base image | the Dockerfiles pin `bookworm` |
| `docker compose` error about `POSTGRES_PASSWORD` | `.env` missing | `Copy-Item .env.example .env` and set it |
| Airflow task: `Permission denied` on `data/checkpoints/...` | Spark container writes as root | the Airflow container runs as root (see `docker-compose.yml`) |
| Dashboard shows Kafka down while Kafka runs | (fixed) wrong client option | update the code; `status.check_kafka()` must return `ok` |
| PowerShell: `A parameter cannot be found` when piping to `docker exec -i` | PowerShell 5.1 adds a BOM to piped text | use `docker cp` + `psql -f` as in the scripts |
| `kafka_describe_topic.ps1` fails to parse | statement inside a `param()` default | scripts resolve the topic after `param()` |
