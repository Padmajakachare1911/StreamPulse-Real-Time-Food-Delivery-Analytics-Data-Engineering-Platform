# Baseline Audit (Phase 0)

Audit date: 2026-10-15. Everything below was observed by running commands; evidence files are in `docs/evidence/00_environment/`.

## 1. Repository state
| Item | Finding |
|---|---|
| Branch | `main` |
| Remote | `origin` → `https://github.com/Padmajakachare1911/StreamPulse-Real-Time-Food-Delivery-Analytics-Data-Engineering-Platform.git` |
| Commits | **None yet** (everything was untracked at the start) |
| Existing source | `src/common/config.py`, `src/producer/order_producer.py`, `src/streaming/order_stream.py` |
| Empty folders | `airflow/`, `config/`, `scripts/`, `sql/`, `tests/` |
| Duplicates | None found |

## 2. What exists and works
- **Docker Desktop + Kafka**: container `streampulse-kafka` (apache/kafka 4.1.0, KRaft) was already running. Topic `food_orders` has 3 partitions, replication factor 1. A message produced to a test topic was consumed back (`kafka_smoke_test.txt`).
- **Toolchain**: Python 3.11.9 (.venv), OpenJDK 17.0.20.1, Docker 29.8.2, Compose v5.6.0, PySpark 4.2.0, kafka-python 3.0.11 (`env_check.txt`).
- **`config.py`**: loads `.env` with `python-dotenv`. Small and readable; kept and extended.

## 3. What is broken or must change
| # | Finding | Evidence | Decision |
|---|---|---|---|
| 1 | **`order_producer.py` generates random fake food orders** (hard-coded restaurants/menu). It is not the dataset, so it must not be used for the project. | source | Retire. Replace with `src/producer/dataset_producer.py` that replays the real Olist file. Reuse its working Kafka settings (`acks=all`, JSON serializer). |
| 2 | **`order_stream.py` has a food-order schema**, `startingOffsets=latest`, hard-coded checkpoint path, no DLQ, no PostgreSQL. | source | Refactor in Phase 4 (keep the `spark.jars.packages` connector line and the `from_json` approach). |
| 3 | **Spark cannot start on this Windows host.** `SparkContext` fails to initialize: `HADOOP_HOME and hadoop.home.dir are unset` (winutils.exe missing). | `spark_baseline_existing_stream.txt` | This is a **hard failure**, not a harmless warning. See section 4. |
| 4 | Topic name `food_orders` and all "food" wording is wrong for the Olist e-commerce data. | `docs/07_DATASET_ADDENDUM_OLIST.md` | Rename to `ecommerce_orders` (config driven). |
| 5 | `Readme.md` starts with a stray `@'` (PowerShell here-string leftover) and is truncated. | source | Rewrite in Phase 11 (and a short fix earlier). |
| 6 | `.gitignore` did not cover checkpoints, Parquet or `.gitkeep` exceptions. | `gitignore_audit.txt` | Fixed in Phase 0. |
| 7 | `.env.example` still names the food topic and lacks most settings required by M0. | source | Extend in Phase 0/1. |
| 8 | Kafka advertises only `localhost:9092`; a Spark container could not reach it. | `docker-compose.yml` | Add a second internal listener in Phase 3/4. |
| 9 | `requirements.txt` is unpinned and lacks `requests`, `pyyaml`, `pyarrow`, `streamlit`. | source | Pin in Phase 1+. |

## 4. HADOOP_HOME / winutils assessment (P0-05)
- **Observed**: `WARN Shell: Did not find winutils.exe` followed by `ERROR SparkContext: Error initializing SparkContext` in `Shell.getWinUtilsPath` during `FileUtil.chmod`. No Parquet or checkpoint files were produced.
- **Conclusion**: on this machine, local-mode PySpark that writes to the Windows file system **is blocked** without winutils/hadoop.dll.
- **Options considered**
  1. Download `winutils.exe` from a third-party repository: rejected (rule 16: never download executables from untrusted sources).
  2. Run Spark inside Docker (Linux): needs no winutils, reproducible for evaluators, uses the already-installed Docker Desktop. **Chosen.**
  3. Run in WSL2: possible, but needs separate Java/Python setup and is less reproducible.
- **Trade-off**: the Spark job runs in a container and reads/writes through a bind mount; Kafka needs an internal listener. Documented in `docs/implementation_plan.md`.

## 5. Spark ↔ Kafka connector compatibility (P0-04)
Declared: PySpark 4.2.0 (Scala 2.13) with `org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0`. Maven resolution of the connector succeeded in the baseline run, but the job never reached the Kafka source because of the winutils failure. **Compatibility is therefore NOT yet verified**; it will be verified in Docker in Phase 4.

## 6. Missing inputs (P0 step 10)
| Input | Status |
|---|---|
| Olist dataset | **Present**: 9 CSV files in `data/raw/` (sizes recorded in Phase 1) |
| Professor's API | **Not specified**: `docs/inputs/api_info.md` does not exist. The agent selects the API in Phase 2 (candidate: Frankfurter BRL→USD per the addendum) |
| API key | None required by the candidate |
| License | To be verified on the Kaggle page in Phase 1 |
| Docker running | Yes |
| Git credentials for push | Unverified until the first push |

## 7. Notes
- PowerShell 5.1 prepends a UTF-8 BOM when piping text to `docker exec -i`; this appears in the smoke-test output only and does not affect Python producers.
- `WSL 1 is not supported` message appeared on `wsl --status`; the default distribution is Ubuntu on WSL2. Not used.
- The extra stopped container `lucid_leavitt` is unrelated to this project and was left alone.
- The `smoke_test` Kafka topic was created for Phase 0 checks only.
