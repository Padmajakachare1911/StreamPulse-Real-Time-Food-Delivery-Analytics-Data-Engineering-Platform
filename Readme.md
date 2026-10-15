# StreamPulse

**Real-Time Order Delivery Analytics & Data Engineering Platform (Olist e-commerce dataset)**

StreamPulse replays a real, historical e-commerce dataset through a streaming data pipeline and turns it into analytics.
It is a student data-engineering project that uses Kafka, Spark Structured Streaming, PostgreSQL, Parquet, Airflow, Streamlit and one real external API.

> **Honest framing.** The data is the *Brazilian E-Commerce Public Dataset by Olist* (about 100,000 real orders, 2016–2018), not food delivery and not live. It is a **historical dataset replayed through a streaming pipeline**. The external API (Frankfurter / ECB exchange rates) provides *reference data*; it does not contain orders.
> (The project name keeps "StreamPulse"; the original subtitle said "Food Delivery". The dataset is e-commerce, so the subtitle was changed: please confirm the retitling with the professor.)

## Architecture (what is implemented)

```
Olist CSV files (data/raw)
   │  join orders + customers + items + payments, map types, validate, enrich with the API
   ▼
Python replay producer ──► Kafka topic "ecommerce_orders" (3 partitions, key = order_id)
   │ rejected rows ──► dead-letter files (stage=ingest)
   ▼
Spark Structured Streaming (runs in Docker) ──► Parquet landing layer (append-only)
   │ malformed messages ──► dead-letter files (stage=stream)
   └────────────────────────► PostgreSQL warehouse (core.*, idempotent upsert) ──► analytics views ──► Streamlit dashboard
External API: Frankfurter / ECB BRL→USD rates (one request per date range, cached, stored in PostgreSQL)
Airflow orchestrates the FINITE steps (bounded replay, Spark availableNow, checks, reconciliation)
```

Delivery guarantee: **at-least-once delivery with an idempotent sink** (not exactly-once). Details: `docs/data_quality_rules.md`, `docs/architecture.md`.

## Quick start (Windows, PowerShell, from the repository root)

1. Put the Olist CSV files in `data/raw/` (download: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce, see `data/raw/README.md`).
2. Create the environment and the settings file:
   ```powershell
   python -m venv .venv
   .\.venv\Scripts\python.exe -m pip install -r requirements.txt
   Copy-Item .env.example .env        # then set POSTGRES_PASSWORD and AIRFLOW_ADMIN_PASSWORD in .env
   ```
3. Start the services and create the tables:
   ```powershell
   .\scripts\kafka_start.ps1
   .\scripts\init_db.ps1
   ```
4. Replay data, process it, and look at it:
   ```powershell
   .\.venv\Scripts\python.exe -m src.producer.dataset_producer --limit 500       # bounded replay (use --full for all orders)
   .\scripts\run_stream.ps1 -Trigger availableNow                               # Spark (in Docker): Kafka -> Parquet + PostgreSQL
   .\.venv\Scripts\python.exe scripts\reconcile.py                              # counts across every layer
   .\scripts\run_dashboard.ps1                                                  # http://localhost:8501
   ```
5. Orchestration and tests: `.\scripts\airflow_up.ps1` (UI at http://localhost:8080), `.\.venv\Scripts\python.exe -m pytest -m unit`.

Full instructions, troubleshooting and shutdown: `docs/setup_guide.md`. Demonstration steps: `docs/manual_demo_guide.md` and `docs/demo_cheatsheet.md`.

## Why Spark runs in Docker
On this Windows machine host Spark fails at start-up (`winutils.exe` missing, see `docs/baseline_audit.md`). Running Spark in a Linux container avoids downloading a third-party executable and makes the run reproducible.

## Documentation map
| Topic | File |
|---|---|
| Architecture and data flow | `docs/architecture.md` |
| Setup, run, shutdown, troubleshooting | `docs/setup_guide.md` |
| Dataset source, license, dictionary, quality report | `docs/data_source.md`, `docs/data_dictionary.md`, `docs/data_quality_report.md` |
| Quality rules, duplicates, Parquet vs PostgreSQL | `docs/data_quality_rules.md` |
| Real API integration | `docs/api_integration.md` |
| Task status and evidence | `docs/task_tracker.md`, `docs/evidence_register.md`, `docs/evidence/` |
| Manual demos, cheat sheet, test cases, viva | `docs/manual_demo_guide.md`, `docs/demo_cheatsheet.md`, `docs/test_cases.md`, `docs/viva_questions.md` |
| Decisions and deviations | `docs/implementation_plan.md` |

## Data attribution
Data: "Brazilian E-Commerce Public Dataset by Olist", Olist, via Kaggle, licensed CC BY-NC-SA 4.0. Used unmodified for a non-commercial university project. The raw files are never committed.
