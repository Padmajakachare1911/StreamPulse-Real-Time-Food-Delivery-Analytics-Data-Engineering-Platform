# Architecture

This page describes only what is **implemented and was run**. Evidence for each box is listed in `docs/evidence_register.md`.

## Data flow
```
 data/raw/*.csv (9 Olist files, read-only)
      │ src/common/orders_source.py   LEFT JOIN orders + customers + aggregated items + aggregated payments
      │ src/common/mapping.py         types from config/field_mapping.yaml
      │ src/processing/classify.py    quality rules from config/quality_rules.yaml
      │ src/api/enrich.py             BRL→USD rate of the purchase date (Frankfurter API, cached)
      ▼
 src/producer/dataset_producer.py ──► Kafka  topic ecommerce_orders (3 partitions, replication factor 1)
      │ rejected                         key = order_id (measured spread 33.4 / 33.4 / 33.2 %)
      ▼
 data/dead_letter/ingest/*.jsonl         value = JSON envelope: record_uid, business_hash, payload, raw, api_enrichment
                                         ▼
                      src/streaming/order_stream.py  (Spark 4.2.0 in Docker, foreachBatch)
                         │ malformed / schema-invalid ──► data/dead_letter/stream/*.jsonl
                         ├──► Parquet  data/processed/parquet/batch_id=NNNNNNNN/   (landing layer, append-only)
                         └──► PostgreSQL core.orders   (INSERT … ON CONFLICT, one transaction per batch)
                                         ▼
                      analytics.* views ──► src/dashboard (Streamlit)
 Batch loaders (src/warehouse/load_reference.py): products, sellers, items, reviews, geolocation centroids,
 ingestion runs, dead letters, API cache → PostgreSQL
 Airflow (airflow/dags): finite steps only; Spark runs with trigger availableNow
```

## User-submitted demo orders (added 2026-10-16)
```
 Streamlit page "Place a demo order"  ──►  src/demo/demo_order.py   same mapping + quality rules (date window ends today),
      │                                         same API enrichment, same producer settings, key = order_id
      │                                         source_category = user_demo, order_id = demo-..., run id = demo-order-...
      ▼
 Kafka topic ecommerce_orders (the SAME topic) ──► Spark job (unchanged) ──► Parquet + core.orders (unchanged)
      │
      └── data/processed/demo_orders/submissions.jsonl   partition/offset of every demo message (used by the trace and by reconcile.py)
 Trace (src/demo/order_trace.py): submission log -> Kafka read-back at that offset -> Parquet row + batch_id (+ BATCH_SUMMARY)
                                  -> PostgreSQL row.  Shown on the dashboard page and by scripts/trace_order.py.
 analytics.historical_orders (view) = core.orders without user_demo; the 11 KPI views read it, so KPIs stay historical.
```
Nothing in the Spark job, the Parquet layout or the warehouse schema was changed for this; demo orders are distinguished only by data
(`source_category`, id prefixes). Step-by-step demonstration: `docs/live_demo_order_guide.md`.

## Components
| Component | Technology | Where |
|---|---|---|
| Source | Olist CSV files | `data/raw` |
| Ingestion | Python 3.11, kafka-python | `src/producer` |
| Broker | Apache Kafka 4.1 (KRaft, single node), Docker | `docker-compose.yml` |
| Stream processing | PySpark 4.2.0 + `spark-sql-kafka-0-10_2.13:4.2.0` in a Linux container | `src/streaming`, `docker/spark` |
| Landing layer | Parquet (pyarrow) | `data/processed/parquet` |
| Warehouse | PostgreSQL 16 | `sql/schema.sql`, `sql/analytics.sql`, `src/warehouse` |
| External API | Frankfurter (ECB reference rates), no key | `src/api` |
| Orchestration | Apache Airflow 2.10 (LocalExecutor) | `airflow/dags`, `docker/airflow` |
| Dashboard | Streamlit 1.66 | `src/dashboard` |

## Identity and duplicates
- `record_uid` = hash(SHA-256 of the orders file + row number): the same file always gives the same IDs, so replays are recognised.
- `business_hash` = hash of the natural key `order_id` (verified unique in the real file).
- Retry duplicate = same `record_uid`; source duplicate = different `record_uid`, same `business_hash`.

## Layers behave differently on purpose
Parquet is an append-only landing layer (it keeps repeats that arrive in later batches); PostgreSQL is the deduplicated warehouse. See `docs/data_quality_rules.md`.

## Guarantees and limits
- Kafka gives durable storage and offsets; the Spark checkpoint records which offsets were processed; the PostgreSQL write is idempotent. Together: **at-least-once delivery with idempotent writes** (effectively-once results in the warehouse). We do **not** claim exactly-once end to end.
- A batch that is re-run after a stop or crash overwrites its own Parquet folder and its own dead-letter file and finds its rows already in PostgreSQL.
- Single-node Kafka (replication factor 1), local Spark mode and a single PostgreSQL instance: this is a demonstration architecture, not a production deployment.

## Deviations from the original plan
| Plan | Implemented | Reason |
|---|---|---|
| Topic `food_orders`, food-delivery data | Topic `ecommerce_orders`, Olist e-commerce data | the dataset is e-commerce (`docs/07_DATASET_ADDENDUM_OLIST.md`) |
| Spark on the Windows host | Spark in Docker | host Spark needs `winutils.exe` |
| Table `core.food_orders` | `core.orders` | naming matches the data |
| Airflow runs the stream | Airflow runs `availableNow` batches | an infinite stream is not a finite task |
