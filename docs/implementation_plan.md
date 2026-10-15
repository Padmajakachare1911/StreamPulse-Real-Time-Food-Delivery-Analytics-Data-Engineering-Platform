# Implementation Plan

StreamPulse — **Real-Time Order Delivery Analytics & Data Engineering Platform (Olist e-commerce dataset)**.
Data is a *historical dataset replayed through a streaming pipeline*, never live orders. (Please confirm the retitling with the professor; the dataset is e-commerce, not food delivery.)

## Pipeline
`Olist CSVs → Python ingestion (join + validate) → Kafka (ecommerce_orders) → Spark Structured Streaming (Docker) → Parquet + PostgreSQL → Streamlit`
plus a real external API (currency rates) cached in PostgreSQL, and Airflow for finite batch tasks.

## Phases
P0 audit · P1 dataset + mapping · P2 API · P3 Kafka producer · P4 Spark · P5 data quality + idempotency · P6 PostgreSQL · P7 Airflow · P8 Streamlit · P9 tests · P10 demo docs · P11 final audit. Order and gates follow `docs/02_WORKFLOW.md`.

## Decisions and deviations
| # | Decision | Why |
|---|---|---|
| D1 | Topic renamed `food_orders` → `ecommerce_orders`, config-driven via `KAFKA_TOPIC`, 3 partitions kept. | Addendum §4: the data is e-commerce; "food" naming would misrepresent it. The old topic is left untouched (not deleted). |
| D2 | Old `order_producer.py` (random fake food orders) is retired, replaced by a dataset replay producer. | It never used real data (rule 3). |
| D3 | **Spark runs in Docker**, not on the Windows host. | Host Spark fails without winutils (`docs/baseline_audit.md` §4); third-party executables are not allowed. |
| D4 | Kafka gets a second internal listener (`kafka:29092`) for containers; host keeps `localhost:9092`. | Containers cannot reach `localhost:9092` of the host. |
| D5 | One order event per `order_id` from `orders`, denormalized with aggregated items, payments and customer city/state. Reviews, sellers, products, geolocation are reference tables loaded in batch. | Addendum §4. |
| D6 | Replay sorted by `order_purchase_timestamp`. | Follows historical order flow. |
| D7 | API choice: Frankfurter (ECB rates, no key) for BRL→USD by purchase date; confirmed after checking live docs in Phase 2. | Addendum §7 option 1; few requests; clear business meaning. Reasoning in `docs/api_integration.md`. |
| D8 | Guarantee wording: at-least-once delivery + idempotent sink (`ON CONFLICT (record_uid)`). | Rule 13. |
| D9 | Kafka message key = `order_id`. | Each order is one event, so no per-order ordering is needed. Measured: order_id spreads 33.4/33.4/33.2 %; customer_state would put 66.9 % in one partition (`docs/evidence/03_kafka/key_options_computed_offline.txt`). |
| D10 | Producer: `acks=all`, idempotence on, gzip, batching; `--limit N` or `--full` is mandatory. | Safe default: a full 99k replay never starts by accident. |
| D11 | 14 real orders are rejected at ingest (8 'delivered' without delivery date, 6 with a delivery date but a different status). | They contradict themselves and would distort delivery-time KPIs; they are kept in the dead-letter queue with the reason, not lost. |
| D12 | Old `order_producer.py` deleted (kept in git history). | It generated random fake food orders, never the dataset. |
| D13 | The limited 100-order run and the full run were published to the same topic. | The first orders therefore exist twice in the topic (same `record_uid`, different `ingest_run_id`); Phase 5/6 prove the sink ignores them. Topic total = 99 + 99,427 = 99,526. |

## Open items / assumptions
- Professor did not name an API (`docs/inputs/api_info.md` absent), so the agent chooses.
- Dataset license CC BY-NC-SA 4.0 was seen on the Kaggle page via a search excerpt (`docs/evidence/01_dataset/license_check.txt`); the student should re-confirm it on the page before submission.
- Four manual demos and all live screenshots are `PENDING_HUMAN`.
