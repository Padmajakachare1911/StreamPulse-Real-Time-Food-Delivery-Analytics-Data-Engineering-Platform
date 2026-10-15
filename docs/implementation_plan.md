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

## Open items / assumptions
- Professor did not name an API (`docs/inputs/api_info.md` absent), so the agent chooses.
- Dataset license is "believed CC BY-NC-SA 4.0"; to be verified against the Kaggle page.
- Four manual demos and all live screenshots are `PENDING_HUMAN`.
