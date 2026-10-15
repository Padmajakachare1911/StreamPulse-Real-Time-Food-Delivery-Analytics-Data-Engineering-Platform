# 07 — Dataset Addendum: Olist Brazilian E-Commerce

**This addendum overrides any "food delivery" assumption in files 00–06.** Everything else in the spec pack still applies.

## 1. What the dataset actually is
- Source: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
- Owner: Olist (anonymized real commercial data, Brazil, roughly 2016–2018)
- License (verify on the Kaggle page and record it): believed to be CC BY-NC-SA 4.0 → attribute Olist, non-commercial use only (fine for coursework). Record attribution text in `docs/data_source.md`.
- Domain: **e-commerce marketplace orders with delivery tracking**, NOT food delivery. Never describe it as food delivery or restaurant data.
- Nature: historical transactional snapshot (not live).

## 2. Files present in `data/raw/` (CSV; do not open/save in Excel, do not modify)
`olist_customers_dataset`, `olist_geolocation_dataset`, `olist_order_items_dataset`, `olist_order_payments_dataset`, `olist_order_reviews_dataset`, `olist_orders_dataset`, `olist_products_dataset`, `olist_sellers_dataset`, `product_category_name_translation`

Check the real extension (`.csv`) and compute SHA-256 for **each** file.

## 3. Expected structure (from Kaggle documentation — MUST be verified by profiling, not trusted)
| Table | Expected grain / key | Notes to verify |
|---|---|---|
| orders | 1 row per `order_id` (~99k) | status values; timestamps; many null delivery dates for non-delivered orders (legitimate, not invalid) |
| order_items | per (`order_id`, `order_item_id`) (~112k) | multiple items per order; price, freight_value |
| order_payments | per (`order_id`, `payment_sequential`) (~104k) | multiple payment rows per order |
| order_reviews | per `review_id` (~99k) | duplicate review_ids / multiple per order possible; mostly null comments |
| customers | per `customer_id` (~99k) | `customer_unique_id` can repeat across customer_ids |
| products, sellers, translation | lookup | category nulls; Portuguese names need translation table |
| geolocation | ~1M rows, zip-prefix level, many duplicate rows | reference table; aggregate to one centroid per zip prefix |

Record the **actual** counts/columns in `docs/data_dictionary.md` and `docs/data_quality_report.md`. If anything differs from this table, the real data wins.

## 4. Pipeline design for a multi-table dataset
- **Primary streamed entity:** one **order event per `order_id`** from `orders`, denormalized in the ingestion step with aggregated items (count, sum price, sum freight), aggregated payments (total value, payment types), and customer city/state. Keep the join logic in `src/common/mapping.py` + `config/field_mapping.yaml`; document the join cardinality and any orders with missing child rows (report them, don't drop silently).
- Reviews, sellers, products, geolocation: loaded as **reference/dimension tables** into PostgreSQL via a batch loader (Airflow-friendly), not streamed — unless profiling shows a strong reason.
- **Record identity:** `record_uid` = hash(file_sha256 of orders file + ':' + row number) as per `04_DATA_CONTRACTS.md`; natural key `order_id` is also unique-constrained (this dataset *does* have a natural key — use it for `business_hash`/source-duplicate detection after verifying uniqueness).
- **Replay order:** sort by `order_purchase_timestamp` so the replay follows historical order flow; support `--rate` to control speed. Describe as *historical dataset replay*.
- **Kafka key:** choose after measuring. `order_id` ≈ even spread; `customer_state` gives per-state ordering but will be **skewed** (São Paulo dominates) — measure and document whichever is chosen.
- **Topic name:** rename from `food_orders` to `ecommerce_orders` (config-driven via `KAFKA_TOPIC`; update scripts/docs; 3 partitions retained). Document the rename and why in `docs/implementation_plan.md`. Do not leave "food" naming that misrepresents the data.
- **Project naming:** keep the repo/name "StreamPulse". Use subtitle **"Real-Time Order Delivery Analytics & Data Engineering Platform (Olist e-commerce dataset)"** in README/docs/dashboard. Flag in the final report that the student should confirm the retitling with the professor.

## 5. Data-quality expectations (derive rules from profiling; examples to check)
- Null delivery/approval timestamps are **valid** for statuses like canceled/unavailable/processing — don't reject them; validate consistency instead (e.g., delivered orders should have delivered date; delivered date ≥ purchase date).
- Timestamp parse rate; delivered-before-purchase anomalies.
- Orders without items / without payments; payment value ≤ 0; negative price/freight.
- Duplicate `order_id` in orders (expected none) — verify.
- Unknown `order_status` values vs the observed closed set.
- Orphan foreign keys across tables (customer_id, order_id, product_id, seller_id).
Report counts for each; reject only with documented justification.

## 6. Analytics supported by this data (build only these kinds of KPIs)
Orders per day/month; order status distribution; revenue (sum of item price + freight, or payment value — define clearly which); average order value; payment-type mix; delivery time (delivered − purchase) and late-delivery rate (delivered > estimated); orders by customer state/city; top product categories (via translation table); review score distribution (if reviews loaded). Revenue **is** supported here because payment/price columns exist — but state the definition used.

## 7. Real API options (agent must verify current docs, availability, quotas, ToS before choosing; document the decision)
Candidates tied to real columns:
1. **Currency exchange (BRL→USD) by purchase date** — e.g., Frankfurter (ECB rates, no key, supports historical time series) or Banco Central do Brasil open data. Enrichment: convert order revenue to USD using the rate on `order_purchase_timestamp` date. ~600–700 unique dates → one or a few time-series requests, cached. Strong fit and quota-friendly.
2. **BrasilAPI (IBGE)** — no key; maps `customer_state` (UF code) to state name/region and city reference data. Enrichment: region-level analytics. Very small number of calls.
3. **Open-Meteo historical weather** — no key; use lat/long of customer zip prefix (centroid from geolocation table) + purchase date to attach weather to orders. Higher volume → must sample/aggregate (e.g., per state capital per day) to respect fair-use limits.
Recommend option 1 as primary (clear business meaning: revenue in a second currency) and optionally option 2 as a small second enrichment. If the professor named an API in `docs/inputs/api_info.md`, use that instead.
Public APIs do not provide Olist orders — state this plainly.

## 8. Demo adjustments
- Demo 1: show orders table row/column counts + the join result (orders → events), reconciliation per `04_DATA_CONTRACTS.md`.
- Demo 2: real request (e.g., exchange-rate time series) → cached in `core.api_cache` → USD revenue visible in SQL and dashboard.
- Demo 3: isolated test CSV in real `orders` schema: 1 valid order, 1 invalid (e.g., delivered order with delivery date before purchase, or non-numeric payment value), 1 exact duplicate `order_id`.
- Demo 4: unchanged.

## 9. Size/performance notes
`olist_geolocation_dataset` is ~60 MB / ~1M rows: process in chunks, aggregate to one row per zip prefix before loading. Never commit any raw file; `.gitignore` must exclude `data/raw/*` (keep a `.gitkeep` and a `data/raw/README.md` telling evaluators to download from the Kaggle link and place files there).
