# Data Quality Report (baseline from profiling)

Source: `docs/evidence/01_dataset/dataset_profile.txt` and `join_and_mapping_check.txt` (real runs on 2026-10-15). Nothing was modified.

## 1. Headline numbers
| Check | Result |
|---|---|
| Orders in file | 99,441 |
| Duplicate `order_id` | **0** |
| Full-row duplicates (orders, items, payments, customers, products, sellers) | 0 |
| Order timestamps that fail to parse | **0** (all 5 timestamp columns) |
| Orders after join with customers/items/payments | 99,441 (no rows lost, no rows multiplied) |
| Rows rejected by the field mapping | **0** |

## 2. Findings that need a decision
| # | Finding | Count | Treatment |
|---|---|---:|---|
| 1 | Orders with **no item rows** | 775 | Keep; `items_count = 0`. Reported, not dropped. By status: unavailable 603, canceled 164, created 5, invoiced 2, shipped 1 (checked with a one-off pandas query on 2026-10-15). |
| 2 | Orders with **no payment row** | 1 | Keep; `payments_count = 0`. That order's status is `delivered`. |
| 3 | Empty `order_approved_at` / carrier date / customer delivery date | 160 / 1,783 / 2,965 | **Valid**: expected for orders not yet approved, shipped or delivered. Not rejected. |
| 4 | `order_status = delivered` but **no delivered date** | 8 | Consistency violation → rule `DELIVERED_WITHOUT_DATE` (Phase 5). |
| 5 | Status **not** delivered but a delivered date exists | 6 | Consistency violation → rule `DELIVERY_DATE_WITH_NON_DELIVERED_STATUS` (Phase 5). |
| 6 | Delivered before purchase / approved before purchase | 0 / 0 | Rule kept anyway (needed for the Demo 3 invalid example). |
| 7 | Delivered after estimated date (late) | 7,827 | **Not invalid**: this is the late-delivery KPI. |
| 8 | `payment_value` ≤ 0 | 9 (6 voucher, 3 `not_defined`) | Zero-value voucher/not_defined payments are real business cases; flagged as a warning, not rejected, in the real run. |
| 9 | `price` ≤ 0 / negative freight | 0 / 0 | Rule kept (reject negative price or freight). |
| 10 | `order_status` values | 8 (closed set) | Rule: value must be in the observed set. |
| 11 | `customer_unique_id` repeated across `customer_id` | 3,345 | Normal. Count real customers with `customer_unique_id`. |
| 12 | Duplicate `review_id` in reviews | 814 | Reference table only. Loaded with key (review_id, order_id). |
| 13 | `shipping_limit_date` up to **2020-04-09** | — | Outside the 2016–2018 range of orders; stored as given, not used in KPIs. |
| 14 | Geolocation: 261,831 exact duplicates; latitude/longitude outside Brazil (lat up to +45, lng −101 to +121) | — | Aggregate by zip prefix; outliers are ignored for region analytics and noted. |
| 15 | Products: 610 without category; 73 category names vs 71 translations | — | Show Portuguese name where no translation exists. |

## 3. Orphan keys (foreign keys that point nowhere)
| Check | Result |
|---|---:|
| orders → unknown customer_id | 0 |
| items → unknown order_id / product_id / seller_id | 0 / 0 / 0 |
| payments → unknown order_id | 0 |
| reviews → unknown order_id | 0 |
| orders without any review | 768 (not an error; reviews are optional) |

## 3b. Rejections in the real run
The quality rules reject **14 real orders** (8 `delivered` without a delivery date, 6 with a delivery date but status `canceled`). Run-level dead-letter files show more lines when runs overlap: `docs/evidence/07_postgresql/rejected_14_vs_15_explained.txt`.

## 4. What this means for the pipeline
- `order_id` is a verified natural key, so `business_hash` = hash of `order_id` and source duplicates are detected by it.
- Valid-looking replay of the full dataset should produce **0 mapping rejections**; the invalid-data demo therefore uses an isolated test file (Demo 3), not the real file.
- Quality rules are defined in Phase 5 (`config/quality_rules.yaml`) and justified from the table above.
