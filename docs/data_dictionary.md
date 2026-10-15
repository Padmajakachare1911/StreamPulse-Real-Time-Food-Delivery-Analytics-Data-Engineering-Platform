# Data Dictionary

All columns below were read from the real files (`docs/evidence/01_dataset/dataset_profile.txt`). Meanings follow the Kaggle documentation; counts are from our profiling run.
All CSV values are read as text first and converted by `config/field_mapping.yaml`.

## 1. Source tables (9 files)

### olist_orders_dataset.csv — 99,441 rows, 8 columns (one row per order)
| Column | Meaning | Empty values |
|---|---|---:|
| order_id | Unique order key (99,441 distinct, no duplicates) | 0 |
| customer_id | Key into customers (one per order) | 0 |
| order_status | One of 8 values: delivered 96,478 · shipped 1,107 · canceled 625 · unavailable 609 · invoiced 314 · processing 301 · created 5 · approved 2 | 0 |
| order_purchase_timestamp | When the order was placed | 0 |
| order_approved_at | Payment approval time | 160 |
| order_delivered_carrier_date | Handed to logistics partner | 1,783 |
| order_delivered_customer_date | Delivered to customer | 2,965 |
| order_estimated_delivery_date | Promised delivery date | 0 |

### olist_order_items_dataset.csv — 112,650 rows, 7 columns (key: order_id + order_item_id, unique)
`order_id, order_item_id, product_id, seller_id, shipping_limit_date, price, freight_value`. `price` ranges 0.85 – 6,735.00; `freight_value` 0.00 – 409.68 (383 zero-freight rows).

### olist_order_payments_dataset.csv — 103,886 rows, 5 columns (key: order_id + payment_sequential, unique)
`order_id, payment_sequential, payment_type, payment_installments, payment_value`. `payment_type`: credit_card 76,795 · boleto 19,784 · voucher 5,775 · debit_card 1,529 · not_defined 3. `payment_value` 0.00 – 13,664.08 (9 rows equal 0).

### olist_order_reviews_dataset.csv — 99,224 rows, 7 columns
`review_id, order_id, review_score, review_comment_title, review_comment_message, review_creation_date, review_answer_timestamp`. `review_id` is **not** unique (98,410 distinct; 814 repeated rows). Comment title empty in 87,656 rows, message empty in 58,247 rows (normal, comments are optional).

### olist_customers_dataset.csv — 99,441 rows, 5 columns
`customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state`. `customer_id` is unique (one per order). `customer_unique_id` repeats 3,345 times, so 96,096 real distinct customers. Zip prefix is text and keeps leading zeros (e.g. `01037`).

### olist_products_dataset.csv — 32,951 rows, 9 columns
`product_id, product_category_name, product_name_lenght, product_description_lenght, product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm`. The misspelling "lenght" is in the real file and is kept. 610 products have no category.

### olist_sellers_dataset.csv — 3,095 rows, 4 columns
`seller_id, seller_zip_code_prefix, seller_city, seller_state`.

### product_category_name_translation.csv — 71 rows, 2 columns
`product_category_name, product_category_name_english`. Products use 73 distinct category names, the translation covers 71, so some categories show their Portuguese name in analytics.

### olist_geolocation_dataset.csv — 1,000,163 rows, 5 columns
`geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, geolocation_city, geolocation_state`. 261,831 rows are exact duplicates; many rows per zip prefix. Reference table: aggregated to one centroid per zip prefix before loading.

## 2. Streamed entity: the order event
One event per `order_id` (99,441 events), mapped by `config/field_mapping.yaml`:

| Canonical field | Type | Required | Source |
|---|---|:-:|---|
| order_id | string | yes | orders.order_id |
| customer_id | string | yes | orders.customer_id |
| order_status | string | yes | orders.order_status |
| purchase_ts | timestamp | yes | orders.order_purchase_timestamp |
| approved_ts | timestamp | no | orders.order_approved_at |
| delivered_carrier_ts | timestamp | no | orders.order_delivered_carrier_date |
| delivered_customer_ts | timestamp | no | orders.order_delivered_customer_date |
| estimated_delivery_ts | timestamp | yes | orders.order_estimated_delivery_date |
| customer_unique_id, customer_zip_code_prefix, customer_city, customer_state | string | no | customers (join on customer_id) |
| items_count, items_price_total, items_freight_total | int / float | yes | order_items aggregated per order (0 when no items) |
| payments_count, payments_value_total, payment_types | int / float / string | yes / yes / no | order_payments aggregated per order (0 when no payments) |

Join check: 99,441 orders in, 99,441 order rows out. 775 orders have no item rows and 1 order has no payment row; they are kept with counts of 0 (`docs/evidence/01_dataset/join_and_mapping_check.txt`).

## 3. Definitions used in analytics
- **Revenue** = `payments_value_total` (money the customer paid, includes freight and vouchers). `items_price_total` is the product value without freight. The dashboard always states which one it shows.
- **Delivery time** = `delivered_customer_ts − purchase_ts`, only for orders with a delivered date.
- **Late delivery** = `delivered_customer_ts > estimated_delivery_ts`.
- Time stamps have **no time zone** in the source; they are treated as local Brazilian time.
