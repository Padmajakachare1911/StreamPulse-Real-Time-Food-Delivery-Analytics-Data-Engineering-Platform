# Data Source

| Item | Value |
|---|---|
| Name | Brazilian E-Commerce Public Dataset by Olist |
| URL | https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce |
| Owner | Olist (Kaggle user `olistbr`) |
| License | **CC BY-NC-SA 4.0** (seen on the Kaggle page, `docs/evidence/01_dataset/license_check.txt`). Attribute Olist, non-commercial use only, share-alike. Fine for coursework. |
| Downloaded | 2026-10-15 (`docs/dataset_info.md`) |
| Location | `data/raw/` (git-ignored; evaluators download it from the link above, see `data/raw/README.md`) |
| Nature | Anonymized, real, **historical** marketplace orders from Brazil, Sept 2016 – Oct 2018 |

## What it is (and is not)
- It **is** e-commerce marketplace orders with delivery tracking.
- It is **not** food-delivery or restaurant data, and it is **not** live data.
- We replay it through Kafka as a **historical dataset replay through a streaming pipeline**.

## Attribution text
> Data: "Brazilian E-Commerce Public Dataset by Olist", Olist, via Kaggle, licensed under CC BY-NC-SA 4.0. Used here unmodified for a non-commercial university project.

## Files and fingerprints
SHA-256 of each file (computed by `scripts/profile_dataset.py`, saved in `docs/evidence/01_dataset/dataset_profile.txt`):

| File | Rows | SHA-256 |
|---|---:|---|
| olist_orders_dataset.csv | 99,441 | `8df58ef3d2d7e9944010f7beecd9b75367f5588ec6e3c91cec19ae3345ef9ecf` |
| olist_order_items_dataset.csv | 112,650 | `0bc4d068c4fe38cbb01bd90e8746e3c613fe7b4baef75fab7b0e329701c3e279` |
| olist_order_payments_dataset.csv | 103,886 | `4f713964f2815dbbaa40b9488268c55aac3627bfce5aa96cf58d1f3616de3cc0` |
| olist_order_reviews_dataset.csv | 99,224 | `012b61c7593e34f51fa614efdf802b9c7056ce6aae5307ddb93236e7cfc797d7` |
| olist_customers_dataset.csv | 99,441 | `983a422239e1712ded753b3bf9ecf47dc73f144d306029dcfa99e70a226883d2` |
| olist_products_dataset.csv | 32,951 | `3e6569628a17fbc75fd206ee357b59e20364b9afa90f5b6cd5b4d624c58aa9cc` |
| olist_sellers_dataset.csv | 3,095 | `1f643d2b950373b85735e7794b20986f528d7a000432e7c6f9bcbb44d0846a0e` |
| olist_geolocation_dataset.csv | 1,000,163 | `b514f6fc991b9566aeba02aa5d67e2c3630f034b60a0e05aa0d082a3b66d88d6` |
| product_category_name_translation.csv | 71 | `a81f0d1f27b27e7293f761bc79e3ce8f348ee39c4b3ed3e49bde38f478586278` |

The hash of the **orders** file feeds every `record_uid`, so replaying the same file always gives the same IDs.

## Domain fit decision
The streamed entity is one **order event** per `order_id` (99,441 orders ≈ the "~100,000 records" requirement). It is built from `orders` joined with customers, aggregated items and aggregated payments. Reviews, sellers, products and geolocation are reference tables loaded in batch.
