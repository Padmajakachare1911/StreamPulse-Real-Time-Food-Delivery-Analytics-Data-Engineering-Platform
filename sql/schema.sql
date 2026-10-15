-- StreamPulse warehouse schema. Safe to run many times (everything is IF NOT EXISTS).
-- Columns come from config/field_mapping.yaml (real Olist columns). Run: .\scripts\init_db.ps1

CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS analytics;

-- One row per order event. Renamed from the skeleton name core.food_orders: the data is e-commerce.
CREATE TABLE IF NOT EXISTS core.orders (
    record_uid              TEXT PRIMARY KEY,          -- same file + same row => same ID (retry/replay safe)
    business_hash           TEXT NOT NULL,             -- hash of the natural key order_id
    order_id                TEXT NOT NULL UNIQUE,      -- verified natural key: one row per order
    customer_id             TEXT NOT NULL,
    order_status            TEXT NOT NULL,
    purchase_ts             TIMESTAMP NOT NULL,        -- source has no time zone: naive local Brazil time
    approved_ts             TIMESTAMP,
    delivered_carrier_ts    TIMESTAMP,
    delivered_customer_ts   TIMESTAMP,                 -- empty is normal for orders not delivered yet
    estimated_delivery_ts   TIMESTAMP NOT NULL,
    customer_unique_id      TEXT,
    customer_zip_code_prefix TEXT,                     -- text: keeps leading zeros
    customer_city           TEXT,
    customer_state          TEXT,
    items_count             INTEGER NOT NULL,          -- 0 when the order has no item rows
    items_price_total       NUMERIC(12,2) NOT NULL,
    items_freight_total     NUMERIC(12,2) NOT NULL,
    payments_count          INTEGER NOT NULL,          -- 0 when the order has no payment row
    payments_value_total    NUMERIC(12,2) NOT NULL,    -- "revenue" in the dashboard
    payment_types           TEXT,
    -- API enrichment (Frankfurter / ECB): null when no rate was found
    fx_rate_brl_usd         NUMERIC(12,6),
    fx_rate_date            DATE,
    fx_match_type           TEXT,
    payments_value_usd      NUMERIC(12,2),
    api_source_name         TEXT,
    api_endpoint            TEXT,
    api_retrieved_at        TIMESTAMPTZ,
    api_http_status         INTEGER,
    api_cache_key           TEXT,
    api_enriched            BOOLEAN NOT NULL DEFAULT FALSE,
    -- provenance
    source_category         TEXT NOT NULL,
    source_file_sha256      TEXT NOT NULL,
    source_row_number       BIGINT NOT NULL,
    ingest_run_id           TEXT NOT NULL,
    kafka_partition         INTEGER,
    kafka_offset            BIGINT,
    raw                     JSONB,                     -- original source columns
    processed_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_orders_purchase_ts ON core.orders (purchase_ts);       -- time-series KPIs
CREATE INDEX IF NOT EXISTS idx_orders_state       ON core.orders (customer_state);    -- state KPIs
CREATE INDEX IF NOT EXISTS idx_orders_status      ON core.orders (order_status);      -- status mix

-- Same business content under a different record_uid: a genuine SOURCE duplicate. The first row is kept.
CREATE TABLE IF NOT EXISTS core.duplicate_records (
    id                  BIGSERIAL PRIMARY KEY,
    record_uid          TEXT NOT NULL,                 -- the discarded copy
    kept_record_uid     TEXT,                          -- the row that was kept
    business_hash       TEXT NOT NULL,
    duplicate_type      TEXT NOT NULL,                 -- source_duplicate | retry_duplicate
    ingest_run_id       TEXT,
    detected_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS core.dead_letter_records (
    dlq_id          UUID PRIMARY KEY,
    stage           TEXT,
    reason_code     TEXT,
    reason_detail   TEXT,
    record_uid      TEXT,
    ingest_run_id   TEXT,
    raw_payload     JSONB,
    rejected_at     TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS core.ingestion_runs (
    ingest_run_id       TEXT PRIMARY KEY,
    started_at          TIMESTAMPTZ,
    finished_at         TIMESTAMPTZ,
    source_file         TEXT,
    source_file_sha256  TEXT,
    rows_read           BIGINT,
    rows_valid          BIGINT,
    rows_rejected       BIGINT,
    rows_published      BIGINT,
    rows_publish_failed BIGINT,
    mode                TEXT
);

-- Raw API responses (one row per request) with provenance.
CREATE TABLE IF NOT EXISTS core.api_cache (
    cache_key       TEXT PRIMARY KEY,
    api_source_name TEXT,
    api_endpoint    TEXT,
    response_json   JSONB,
    http_status     INTEGER,
    retrieved_at    TIMESTAMPTZ
);

-- Daily BRL -> USD rates parsed from those responses.
CREATE TABLE IF NOT EXISTS core.fx_rates (
    rate_date       DATE NOT NULL,
    base_currency   TEXT NOT NULL,
    quote_currency  TEXT NOT NULL,
    rate            NUMERIC(12,6) NOT NULL,
    api_source_name TEXT NOT NULL,
    api_cache_key   TEXT NOT NULL,
    PRIMARY KEY (rate_date, base_currency, quote_currency)
);

-- Reference (dimension) tables, loaded in batch by src/warehouse/load_reference.py.
CREATE TABLE IF NOT EXISTS core.dim_products (
    product_id                  TEXT PRIMARY KEY,
    product_category_name       TEXT,
    product_category_name_english TEXT,                -- from the translation table; null if untranslated
    product_name_lenght         INTEGER,               -- spelled like this in the source file
    product_description_lenght  INTEGER,
    product_photos_qty          INTEGER,
    product_weight_g            INTEGER,
    product_length_cm           INTEGER,
    product_height_cm           INTEGER,
    product_width_cm            INTEGER
);

CREATE TABLE IF NOT EXISTS core.dim_sellers (
    seller_id               TEXT PRIMARY KEY,
    seller_zip_code_prefix  TEXT,
    seller_city             TEXT,
    seller_state            TEXT
);

CREATE TABLE IF NOT EXISTS core.order_items (
    order_id            TEXT NOT NULL,
    order_item_id       INTEGER NOT NULL,
    product_id          TEXT NOT NULL,
    seller_id           TEXT NOT NULL,
    shipping_limit_date TIMESTAMP,
    price               NUMERIC(12,2) NOT NULL,
    freight_value       NUMERIC(12,2) NOT NULL,
    PRIMARY KEY (order_id, order_item_id)
);

CREATE TABLE IF NOT EXISTS core.order_reviews (
    review_id               TEXT NOT NULL,
    order_id                TEXT NOT NULL,
    review_score            INTEGER,
    review_comment_title    TEXT,
    review_comment_message  TEXT,
    review_creation_date    TIMESTAMP,
    review_answer_timestamp TIMESTAMP,
    PRIMARY KEY (review_id, order_id)                  -- review_id alone repeats in the real file
);

-- One centroid per zip prefix (the raw file has ~1,000,000 rows with many duplicates).
CREATE TABLE IF NOT EXISTS core.geo_zip (
    zip_code_prefix TEXT PRIMARY KEY,
    lat             DOUBLE PRECISION,
    lng             DOUBLE PRECISION,
    city            TEXT,
    state           TEXT,
    source_rows     INTEGER
);
