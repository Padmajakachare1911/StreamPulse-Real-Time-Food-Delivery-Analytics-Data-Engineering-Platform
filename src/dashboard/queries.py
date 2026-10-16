"""Database access for the dashboard. No Streamlit code here, so it can be tested alone.

Every number on the dashboard comes from one of these functions (PostgreSQL views/tables or Parquet).
Errors are logged in full for the developer but only a short, safe message reaches the user: no
connection strings, passwords or internal exception texts.
"""
import logging
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pyarrow.dataset as ds

from src.common.config import PARQUET_OUTPUT_DIR
from src.warehouse.db import get_connection

log = logging.getLogger("dashboard.queries")


class QueryError(Exception):
    """Raised with a message that is safe to show to dashboard users."""


def numeric_columns_to_float(frame: pd.DataFrame) -> pd.DataFrame:
    """PostgreSQL NUMERIC arrives as Python Decimal; charts would treat it as text labels. Make it float."""
    for column in frame.columns:
        if frame[column].dtype == object and frame[column].map(lambda v: isinstance(v, Decimal)).any():
            frame[column] = frame[column].astype(float)
    return frame


def run_query(sql: str, params: tuple | None = None) -> pd.DataFrame:
    """Run a read-only query with bound parameters and return a DataFrame."""
    try:
        conn = get_connection(connect_timeout=3)
        try:
            with conn, conn.cursor() as cur:
                cur.execute(sql, params)
                columns = [c.name for c in cur.description]
                return numeric_columns_to_float(pd.DataFrame(cur.fetchall(), columns=columns))
        finally:
            conn.close()
    except Exception as exc:
        log.error("query failed: %s: %s", type(exc).__name__, str(exc).splitlines()[0] if str(exc) else "")
        raise QueryError("The database is unavailable or the query failed.") from None


# ---- warehouse overview ----------------------------------------------------------------------
def overview() -> pd.DataFrame:
    return run_query("""
        SELECT count(*) AS orders, count(DISTINCT record_uid) AS distinct_record_uid,
               count(DISTINCT order_id) AS distinct_order_id, min(purchase_ts) AS first_purchase,
               max(purchase_ts) AS last_purchase, max(processed_at) AS last_processed_at
        FROM core.orders""")


def source_files() -> pd.DataFrame:
    return run_query("SELECT source_file_sha256, count(*) AS orders FROM core.orders GROUP BY 1 ORDER BY 2 DESC")


def reference_table_counts() -> pd.DataFrame:
    return run_query("""
        SELECT 'order items' AS table_name, count(*) AS rows FROM core.order_items
        UNION ALL SELECT 'products', count(*) FROM core.dim_products
        UNION ALL SELECT 'sellers', count(*) FROM core.dim_sellers
        UNION ALL SELECT 'reviews', count(*) FROM core.order_reviews
        UNION ALL SELECT 'geolocation zip prefixes', count(*) FROM core.geo_zip""")


# ---- ingestion and quality -------------------------------------------------------------------
def ingestion_runs() -> pd.DataFrame:
    return run_query("""
        SELECT ingest_run_id AS run, mode, started_at, rows_read, rows_valid, rows_rejected, rows_published,
               rows_publish_failed FROM core.ingestion_runs ORDER BY started_at""")


def dead_letter_summary() -> pd.DataFrame:
    return run_query("""
        SELECT stage, reason_code, count(*) AS records, count(DISTINCT record_uid) AS distinct_records
        FROM core.dead_letter_records GROUP BY 1, 2 ORDER BY 1, 2""")


def duplicate_summary() -> pd.DataFrame:
    return run_query("SELECT duplicate_type, count(*) AS records FROM core.duplicate_records GROUP BY 1")


def completeness() -> pd.DataFrame:
    return run_query("""
        SELECT count(*) FILTER (WHERE items_count = 0)    AS orders_without_items,
               count(*) FILTER (WHERE payments_count = 0) AS orders_without_payments,
               count(*) FILTER (WHERE order_status = 'delivered' AND delivered_customer_ts IS NULL) AS delivered_without_date,
               count(*) FILTER (WHERE delivered_customer_ts < purchase_ts) AS delivered_before_purchase
        FROM core.orders""")


# ---- KPIs (the analytics views) --------------------------------------------------------------
def revenue_summary() -> pd.DataFrame:
    return run_query("SELECT * FROM analytics.revenue_summary")


def status_distribution() -> pd.DataFrame:
    return run_query("SELECT * FROM analytics.order_status_distribution ORDER BY orders DESC")


def orders_per_month() -> pd.DataFrame:
    return run_query("SELECT * FROM analytics.orders_per_month ORDER BY order_month")


def late_delivery() -> pd.DataFrame:
    return run_query("SELECT * FROM analytics.late_delivery_summary")


def delivery_time_by_month() -> pd.DataFrame:
    return run_query("SELECT * FROM analytics.delivery_time_by_month ORDER BY order_month")


def orders_by_state(limit: int = 10) -> pd.DataFrame:
    return run_query("SELECT * FROM analytics.orders_by_state ORDER BY orders DESC LIMIT %s", (limit,))


def payment_mix() -> pd.DataFrame:
    return run_query("SELECT * FROM analytics.payment_type_mix ORDER BY orders_using_type DESC")


def top_categories(limit: int = 10) -> pd.DataFrame:
    return run_query("SELECT * FROM analytics.top_product_categories ORDER BY items_sold DESC LIMIT %s", (limit,))


def review_scores() -> pd.DataFrame:
    return run_query("SELECT * FROM analytics.review_score_distribution ORDER BY review_score")


# ---- API ---------------------------------------------------------------------------------------
def api_enrichment() -> pd.DataFrame:
    return run_query("SELECT * FROM analytics.api_enrichment_summary")


def fx_rates() -> pd.DataFrame:
    return run_query("SELECT rate_date, rate FROM core.fx_rates WHERE base_currency = 'BRL' AND quote_currency = 'USD' ORDER BY rate_date")


def api_cache_last_success() -> pd.DataFrame:
    return run_query("""
        SELECT max(retrieved_at) AS last_success, count(*) AS cached_responses
        FROM core.api_cache WHERE http_status BETWEEN 200 AND 299""")


# ---- recent records (business columns only: no customer identifiers) ---------------------------
def recent_orders(limit: int = 20) -> pd.DataFrame:
    return run_query("""
        SELECT order_id, source_category AS source, order_status, purchase_ts, customer_state, items_count,
               payments_value_total, payments_value_usd, ingest_run_id, processed_at
        FROM core.orders ORDER BY processed_at DESC, purchase_ts DESC LIMIT %s""", (limit,))


def recent_demo_orders(limit: int = 10) -> pd.DataFrame:
    """Orders placed from the dashboard form (never historical Olist rows)."""
    return run_query("""
        SELECT order_id, order_status, customer_state, items_count, payments_value_total, payments_value_usd,
               kafka_partition, kafka_offset, purchase_ts, processed_at
        FROM core.orders WHERE source_category = 'user_demo' ORDER BY processed_at DESC LIMIT %s""", (limit,))


def demo_order_count() -> pd.DataFrame:
    return run_query("SELECT count(*) AS demo_orders FROM core.orders WHERE source_category = 'user_demo'")


# ---- Parquet landing layer ---------------------------------------------------------------------
def parquet_counts(parquet_dir: Path = PARQUET_OUTPUT_DIR) -> dict:
    """{'files': n, 'rows': n, 'distinct_record_uid': n}. Raises QueryError if no Parquet exists."""
    files = list(Path(parquet_dir).rglob("*.parquet"))
    if not files:
        raise QueryError("No Parquet files found yet.")
    try:
        uids = ds.dataset(str(parquet_dir), format="parquet", partitioning="hive").to_table(columns=["record_uid"]).column("record_uid").to_pylist()
    except Exception as exc:
        log.error("parquet read failed: %s", type(exc).__name__)
        raise QueryError("The Parquet files could not be read.") from None
    return {"files": len(files), "rows": len(uids), "distinct_record_uid": len(set(uids))}
