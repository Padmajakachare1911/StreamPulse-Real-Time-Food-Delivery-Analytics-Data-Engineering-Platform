"""Write one Spark micro-batch into PostgreSQL, safely and repeatably.

How duplicates are classified (docs/03 M5, docs/04 section 2):
  retry duplicate  : record_uid already known (stored in core.orders, or already logged in
                     core.duplicate_records as a discarded source duplicate)
                     -> nothing is written, only counted
  source duplicate : different record_uid, same business_hash (same order_id)
                     -> first copy kept, the other is logged in core.duplicate_records
  new record       : inserted

The whole batch runs in ONE transaction, so a crash never leaves half a batch behind,
and running the same batch again changes nothing (idempotent sink).
"""
import logging

from psycopg2.extras import execute_values

from src.warehouse.db import get_connection

log = logging.getLogger(__name__)

# Columns of core.orders that a Spark row provides (processed_at has a default).
ORDER_COLUMNS = [
    "record_uid", "business_hash", "order_id", "customer_id", "order_status", "purchase_ts", "approved_ts",
    "delivered_carrier_ts", "delivered_customer_ts", "estimated_delivery_ts", "customer_unique_id",
    "customer_zip_code_prefix", "customer_city", "customer_state", "items_count", "items_price_total",
    "items_freight_total", "payments_count", "payments_value_total", "payment_types", "fx_rate_brl_usd",
    "fx_rate_date", "fx_match_type", "payments_value_usd", "api_source_name", "api_endpoint",
    "api_retrieved_at", "api_http_status", "api_cache_key", "api_enriched", "source_category",
    "source_file_sha256", "source_row_number", "ingest_run_id", "kafka_partition", "kafka_offset", "raw",
]
COLUMN_LIST = ", ".join(ORDER_COLUMNS)


def _row_values(row: dict) -> tuple:
    """Order the values like ORDER_COLUMNS. Spark calls the raw JSON column `raw_json`."""
    return tuple(row["raw_json"] if col == "raw" else row[col] for col in ORDER_COLUMNS)


def upsert_orders(conn, rows: list[dict]) -> dict:
    """Insert new orders, count retries, log source duplicates. Caller commits."""
    result = {"attempted": len(rows), "inserted": 0, "retry_duplicates": 0, "source_duplicates": 0}
    if not rows:
        return result
    with conn.cursor() as cur:
        # Staging table: same columns as core.orders, no unique constraints, vanishes at COMMIT.
        cur.execute("CREATE TEMP TABLE staging_orders (LIKE core.orders INCLUDING DEFAULTS) ON COMMIT DROP")
        execute_values(cur, f"INSERT INTO staging_orders ({COLUMN_LIST}) VALUES %s",
                       [_row_values(r) for r in rows], page_size=1000)

        # Retry duplicates: this exact record_uid was seen before, either stored or already logged
        # as a discarded source duplicate (so re-ingesting a file never logs the same copy twice).
        cur.execute("""
            CREATE TEMP TABLE known_uids ON COMMIT DROP AS
            SELECT s.record_uid FROM staging_orders s
            WHERE EXISTS (SELECT 1 FROM core.orders o WHERE o.record_uid = s.record_uid)
               OR EXISTS (SELECT 1 FROM core.duplicate_records d WHERE d.record_uid = s.record_uid)
        """)
        cur.execute("SELECT count(*) FROM known_uids")
        result["retry_duplicates"] = cur.fetchone()[0]

        # Candidates = not a retry. Among them, the first copy per business_hash wins (lowest source row).
        cur.execute("""
            CREATE TEMP TABLE fresh_orders ON COMMIT DROP AS
            SELECT s.*, row_number() OVER (PARTITION BY s.business_hash ORDER BY s.source_row_number, s.record_uid) AS rn
            FROM staging_orders s
            WHERE s.record_uid NOT IN (SELECT record_uid FROM known_uids)
        """)

        # Source duplicates: a later copy in this batch, or the business_hash is already stored.
        cur.execute("""
            INSERT INTO core.duplicate_records (record_uid, kept_record_uid, business_hash, duplicate_type, ingest_run_id)
            SELECT f.record_uid,
                   COALESCE((SELECT o.record_uid FROM core.orders o WHERE o.business_hash = f.business_hash),
                            (SELECT f2.record_uid FROM fresh_orders f2 WHERE f2.business_hash = f.business_hash AND f2.rn = 1)),
                   f.business_hash, 'source_duplicate', f.ingest_run_id
            FROM fresh_orders f
            WHERE f.rn > 1 OR EXISTS (SELECT 1 FROM core.orders o WHERE o.business_hash = f.business_hash)
        """)
        result["source_duplicates"] = cur.rowcount

        cur.execute(f"""
            INSERT INTO core.orders ({COLUMN_LIST})
            SELECT {COLUMN_LIST} FROM fresh_orders f
            WHERE f.rn = 1 AND NOT EXISTS (SELECT 1 FROM core.orders o WHERE o.business_hash = f.business_hash)
            ON CONFLICT DO NOTHING
        """)
        result["inserted"] = cur.rowcount
    return result


def write_batch(spark_rows, batch_id: int) -> dict:
    """Called by the Spark job for every micro-batch (see order_stream.py)."""
    rows = [r.asDict() for r in spark_rows]
    conn = get_connection()
    try:
        with conn:  # one transaction: commit on success, roll back on error
            result = upsert_orders(conn, rows)
    finally:
        conn.close()
    result["batch_id"] = batch_id
    accounted = result["inserted"] + result["retry_duplicates"] + result["source_duplicates"]
    if accounted != result["attempted"]:  # should never happen; make it loud if it does
        log.error("PostgreSQL batch %s: %d rows attempted but only %d accounted for", batch_id, result["attempted"], accounted)
    return result
