"""PostgreSQL warehouse tests (M6) against a REAL PostgreSQL.

They create a throwaway database `streampulse_test`, so real data is never touched.
Skipped (with the reason shown) when PostgreSQL is not reachable.
"""
from datetime import datetime

import psycopg2
import pytest

from src.warehouse import db, load

pytestmark = pytest.mark.integration
TEST_DB = "streampulse_test"


def _admin_connection():
    return psycopg2.connect(host=db.POSTGRES_HOST, port=db.POSTGRES_PORT, dbname="postgres", user=db.POSTGRES_USER,
                            password=db.POSTGRES_PASSWORD, connect_timeout=3)


@pytest.fixture(scope="module")
def test_db():
    """Create streampulse_test, apply the real schema, drop it afterwards."""
    ok, message = db.ping()
    if not ok:
        pytest.skip(f"PostgreSQL not reachable: {message}")
    original = db.POSTGRES_DB
    admin = _admin_connection()
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS {TEST_DB}")
        cur.execute(f"CREATE DATABASE {TEST_DB}")
    db.POSTGRES_DB = TEST_DB
    db.init_schema()
    yield
    db.POSTGRES_DB = original
    with admin.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS {TEST_DB}")
    admin.close()


@pytest.fixture
def clean_tables(test_db):
    with db.get_connection() as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE core.orders, core.duplicate_records")


def order_row(uid="uid-1", order_id="order-1", **changes) -> dict:
    row = {
        "record_uid": uid, "business_hash": f"hash-{order_id}", "order_id": order_id, "customer_id": "c1",
        "order_status": "delivered", "purchase_ts": datetime(2017, 10, 2, 10, 56, 33), "approved_ts": None,
        "delivered_carrier_ts": None, "delivered_customer_ts": datetime(2017, 10, 10, 21, 25, 13),
        "estimated_delivery_ts": datetime(2017, 10, 18), "customer_unique_id": "u1",
        "customer_zip_code_prefix": "01037", "customer_city": "sao paulo", "customer_state": "SP",
        "items_count": 1, "items_price_total": 100.0, "items_freight_total": 20.0, "payments_count": 1,
        "payments_value_total": 120.0, "payment_types": "credit_card", "fx_rate_brl_usd": 0.3,
        "fx_rate_date": datetime(2017, 10, 2).date(), "fx_match_type": "exact", "payments_value_usd": 36.0,
        "api_source_name": "frankfurter-ecb", "api_endpoint": "e", "api_retrieved_at": datetime(2026, 10, 15),
        "api_http_status": 200, "api_cache_key": "k", "api_enriched": True, "source_category": "kafka_replayed",
        "source_file_sha256": "abc", "source_row_number": 1, "ingest_run_id": "r1", "kafka_partition": 0,
        "kafka_offset": 0, "raw_json": '{"order_id": "order-1"}',
    }
    row.update(changes)
    return row


def run_batch(rows):
    conn = db.get_connection()
    try:
        with conn:
            return load.upsert_orders(conn, rows)
    finally:
        conn.close()


def count(sql: str) -> int:
    with db.get_connection() as conn, conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchone()[0]


def test_schema_has_the_core_tables(test_db):
    assert count("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'core'") >= 10
    db.init_schema()  # running it again must not fail


def test_first_insert_creates_one_row(clean_tables):
    result = run_batch([order_row()])
    assert result["inserted"] == 1 and result["retry_duplicates"] == 0
    assert count("SELECT count(*) FROM core.orders") == 1
    assert count("SELECT count(*) FROM core.orders WHERE raw ->> 'order_id' = 'order-1'") == 1  # jsonb works


def test_inserting_the_same_batch_twice_keeps_one_row(clean_tables):
    run_batch([order_row()])
    second = run_batch([order_row()])
    assert second["inserted"] == 0 and second["retry_duplicates"] == 1
    assert count("SELECT count(*) FROM core.orders") == 1
    assert count("SELECT count(DISTINCT record_uid) FROM core.orders") == 1


def test_same_order_with_new_record_uid_is_a_source_duplicate(clean_tables):
    run_batch([order_row(uid="uid-1")])
    result = run_batch([order_row(uid="uid-2")])           # same order_id/business_hash, different row
    assert result["inserted"] == 0 and result["source_duplicates"] == 1 and result["retry_duplicates"] == 0
    assert count("SELECT count(*) FROM core.orders") == 1
    assert count("SELECT count(*) FROM core.duplicate_records WHERE kept_record_uid = 'uid-1'") == 1


def test_source_duplicate_inside_one_batch_keeps_the_first_row(clean_tables):
    batch = [order_row(uid="uid-a", source_row_number=5), order_row(uid="uid-b", source_row_number=2)]
    result = run_batch(batch)
    assert result["inserted"] == 1 and result["source_duplicates"] == 1
    assert count("SELECT count(*) FROM core.orders WHERE record_uid = 'uid-b'") == 1   # lowest source row wins


def test_counts_always_add_up(clean_tables):
    run_batch([order_row(uid="u1", order_id="o1")])
    batch = [order_row(uid="u1", order_id="o1"),                 # retry
             order_row(uid="u9", order_id="o1"),                 # source duplicate
             order_row(uid="u2", order_id="o2"),                 # new
             order_row(uid="u3", order_id="o3")]                 # new
    r = run_batch(batch)
    assert r["attempted"] == r["inserted"] + r["retry_duplicates"] + r["source_duplicates"] == 4
    assert (r["inserted"], r["retry_duplicates"], r["source_duplicates"]) == (2, 1, 1)
