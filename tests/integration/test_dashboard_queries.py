"""Dashboard query functions against a REAL PostgreSQL (throwaway database streampulse_dash_test)."""
from datetime import datetime

import psycopg2
import pytest

from src.dashboard import queries
from src.warehouse import db, load

pytestmark = pytest.mark.integration
TEST_DB = "streampulse_dash_test"


@pytest.fixture(scope="module")
def dash_db():
    ok, message = db.ping()
    if not ok:
        pytest.skip(f"PostgreSQL not reachable: {message}")
    original = db.POSTGRES_DB
    admin = psycopg2.connect(host=db.POSTGRES_HOST, port=db.POSTGRES_PORT, dbname="postgres", user=db.POSTGRES_USER,
                             password=db.POSTGRES_PASSWORD, connect_timeout=3)
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS {TEST_DB} WITH (FORCE)")
        cur.execute(f"CREATE DATABASE {TEST_DB}")
    db.POSTGRES_DB = TEST_DB
    db.init_schema()
    yield
    db.POSTGRES_DB = original
    with admin.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS {TEST_DB} WITH (FORCE)")
    admin.close()


def order(uid, order_id, status="delivered", delivered=datetime(2017, 10, 10), estimated=datetime(2017, 10, 18), value=120.0):
    return {
        "record_uid": uid, "business_hash": f"h-{order_id}", "order_id": order_id, "customer_id": "c1", "order_status": status,
        "purchase_ts": datetime(2017, 10, 2, 10, 0), "approved_ts": None, "delivered_carrier_ts": None, "delivered_customer_ts": delivered,
        "estimated_delivery_ts": estimated, "customer_unique_id": "u-secret", "customer_zip_code_prefix": "01037", "customer_city": "sao paulo",
        "customer_state": "SP", "items_count": 1, "items_price_total": 100.0, "items_freight_total": 20.0, "payments_count": 1,
        "payments_value_total": value, "payment_types": "credit_card", "fx_rate_brl_usd": 0.3, "fx_rate_date": datetime(2017, 10, 2).date(),
        "fx_match_type": "exact", "payments_value_usd": value * 0.3, "api_source_name": "frankfurter-ecb", "api_endpoint": "e",
        "api_retrieved_at": datetime(2026, 10, 15), "api_http_status": 200, "api_cache_key": "k", "api_enriched": True,
        "source_category": "kafka_replayed", "source_file_sha256": "abc", "source_row_number": 1, "ingest_run_id": "r1",
        "kafka_partition": 0, "kafka_offset": 0, "raw_json": "{}",
    }


def insert(rows):
    conn = db.get_connection()
    with conn:
        load.upsert_orders(conn, rows)
    conn.close()


def test_queries_on_an_empty_warehouse_return_empty_results_not_errors(dash_db):
    assert int(queries.overview().iloc[0]["orders"]) == 0
    assert queries.recent_orders().empty
    assert queries.status_distribution().empty
    assert queries.dead_letter_summary().empty


def test_overview_kpis_and_recent_orders_reflect_the_stored_rows(dash_db):
    insert([order("u1", "o1"), order("u2", "o2", status="shipped", delivered=None),
            order("u3", "o3", delivered=datetime(2017, 10, 25))])           # o3 is late
    overview = queries.overview().iloc[0]
    assert int(overview["orders"]) == 3 and int(overview["distinct_order_id"]) == 3
    revenue = queries.revenue_summary().iloc[0]
    assert float(revenue["revenue_brl"]) == pytest.approx(360.0)
    late = queries.late_delivery().iloc[0]
    assert int(late["delivered_orders"]) == 2 and int(late["late_orders"]) == 1
    statuses = queries.status_distribution().set_index("order_status")["orders"]
    assert int(statuses["delivered"]) == 2 and int(statuses["shipped"]) == 1
    assert queries.api_enrichment().iloc[0]["orders_enriched"] == 3


def test_recent_orders_never_exposes_customer_identifiers(dash_db):
    recent = queries.recent_orders(5)
    assert not recent.empty
    assert "customer_id" not in recent.columns and "customer_unique_id" not in recent.columns
    assert "u-secret" not in recent.to_string()


def test_completeness_counts_problem_rows(dash_db):
    comp = queries.completeness().iloc[0]
    assert int(comp["delivered_before_purchase"]) == 0 and int(comp["delivered_without_date"]) == 0


def test_limit_parameter_is_bound_not_formatted(dash_db):
    assert len(queries.recent_orders(2)) == 2
    assert len(queries.orders_by_state(1)) == 1


# ---- real Kafka: a positive test, so the "down" unit tests cannot pass for the wrong reason ----
def test_kafka_check_is_ok_against_the_real_broker():
    from src.dashboard import status
    result = status.check_kafka()
    if result.state == "down" and "not reachable" in result.detail:
        pytest.skip("Kafka broker is not running")
    assert result.state == "ok", result
    assert status.kafka_topic_messages() is not None and status.kafka_topic_messages() > 0


def test_numeric_view_columns_are_floats_not_decimals(dash_db):
    delivery = queries.delivery_time_by_month()
    assert not delivery.empty and delivery["avg_delivery_days"].dtype == float
    assert queries.revenue_summary()["revenue_brl"].dtype == float


# ---- user-submitted demo orders (kept last: they add rows to the shared throwaway database) ----------------------
def test_demo_orders_are_listed_separately_and_excluded_from_historical_kpis(dash_db):
    before = int(queries.revenue_summary().iloc[0]["orders"])
    revenue_before = float(queries.revenue_summary().iloc[0]["revenue_brl"])
    demo = order("u-demo", "demo-abc", status="approved", delivered=None, value=500.0)
    demo.update(source_category="user_demo", ingest_run_id="demo-order-x", customer_id="demo-customer-1")
    insert([demo])
    assert int(queries.revenue_summary().iloc[0]["orders"]) == before                 # historical KPIs unchanged
    assert float(queries.revenue_summary().iloc[0]["revenue_brl"]) == pytest.approx(revenue_before)   # the 500 BRL demo order is not counted
    assert int(queries.overview().iloc[0]["orders"]) == before + 1                      # pipeline accounting still sees every row
    assert list(queries.recent_demo_orders()["order_id"]) == ["demo-abc"]
    assert int(queries.demo_order_count().iloc[0]["demo_orders"]) == 1
    recent = queries.recent_orders(50).set_index("order_id")["source"]
    assert recent["demo-abc"] == "user_demo" and recent["o1"] == "kafka_replayed"
    states = queries.status_distribution().set_index("order_status")["orders"]
    assert "approved" not in states.index                                              # the demo order's status is not counted
