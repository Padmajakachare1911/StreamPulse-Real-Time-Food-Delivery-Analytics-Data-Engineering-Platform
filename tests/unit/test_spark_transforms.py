"""Spark transform tests (M4) on small static DataFrames.

Marker `spark`: PySpark cannot start on this Windows host (winutils.exe is missing, see
docs/baseline_audit.md), so these run inside the Spark container:
    .\\scripts\\run_tests_docker.ps1
"""
import json

import pytest

from src.common.config import FIELD_MAPPING_FILE
from src.common.mapping import load_mapping

pytestmark = pytest.mark.spark
pyspark = pytest.importorskip("pyspark")

from src.streaming.transforms import (MALFORMED_JSON, SCHEMA_MISMATCH, dedup_in_batch,  # noqa: E402
                                      flatten_valid, parse_messages, split_valid_invalid)


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    session = (SparkSession.builder.master("local[1]").appName("transform-tests")
               .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2").getOrCreate())
    yield session
    session.stop()


@pytest.fixture(scope="module")
def mapping():
    return load_mapping(FIELD_MAPPING_FILE)


def envelope(uid="uid1", order_id="o1", enriched=True, **payload_changes) -> dict:
    payload = {
        "order_id": order_id, "customer_id": "c1", "order_status": "delivered",
        "purchase_ts": "2017-10-02T10:56:33", "approved_ts": "2017-10-02T11:07:15",
        "delivered_carrier_ts": "2017-10-04T19:55:00", "delivered_customer_ts": "2017-10-10T21:25:13",
        "estimated_delivery_ts": "2017-10-18T00:00:00", "customer_unique_id": "u1",
        "customer_zip_code_prefix": "01037", "customer_city": "sao paulo", "customer_state": "SP",
        "items_count": 2, "items_price_total": 100.5, "items_freight_total": 20.0,
        "payments_count": 1, "payments_value_total": 120.5, "payment_types": "credit_card",
    } | payload_changes
    return {
        "schema_version": "1.0", "record_uid": uid, "business_hash": "bh1", "source_category": "kafka_replayed",
        "source_file": "orders.csv", "source_file_sha256": "abc", "source_row_number": 7, "ingest_run_id": "r1",
        "ingested_at": "2026-10-15T16:00:00+00:00", "payload": payload, "raw": {"order_id": order_id},
        "api_enrichment": ({"fx_rate_brl_usd": 0.3, "fx_rate_date": "2017-10-02", "fx_match_type": "exact",
                            "payments_value_usd": 36.15, "api_source_name": "frankfurter-ecb", "api_endpoint": "e",
                            "api_retrieved_at": "2026-10-15T16:00:00+00:00", "api_http_status": 200,
                            "api_cache_key": "k"} if enriched else None),
    }


def kafka_df(spark, messages: list):
    """Rows shaped like Spark's Kafka source: partition, offset, timestamp, value (bytes)."""
    rows = [(p, o, None, (m if isinstance(m, str) else json.dumps(m)).encode("utf-8")) for p, o, m in messages]
    return spark.createDataFrame(rows, "partition int, offset long, timestamp timestamp, value binary")


def test_valid_message_is_parsed_and_flattened_with_types(spark, mapping):
    df = kafka_df(spark, [(0, 5, envelope())])
    valid, invalid = split_valid_invalid(parse_messages(df, mapping))
    assert invalid.count() == 0
    row = flatten_valid(valid, mapping).collect()[0].asDict()
    assert row["record_uid"] == "uid1" and row["order_id"] == "o1"
    assert row["purchase_ts"].year == 2017          # text -> timestamp
    assert row["items_count"] == 2 and row["payments_value_total"] == 120.5
    assert row["api_enriched"] is True and row["fx_rate_brl_usd"] == 0.3
    assert row["kafka_partition"] == 0 and row["kafka_offset"] == 5
    assert json.loads(row["raw_json"]) == {"order_id": "o1"}   # original columns kept as JSON text


def test_missing_enrichment_gives_api_enriched_false(spark, mapping):
    df = kafka_df(spark, [(0, 1, envelope(enriched=False))])
    valid, _ = split_valid_invalid(parse_messages(df, mapping))
    row = flatten_valid(valid, mapping).collect()[0]
    assert row["api_enriched"] is False and row["fx_rate_brl_usd"] is None


def test_malformed_json_is_routed_to_invalid_not_crashing(spark, mapping):
    df = kafka_df(spark, [(0, 1, "this is {not json"), (0, 2, envelope())])
    valid, invalid = split_valid_invalid(parse_messages(df, mapping))
    assert valid.count() == 1
    assert [r["reason_code"] for r in invalid.collect()] == [MALFORMED_JSON]


def test_message_without_record_uid_is_schema_mismatch(spark, mapping):
    broken = envelope()
    del broken["record_uid"]
    df = kafka_df(spark, [(1, 9, broken)])
    _, invalid = split_valid_invalid(parse_messages(df, mapping))
    assert [r["reason_code"] for r in invalid.collect()] == [SCHEMA_MISMATCH]


def test_in_batch_dedup_keeps_lowest_offset(spark, mapping):
    df = kafka_df(spark, [(0, 8, envelope(uid="same")), (0, 3, envelope(uid="same")), (1, 1, envelope(uid="other"))])
    valid, _ = split_valid_invalid(parse_messages(df, mapping))
    unique = dedup_in_batch(flatten_valid(valid, mapping)).collect()
    by_uid = {r["record_uid"]: r["kafka_offset"] for r in unique}
    assert by_uid == {"same": 3, "other": 1}
