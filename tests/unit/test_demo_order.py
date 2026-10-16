"""Unit tests for user-submitted demo orders and their trace logic. No Kafka, Spark or database is needed:
the Kafka producer is replaced by a small fake that records what would be sent (this tests OUR logic; the real
flow through Kafka, Spark, Parquet and PostgreSQL is tested in tests/e2e and run for real in the demo evidence)."""
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from scripts.reconcile import render, status_of
from src.common.config import FIELD_MAPPING_FILE, QUALITY_RULES_FILE
from src.common.mapping import load_mapping
from src.demo import demo_order as d
from src.demo import order_trace as t
from src.demo.submissions import demo_message_counts, read_submissions
from src.processing.classify import classify_record
from src.processing.validators import load_rules
from src.producer.dataset_producer import make_record_uid

pytestmark = pytest.mark.unit
NOW = datetime(2026, 10, 16, 10, 30, 0)


class FakeProducer:
    def __init__(self):
        self.sent, self.next_offset = [], 100

    def send(self, topic, key, value, headers):
        self.sent.append({"topic": topic, "key": key, "value": value})
        meta = SimpleNamespace(partition=2, offset=self.next_offset)
        self.next_offset += 1
        return SimpleNamespace(get=lambda timeout=None: meta)

    def flush(self):
        pass

    def close(self):
        pass


def make(**changes) -> d.DemoOrderInput:
    return d.DemoOrderInput(**{"items_count": 2, "items_price_total": 150.0, "items_freight_total": 20.0, **changes})


def envelope(**kwargs):
    return d.prepare_demo_order(make(), now=NOW, rate_table={}, order_id="demo-abc", row_number=7, **kwargs)


# ---------------------------------------------------------------- the order itself
def test_demo_order_is_marked_and_uses_the_existing_schema():
    env = envelope()
    assert env["source_category"] == "user_demo" and env["payload"]["order_id"] == "demo-abc"
    assert env["ingest_run_id"].startswith("demo-order-") and env["source_file"] == d.DEMO_SOURCE_FILE
    mapping = load_mapping(FIELD_MAPPING_FILE)
    assert set(env["payload"]) == {f["canonical_name"] for f in mapping["fields"]}      # no new or missing columns
    assert env["payload"]["payments_value_total"] == 170.0 and env["payload"]["payments_count"] == 1
    assert env["payload"]["customer_id"].startswith("demo-") and env["api_enrichment"] is None


def test_record_uid_follows_the_existing_identity_rule():
    env = envelope()
    assert env["record_uid"] == make_record_uid(d.DEMO_SOURCE_SHA256, 7)


def test_enrichment_uses_the_given_rate_table():
    rates = {"2026-10-16": 0.18}
    prov = {"api_source_name": "Frankfurter", "api_endpoint": "x", "api_retrieved_at": "t", "api_http_status": 200, "api_cache_key": "k"}
    env = d.prepare_demo_order(make(), now=NOW, rate_table=rates, provenance=prov, row_number=1)
    assert env["api_enrichment"]["payments_value_usd"] == round(170.0 * 0.18, 2) and env["api_enrichment"]["fx_match_type"] == "exact"


def test_weekend_style_date_uses_the_previous_business_day():
    prov = {"api_source_name": "F", "api_endpoint": "x", "api_retrieved_at": "t", "api_http_status": 200, "api_cache_key": "k"}
    env = d.prepare_demo_order(make(), now=NOW, rate_table={"2026-10-14": 0.2}, provenance=prov, row_number=1)
    assert env["api_enrichment"]["fx_match_type"] == "previous_business_day" and env["api_enrichment"]["fx_rate_date"] == "2026-10-14"


def test_historical_rules_are_unchanged_and_still_reject_a_2026_purchase():
    """The demo path must not loosen the historical rules: only a demo-only copy moves the date window."""
    rules = load_rules(QUALITY_RULES_FILE)
    assert rules["purchase_date_range"]["max"] == "2018-12-31"
    row = d.build_demo_row(make(), NOW)
    _, problems, _ = classify_record(row, load_mapping(FIELD_MAPPING_FILE), rules)
    assert any("purchase_date_in_range" in p["detail"] for p in problems)
    assert d.demo_rules(rules, "2026-10-16")["purchase_date_range"]["max"] == "2026-10-16"
    assert rules["purchase_date_range"]["max"] == "2018-12-31"          # the original was not modified


@pytest.mark.parametrize("changes, text", [
    ({"customer_state": "XX"}, "customer_state"), ({"customer_zip_code_prefix": "12"}, "zip"), ({"items_count": 0}, "items_count"),
    ({"items_price_total": 0}, "items_price_total"), ({"items_freight_total": -1}, "freight"), ({"payment_type": "cash"}, "payment_type"),
    ({"order_status": "delivered"}, "order_status"), ({"order_status": "canceled"}, "order_status"), ({"customer_city": "  "}, "city"),
])
def test_bad_form_input_is_rejected_before_anything_is_sent(changes, text):
    with pytest.raises(d.DemoOrderError) as error:
        d.prepare_demo_order(make(**changes), now=NOW, rate_table={})
    assert text in str(error.value)


def test_created_order_has_no_approval_time_and_other_statuses_do():
    assert d.build_demo_row(make(order_status="created"), NOW)["order_approved_at"] is None
    assert d.build_demo_row(make(order_status="approved"), NOW)["order_approved_at"] is not None


# ---------------------------------------------------------------- publish, log, resend
def test_submit_publishes_one_keyed_message_and_logs_its_kafka_position(tmp_path):
    producer, log = FakeProducer(), tmp_path / "s.jsonl"
    record = d.submit_demo_order(make(), topic="t1", producer=producer, log_path=log, now=NOW, rate_table={}, order_id="demo-1", row_number=1)
    assert len(producer.sent) == 1 and producer.sent[0]["key"] == "demo-1" and producer.sent[0]["topic"] == "t1"
    assert (record["partition"], record["offset"], record["kind"]) == (2, 100, "submit")
    assert read_submissions("t1", log)[0]["order_id"] == "demo-1" and read_submissions("other", log) == []


def test_resend_publishes_the_identical_message_with_the_same_record_uid(tmp_path):
    producer, log = FakeProducer(), tmp_path / "s.jsonl"
    first = d.submit_demo_order(make(), topic="t1", producer=producer, log_path=log, now=NOW, rate_table={}, order_id="demo-1", row_number=1)
    again = d.resend_demo_order("demo-1", topic="t1", producer=producer, log_path=log)
    assert again["kind"] == "resend" and again["record_uid"] == first["record_uid"] and again["offset"] == 101
    assert producer.sent[0]["value"] == producer.sent[1]["value"]
    assert demo_message_counts("t1", log) == {"messages": 2, "orders": 1, "record_uids": 1}


def test_resend_of_an_unknown_order_is_refused(tmp_path):
    with pytest.raises(d.DemoOrderError):
        d.resend_demo_order("demo-nope", topic="t1", producer=FakeProducer(), log_path=tmp_path / "s.jsonl")


def test_kafka_failure_is_reported_safely_and_nothing_is_logged(tmp_path):
    class Broken(FakeProducer):
        def send(self, *a, **k):
            raise RuntimeError("secret internal detail")
    log = tmp_path / "s.jsonl"
    with pytest.raises(d.DemoOrderError) as error:
        d.submit_demo_order(make(), topic="t1", producer=Broken(), log_path=log, now=NOW, rate_table={})
    assert "secret" not in str(error.value) and not log.exists()


# ---------------------------------------------------------------- trace decisions
SUB = [{"topic": "t", "partition": 2, "offset": 100, "order_id": "demo-1", "record_uid": "u1", "api_enriched": True,
        "submitted_at": datetime(2026, 10, 16, 5, 0, 0, tzinfo=timezone.utc).isoformat()}]
KAFKA = {"partition": 2, "offset": 100, "key": "demo-1", "order_id": "demo-1", "record_uid": "u1", "source_category": "user_demo"}
PARQ = [{"record_uid": "u1", "order_id": "demo-1", "batch_id": "00000014", "kafka_partition": 2, "kafka_offset": 100}]
BATCH = {"batch_id": 14, "input_rows": 1, "valid_rows": 1, "invalid_rows": 0, "in_batch_duplicates": 0, "parquet_rows_written": 1, "seconds": 4.2}
PG = {"order_status": "created", "source_category": "user_demo", "rows_for_this_order": 1, "kafka_partition": 2, "kafka_offset": 100,
      "processed_at": datetime(2026, 10, 16, 5, 0, 9, tzinfo=timezone.utc)}


def states(stages):
    return [s.state for s in stages]


def test_complete_when_every_layer_has_the_order():
    stages = t.build_stages(SUB, KAFKA, PARQ, BATCH, PG)
    assert states(stages) == ["done"] * 4 and t.overall_state(stages) == "complete"
    assert "batch 14" in stages[2].detail and t.latency_seconds(SUB, PG) == 9.0


def test_waiting_for_spark_when_only_kafka_has_it():
    stages = t.build_stages(SUB, KAFKA, None, None, None)
    assert states(stages) == ["done", "done", "waiting", "waiting"] and t.overall_state(stages) == "in_progress"
    assert "run_stream" in stages[2].detail and t.latency_seconds(SUB, None) is None


def test_unavailable_is_never_reported_as_done():
    stages = t.build_stages(SUB, t.UNAVAILABLE, PARQ, BATCH, t.UNAVAILABLE)
    assert states(stages) == ["done", "unavailable", "done", "unavailable"] and t.overall_state(stages) == "unavailable"


def test_unknown_order_is_not_traceable_and_wrong_kafka_message_is_not_accepted():
    unknown = t.build_stages([], None, None, None, None)
    assert unknown[0].state == "waiting" and t.overall_state(unknown) == "not_found"
    other = {**KAFKA, "order_id": "someone-else"}
    assert t.build_stages(SUB, other, None, None, None)[1].state == "waiting"


def test_resend_is_mentioned_and_later_copies_are_explained():
    subs = SUB + [{**SUB[0], "offset": 105}]
    pg = {**PG, "kafka_offset": 100}
    stages = t.build_stages(subs, KAFKA, PARQ, BATCH, pg)
    assert "sent again 1 time" in stages[0].detail and stages[0].facts["messages_sent"] == 2
    later = t.build_stages(subs, KAFKA, PARQ, BATCH, {**PG, "kafka_offset": 99})
    assert "ignored" in later[3].detail


# ---------------------------------------------------------------- reconciliation accounting for demo messages
def test_reconcile_treats_demo_messages_as_explained_not_as_a_match_or_an_error():
    row = {"stage": "4", "expected": 100, "actual": 103, "explained": {"demo order messages": 3}, "note": ""}
    assert status_of(row) == "EXPLAINED"
    assert status_of({**row, "actual": 104}) == "UNEXPLAINED"                   # one message beyond the demo log is still caught
    text, ok = render([("x", [{**row, "actual": 104}])])
    assert not ok and "UNEXPLAINED MISMATCH FOUND" in text
