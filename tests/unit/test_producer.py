"""Unit tests for the replay producer (M2). Kafka is replaced by a fake: no broker needed."""
import argparse
import json

import pytest

from src.producer import dataset_producer as dp

pytestmark = pytest.mark.unit

ORDER_HEADER = ("order_id,customer_id,order_status,order_purchase_timestamp,order_approved_at,"
                "order_delivered_carrier_date,order_delivered_customer_date,order_estimated_delivery_date\n")
GOOD = "o1,c1,delivered,2017-10-02 10:56:33,2017-10-02 11:07:15,2017-10-04 19:55:00,2017-10-10 21:25:13,2017-10-18 00:00:00\n"
BAD = "o2,c1,delivered,2017-10-03 10:00:00,2017-10-03 10:10:00,2017-10-04 10:00:00,,2017-10-18 00:00:00\n"  # delivered, no date


class FakeFuture:
    def __init__(self, fail: bool):
        self.fail = fail

    def add_callback(self, fn):
        if not self.fail:
            fn(type("Meta", (), {"partition": 1})())
        return self

    def add_errback(self, fn):
        if self.fail:
            fn(RuntimeError("broker down"))
        return self


class FakeProducer:
    def __init__(self, fail=False):
        self.fail, self.sent, self.closed = fail, [], False

    def send(self, topic, key, value, headers=None):
        self.sent.append({"topic": topic, "key": key, "value": value})
        return FakeFuture(self.fail)

    def flush(self):
        pass

    def close(self):
        self.closed = True


@pytest.fixture
def env(tmp_path, monkeypatch):
    """A tiny dataset folder + redirected output folders."""
    (tmp_path / "olist_customers_dataset.csv").write_text(
        "customer_id,customer_unique_id,customer_zip_code_prefix,customer_city,customer_state\nc1,u1,01037,sao paulo,SP\n")
    (tmp_path / "olist_order_items_dataset.csv").write_text(
        "order_id,order_item_id,product_id,seller_id,shipping_limit_date,price,freight_value\no1,1,p1,s1,2017-10-05 10:00:00,100.0,20.0\n")
    (tmp_path / "olist_order_payments_dataset.csv").write_text(
        "order_id,payment_sequential,payment_type,payment_installments,payment_value\no1,1,credit_card,1,120.0\n")
    monkeypatch.setattr(dp, "DATASET_DIR", tmp_path)
    monkeypatch.setattr(dp, "DEAD_LETTER_DIR", tmp_path / "dlq")
    monkeypatch.setattr(dp, "INGESTION_RUNS_DIR", tmp_path / "runs")
    monkeypatch.setattr(dp, "PROJECT_ROOT", tmp_path)
    return tmp_path


def make_args(env, rows, **overrides):
    orders = env / "orders.csv"
    orders.write_text(ORDER_HEADER + "".join(rows))
    base = dict(input=str(orders), bootstrap="x", topic="t", limit=0, full=True, dry_run=False, rate=0,
                batch_size=1000, run_id="test-run", key_field="order_id", no_enrich=True)
    base.update(overrides)
    return argparse.Namespace(**base)


def test_record_uid_is_deterministic_and_row_specific():
    assert dp.make_record_uid("abc", 5) == dp.make_record_uid("abc", 5)
    assert dp.make_record_uid("abc", 5) != dp.make_record_uid("abc", 6)
    assert dp.make_record_uid("abc", 5) != dp.make_record_uid("abd", 5)
    assert len(dp.make_record_uid("abc", 5)) == 32


def test_valid_record_is_published_with_envelope_and_key(env, monkeypatch):
    fake = FakeProducer()
    monkeypatch.setattr(dp, "create_producer", lambda bootstrap: fake)
    summary = dp.run(make_args(env, [GOOD]))
    assert summary["rows_published"] == 1 and summary["rows_rejected"] == 0
    message = fake.sent[0]
    assert message["key"] == "o1"                                   # key = order_id
    envelope = message["value"]
    assert envelope["source_category"] == "kafka_replayed"
    assert envelope["payload"]["items_count"] == 1 and envelope["payload"]["payments_value_total"] == 120.0
    assert envelope["raw"]["order_status"] == "delivered"            # original columns preserved
    assert json.dumps(envelope)                                      # serialisable
    assert summary["partition_counts_from_delivery_callbacks"] == {"1": 1}
    assert fake.closed


def test_invalid_record_goes_to_dead_letter_with_reason(env, monkeypatch):
    fake = FakeProducer()
    monkeypatch.setattr(dp, "create_producer", lambda bootstrap: fake)
    summary = dp.run(make_args(env, [GOOD, BAD]))
    assert summary["rows_read"] == 2 and summary["rows_valid"] == 1 and summary["rows_rejected"] == 1
    assert len(fake.sent) == 1                                       # the invalid one was not published
    line = json.loads((env / "dlq" / "ingest" / "test-run.jsonl").read_text().splitlines()[0])
    assert line["reason_code"] == "INCONSISTENT" and "delivered_status_has_date" in line["reason_detail"]
    assert line["stage"] == "ingest" and line["ingest_run_id"] == "test-run" and line["record_uid"]


def test_dry_run_publishes_nothing(env, monkeypatch):
    def boom(bootstrap):
        raise AssertionError("dry-run must not create a Kafka producer")
    monkeypatch.setattr(dp, "create_producer", boom)
    summary = dp.run(make_args(env, [GOOD], dry_run=True))
    assert summary["mode"] == "dry-run" and summary["rows_valid"] == 1 and summary["rows_published"] == 0


def test_publish_failure_is_counted(env, monkeypatch):
    monkeypatch.setattr(dp, "create_producer", lambda bootstrap: FakeProducer(fail=True))
    summary = dp.run(make_args(env, [GOOD]))
    assert summary["rows_publish_failed"] == 1 and summary["rows_published"] == 0


def test_exact_duplicate_is_counted_as_source_duplicate_but_not_dropped(env, monkeypatch):
    fake = FakeProducer()
    monkeypatch.setattr(dp, "create_producer", lambda bootstrap: fake)
    summary = dp.run(make_args(env, [GOOD, GOOD]))
    assert summary["source_duplicates_in_input"] == 1 and summary["rows_published"] == 2
    uids = {m["value"]["record_uid"] for m in fake.sent}
    hashes = {m["value"]["business_hash"] for m in fake.sent}
    assert len(uids) == 2 and len(hashes) == 1       # different rows, same business content


def test_rerun_of_same_file_gives_same_record_uids(env, monkeypatch):
    first, second = FakeProducer(), FakeProducer()
    producers = iter([first, second])
    monkeypatch.setattr(dp, "create_producer", lambda bootstrap: next(producers))
    args = make_args(env, [GOOD])
    dp.run(args)
    dp.run(args)
    assert first.sent[0]["value"]["record_uid"] == second.sent[0]["value"]["record_uid"]


def test_limit_restricts_rows(env, monkeypatch):
    monkeypatch.setattr(dp, "create_producer", lambda bootstrap: FakeProducer())
    summary = dp.run(make_args(env, [GOOD, BAD], limit=1, full=False))
    assert summary["rows_read"] == 1 and summary["mode"] == "limited"


def test_cli_requires_limit_or_full():
    with pytest.raises(SystemExit):
        dp.parse_args([])


def test_skip_selects_a_disjoint_slice_with_original_row_numbers(env, monkeypatch):
    fake = FakeProducer()
    monkeypatch.setattr(dp, "create_producer", lambda bootstrap: fake)
    second = GOOD.replace("o1", "o9")
    summary = dp.run(make_args(env, [GOOD, second], skip=1, limit=1, full=False))
    assert summary["rows_read"] == 1 and summary["skip_rows"] == 1
    assert fake.sent[0]["key"] == "o9" and fake.sent[0]["value"]["source_row_number"] == 2   # row number is not renumbered
