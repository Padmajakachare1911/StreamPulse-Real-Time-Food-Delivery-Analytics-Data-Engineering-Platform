"""Unit tests for the quality rules (M5) and the dead-letter format."""
import json

import pytest

from src.common.config import QUALITY_RULES_FILE
from src.processing.deadletter import DeadLetterWriter, make_dead_letter
from src.processing.validators import load_rules, split_by_severity, validate_payload

pytestmark = pytest.mark.unit


@pytest.fixture(scope="module")
def rules():
    return load_rules(QUALITY_RULES_FILE)


def good() -> dict:
    return {"order_status": "delivered", "purchase_ts": "2017-10-02T10:56:33", "approved_ts": "2017-10-02T11:07:15",
            "delivered_customer_ts": "2017-10-10T21:25:13", "items_price_total": 100.0, "items_freight_total": 20.0,
            "payments_value_total": 120.0, "payments_count": 1}


def names(problems):
    return {p["rule"] for p in problems}


def test_clean_record_has_no_problems(rules):
    assert validate_payload(good(), rules) == []


def test_not_delivered_order_with_empty_delivery_date_is_valid(rules):
    p = good() | {"order_status": "shipped", "delivered_customer_ts": None}
    assert validate_payload(p, rules) == []          # empty delivery date is normal here


@pytest.mark.parametrize("change, rule", [
    ({"order_status": "teleported"}, "status_in_allowed_set"),
    ({"purchase_ts": "1999-01-01T00:00:00"}, "purchase_date_in_range"),
    ({"items_price_total": -1.0}, "amounts_not_negative"),
    ({"items_freight_total": -0.01}, "amounts_not_negative"),
    ({"payments_value_total": -5.0}, "amounts_not_negative"),
    ({"approved_ts": "2017-10-01T00:00:00"}, "approved_not_before_purchase"),
    ({"delivered_customer_ts": "2017-10-01T00:00:00"}, "delivered_not_before_purchase"),
    ({"delivered_customer_ts": None}, "delivered_status_has_date"),
    ({"order_status": "canceled"}, "delivered_date_matches_status"),
])
def test_each_reject_rule_fires(rules, change, rule):
    problems = validate_payload(good() | change, rules)
    assert rule in names(problems)
    rejections, _ = split_by_severity(problems)
    assert any(p["rule"] == rule for p in rejections)


def test_zero_payment_total_is_only_a_warning(rules):
    problems = validate_payload(good() | {"payments_value_total": 0.0}, rules)
    rejections, warnings = split_by_severity(problems)
    assert rejections == [] and names(warnings) == {"payment_total_positive"}


def test_dead_letter_record_format_and_file(tmp_path):
    record = make_dead_letter("ingest", "RANGE_ERROR", "negative price", {"order_id": "x"}, "run1", "uid1")
    assert set(record) == {"dlq_id", "stage", "reason_code", "reason_detail", "record_uid",
                           "ingest_run_id", "raw_payload", "rejected_at"}
    writer = DeadLetterWriter(tmp_path, "ingest", "run1")
    writer.write(record)
    assert json.loads(writer.path.read_text().splitlines()[0])["reason_code"] == "RANGE_ERROR"
    assert writer.count == 1


def test_dead_letter_file_is_replaced_for_the_same_run_id(tmp_path):
    first = DeadLetterWriter(tmp_path, "ingest", "run1")
    first.write(make_dead_letter("ingest", "X", "d", {}, "run1"))
    second = DeadLetterWriter(tmp_path, "ingest", "run1")   # same run id starts fresh
    second.write(make_dead_letter("ingest", "X", "d", {}, "run1"))
    assert len(second.path.read_text().splitlines()) == 1
