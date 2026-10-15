"""Unit tests for src/common/mapping.py (M1) and config.py (M0)."""
import pytest

from src.common.config import FIELD_MAPPING_FILE, get_setting
from src.common.mapping import MISSING_REQUIRED, TYPE_ERROR, apply_mapping, load_mapping

pytestmark = pytest.mark.unit


@pytest.fixture(scope="module")
def mapping():
    return load_mapping(FIELD_MAPPING_FILE)


def good_record() -> dict:
    """A record shaped like a real joined order row (all values are text)."""
    return {
        "order_id": "o1", "customer_id": "c1", "order_status": "delivered",
        "order_purchase_timestamp": "2017-10-02 10:56:33",
        "order_approved_at": "2017-10-02 11:07:15",
        "order_delivered_carrier_date": "2017-10-04 19:55:00",
        "order_delivered_customer_date": "2017-10-10 21:25:13",
        "order_estimated_delivery_date": "2017-10-18 00:00:00",
        "customer_unique_id": "u1", "customer_zip_code_prefix": "01037",
        "customer_city": "sao paulo", "customer_state": "SP",
        "items_count": 2, "items_price_total": 100.5, "items_freight_total": 20.0,
        "payments_count": 1, "payments_value_total": 120.5, "payment_types": "credit_card",
        "unmapped_extra_column": "keep me",
    }


def test_happy_path_converts_types(mapping):
    payload, errors = apply_mapping(good_record(), mapping)
    assert errors == []
    assert payload["purchase_ts"] == "2017-10-02T10:56:33"
    assert payload["items_count"] == 2
    assert payload["payments_value_total"] == 120.5
    assert payload["customer_zip_code_prefix"] == "01037"  # leading zero kept


def test_empty_optional_field_is_null_not_error(mapping):
    record = good_record()
    record["order_delivered_customer_date"] = ""  # e.g. order not delivered yet
    payload, errors = apply_mapping(record, mapping)
    assert errors == []
    assert payload["delivered_customer_ts"] is None


def test_missing_required_field_is_reported(mapping):
    record = good_record()
    record["order_id"] = "  "
    payload, errors = apply_mapping(record, mapping)
    assert payload is None
    assert errors[0]["reason_code"] == MISSING_REQUIRED
    assert errors[0]["field"] == "order_id"


def test_bad_type_is_reported(mapping):
    record = good_record()
    record["payments_value_total"] = "abc"
    payload, errors = apply_mapping(record, mapping)
    assert payload is None
    assert errors[0]["reason_code"] == TYPE_ERROR


def test_bad_timestamp_is_reported(mapping):
    record = good_record()
    record["order_purchase_timestamp"] = "02/10/2017"
    _, errors = apply_mapping(record, mapping)
    assert errors[0]["reason_code"] == TYPE_ERROR


def test_extra_columns_are_not_modified(mapping):
    record = good_record()
    before = dict(record)
    apply_mapping(record, mapping)
    assert record == before  # caller keeps the original columns for `raw`


def test_missing_setting_raises_clear_error(monkeypatch):
    monkeypatch.delenv("SOME_UNSET_SETTING_FOR_TEST", raising=False)
    with pytest.raises(ValueError, match="SOME_UNSET_SETTING_FOR_TEST"):
        get_setting("SOME_UNSET_SETTING_FOR_TEST")


def test_default_and_override(monkeypatch):
    assert get_setting("SOME_UNSET_SETTING_FOR_TEST", "fallback") == "fallback"
    monkeypatch.setenv("SOME_UNSET_SETTING_FOR_TEST", "override")
    assert get_setting("SOME_UNSET_SETTING_FOR_TEST", "fallback") == "override"
