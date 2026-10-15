"""Unit tests for the API client and enrichment (M3).

The HTTP session is faked here ON PURPOSE: unit tests check our retry/validation
logic. They are NOT proof that the real API works; see tests/integration for that.
"""
import pytest
import requests

from src.api.client import ApiError, ApiSchemaError, FrankfurterClient, sanitize_url
from src.api.enrich import MAX_GAP_DAYS, build_rate_table, enrich_payload, find_rate

pytestmark = pytest.mark.unit

GOOD_ROWS = [
    {"date": "2017-10-05", "base": "BRL", "quote": "USD", "rate": 0.31},
    {"date": "2017-10-06", "base": "BRL", "quote": "USD", "rate": 0.32},
]


class FakeResponse:
    def __init__(self, status=200, body=None, headers=None, text="", json_error=False):
        self.status_code = status
        self._body = body
        self.headers = headers or {}
        self.text = text
        self.url = "https://example.test/v2/providers/ecb/rates?base=BRL&token=abc123"
        self._json_error = json_error

    def json(self):
        if self._json_error:
            raise ValueError("not json")
        return self._body


class FakeSession:
    """Returns the queued responses one by one; an Exception item is raised instead."""
    def __init__(self, queue):
        self.queue = list(queue)
        self.calls = 0
        self.headers = {}

    def get(self, url, params=None, timeout=None):
        self.calls += 1
        item = self.queue.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def make_client(queue, tmp_path=None, **kw):
    sleeps = []
    client = FakeSession(queue)
    api = FrankfurterClient("https://example.test/v2", "test-agent", cache_dir=tmp_path, session=client,
                            backoff_seconds=1, min_interval_seconds=0, sleep=sleeps.append, **kw)
    return api, client, sleeps


def test_success_returns_rows_and_provenance():
    api, _, _ = make_client([FakeResponse(200, GOOD_ROWS)])
    result = api.get_rates("brl", "usd", "2017-10-05", "2017-10-06")
    assert len(result.rows) == 2
    assert result.provenance["api_http_status"] == 200
    assert result.provenance["cache_hit"] is False
    assert "abc123" not in result.provenance["api_endpoint"]  # secret-looking value masked


def test_client_error_4xx_does_not_retry():
    api, session, _ = make_client([FakeResponse(422, text="invalid currency")])
    with pytest.raises(ApiError, match="422"):
        api.get_rates("ABC", "usd", "2017-10-05", "2017-10-06")
    assert session.calls == 1


def test_429_honours_retry_after_then_succeeds():
    api, session, sleeps = make_client([FakeResponse(429, headers={"Retry-After": "7"}), FakeResponse(200, GOOD_ROWS)])
    result = api.get_rates("BRL", "USD", "2017-10-05", "2017-10-06")
    assert len(result.rows) == 2
    assert session.calls == 2
    assert 7.0 in sleeps


def test_5xx_retries_with_backoff_then_fails():
    api, session, sleeps = make_client([FakeResponse(503)] * 3, max_retries=2)
    with pytest.raises(ApiError, match="failed after 3 attempts"):
        api.get_rates("BRL", "USD", "2017-10-05", "2017-10-06")
    assert session.calls == 3
    assert sleeps == [1, 2]  # exponential: 1s then 2s


def test_timeout_is_retried():
    api, session, _ = make_client([requests.Timeout(), FakeResponse(200, GOOD_ROWS)])
    assert len(api.get_rates("BRL", "USD", "2017-10-05", "2017-10-06").rows) == 2
    assert session.calls == 2


def test_malformed_json_is_schema_error():
    api, _, _ = make_client([FakeResponse(200, json_error=True)])
    with pytest.raises(ApiSchemaError, match="not valid JSON"):
        api.get_rates("BRL", "USD", "2017-10-05", "2017-10-06")


@pytest.mark.parametrize("body", [
    {"not": "a list"},
    [],
    [{"date": "2017-10-05"}],
    [{"date": "05/10/2017", "base": "BRL", "quote": "USD", "rate": 0.3}],
    [{"date": "2017-10-05", "base": "BRL", "quote": "USD", "rate": -1}],
])
def test_schema_mismatch_is_rejected(body):
    api, _, _ = make_client([FakeResponse(200, body)])
    with pytest.raises(ApiSchemaError):
        api.get_rates("BRL", "USD", "2017-10-05", "2017-10-06")


def test_second_call_is_served_from_cache(tmp_path):
    api, session, _ = make_client([FakeResponse(200, GOOD_ROWS)], tmp_path=tmp_path)
    api.get_rates("BRL", "USD", "2017-10-05", "2017-10-06")
    again = api.get_rates("BRL", "USD", "2017-10-05", "2017-10-06")
    assert session.calls == 1
    assert again.provenance["cache_hit"] is True


def test_sanitize_url_masks_secrets():
    assert sanitize_url("https://x/y?a=1&apikey=SECRET&b=2") == "https://x/y?a=1&apikey=***&b=2"


def test_find_rate_exact_and_weekend_fallback():
    table = build_rate_table(type("R", (), {"rows": GOOD_ROWS})())
    assert find_rate(table, "2017-10-05") == (0.31, "2017-10-05", "exact")
    assert find_rate(table, "2017-10-07") == (0.32, "2017-10-06", "previous_business_day")  # Saturday
    assert find_rate(table, "2017-10-01") is None  # nothing on/before within the allowed gap
    assert MAX_GAP_DAYS == 7


def test_enrich_payload_converts_and_adds_provenance():
    prov = {"api_source_name": "s", "api_endpoint": "e", "api_retrieved_at": "t", "api_http_status": 200, "api_cache_key": "k"}
    block = enrich_payload({"purchase_ts": "2017-10-06T08:00:00", "payments_value_total": 100.0}, {"2017-10-06": 0.32}, prov)
    assert block["payments_value_usd"] == 32.0
    assert block["api_cache_key"] == "k"
    assert enrich_payload({"purchase_ts": "2010-01-01T00:00:00", "payments_value_total": 1.0}, {"2017-10-06": 0.32}, prov) is None
