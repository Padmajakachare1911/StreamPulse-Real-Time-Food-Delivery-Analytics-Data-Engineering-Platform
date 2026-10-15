"""Real call to the Frankfurter API (no mocks). Skipped when the machine is offline."""
import pytest
import requests

from src.api.client import FrankfurterClient
from src.common.config import API_BASE_URL, API_USER_AGENT

pytestmark = pytest.mark.integration


def _online() -> bool:
    try:
        requests.get(API_BASE_URL + "/currency/eur", timeout=5)
        return True
    except requests.RequestException:
        return False


@pytest.mark.skipif(not _online(), reason="API not reachable (offline)")
def test_real_request_returns_valid_rates():
    client = FrankfurterClient(API_BASE_URL, API_USER_AGENT, cache_dir=None)
    result = client.get_rates("BRL", "USD", "2017-10-02", "2017-10-08")
    assert result.provenance["api_http_status"] == 200
    assert result.provenance["cache_hit"] is False
    assert len(result.rows) >= 4  # one working week
    assert all(0.1 < row["rate"] < 1.0 for row in result.rows)  # BRL is worth well under 1 USD
