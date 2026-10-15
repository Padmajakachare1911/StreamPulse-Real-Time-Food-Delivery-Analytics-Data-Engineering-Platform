"""The REAL API responses are stored with provenance in the real warehouse (read-only checks).
Skipped (with the reason shown) when PostgreSQL is not reachable or nothing was loaded yet."""
import pytest

from src.dashboard import queries

pytestmark = pytest.mark.integration


def rows(sql: str):
    try:
        return queries.run_query(sql)
    except queries.QueryError:
        pytest.skip("PostgreSQL not reachable")


def test_api_responses_are_cached_with_provenance():
    cache = rows("SELECT api_source_name, api_endpoint, http_status, retrieved_at, jsonb_array_length(response_json) AS n FROM core.api_cache")
    if cache.empty:
        pytest.skip("core.api_cache is empty: run python -m src.warehouse.load_reference --only api")
    assert (cache["api_source_name"] == "frankfurter-ecb").all()
    assert (cache["http_status"] == 200).all()
    assert cache["api_endpoint"].str.startswith("https://api.frankfurter.dev/v2/providers/ecb/rates").all()
    assert cache["retrieved_at"].notna().all() and (cache["n"] > 0).all()


def test_fx_rates_are_plausible_and_complete_for_the_order_period():
    fx = rows("SELECT min(rate_date) AS first, max(rate_date) AS last, count(*) AS n, min(rate) AS lo, max(rate) AS hi FROM core.fx_rates")
    row = fx.iloc[0]
    if int(row["n"]) == 0:
        pytest.skip("core.fx_rates is empty")
    assert str(row["first"]) <= "2016-09-05" and str(row["last"]) >= "2018-10-17"   # covers every order date
    assert 0.1 < float(row["lo"]) and float(row["hi"]) < 1.0                        # 1 BRL is worth well under 1 USD


def test_enriched_orders_carry_api_provenance():
    result = rows("""SELECT count(*) AS n, count(*) FILTER (WHERE api_enriched AND api_source_name IS NOT NULL
                     AND api_http_status = 200 AND api_cache_key IS NOT NULL AND fx_rate_brl_usd > 0) AS ok FROM core.orders""").iloc[0]
    if int(result["n"]) == 0:
        pytest.skip("core.orders is empty")
    assert int(result["ok"]) == int(result["n"])
