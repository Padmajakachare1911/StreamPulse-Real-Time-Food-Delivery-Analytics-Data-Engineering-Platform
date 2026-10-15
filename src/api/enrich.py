"""Attach a BRL -> USD value to an order, using rates fetched from the real API.

Matching logic (documented in docs/api_integration.md):
  * the lookup key is the order's purchase DATE, so ~600 unique dates are needed
    instead of 99,441 per-order calls (rule 10: never call an API per record)
  * the ECB publishes no rate on weekends/holidays, so we use the latest rate on or
    before the purchase date, but never one older than MAX_GAP_DAYS
  * orders with no usable rate get `api_enrichment = None` and are counted, not hidden
"""
from datetime import date, timedelta

from src.api.client import ApiResult

MAX_GAP_DAYS = 7


def build_rate_table(result: ApiResult) -> dict[str, float]:
    """{'2017-10-02': 0.31507, ...} from the API rows."""
    return {row["date"]: float(row["rate"]) for row in result.rows}


def find_rate(rate_table: dict[str, float], purchase_date: str) -> tuple[float, str, str] | None:
    """Return (rate, rate_date, match_type) for a YYYY-MM-DD date, or None."""
    if purchase_date in rate_table:
        return rate_table[purchase_date], purchase_date, "exact"
    day = date.fromisoformat(purchase_date)
    for back in range(1, MAX_GAP_DAYS + 1):
        earlier = (day - timedelta(days=back)).isoformat()
        if earlier in rate_table:
            return rate_table[earlier], earlier, "previous_business_day"
    return None


def enrich_payload(payload: dict, rate_table: dict[str, float], provenance: dict) -> dict | None:
    """Build the `api_enrichment` block for one mapped order payload."""
    purchase_ts = payload.get("purchase_ts")
    if not purchase_ts:
        return None
    found = find_rate(rate_table, purchase_ts[:10])
    if found is None:
        return None
    rate, rate_date, match_type = found
    return {
        "fx_rate_brl_usd": rate,
        "fx_rate_date": rate_date,
        "fx_match_type": match_type,
        "payments_value_usd": round(payload["payments_value_total"] * rate, 2),
        "api_source_name": provenance["api_source_name"],
        "api_endpoint": provenance["api_endpoint"],
        "api_retrieved_at": provenance["api_retrieved_at"],
        "api_http_status": provenance["api_http_status"],
        "api_cache_key": provenance["api_cache_key"],
    }
