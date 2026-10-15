"""Make a REAL request to the exchange-rate API and show what came back.

Used for Demo 2 and for the evidence file docs/evidence/02_api/api_real_request.txt.

  python scripts/api_demo_call.py --sample            small real request (one week)
  python scripts/api_demo_call.py --full-range        the date span of the real orders
  python scripts/api_demo_call.py --simulate-failure  real client, unreachable host: shows retry/backoff
"""
import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.api.client import ApiError, FrankfurterClient  # noqa: E402
from src.api.enrich import build_rate_table, enrich_payload, find_rate  # noqa: E402
from src.common.config import API_BASE_URL, API_USER_AGENT, PROJECT_ROOT  # noqa: E402

CACHE_DIR = PROJECT_ROOT / "data" / "processed" / "api_cache"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--sample", action="store_true", help="request one week: 2017-10-02 to 2017-10-08")
    parser.add_argument("--full-range", action="store_true", help="request 2016-09-04 to 2018-10-17 (order date span)")
    parser.add_argument("--simulate-failure", action="store_true",
                        help="point the real client at an unreachable host to show retry/backoff")
    parser.add_argument("--no-cache", action="store_true", help="ignore and do not write the local cache")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    start, end = ("2016-09-04", "2018-10-17") if args.full_range else ("2017-10-02", "2017-10-08")
    base_url = "https://frankfurter-host-does-not-exist.invalid/v2" if args.simulate_failure else API_BASE_URL
    client = FrankfurterClient(
        base_url, API_USER_AGENT, cache_dir=None if (args.no_cache or args.simulate_failure) else CACHE_DIR,
        max_retries=2, backoff_seconds=1.0,
    )

    print(f"Provider : Frankfurter (ECB reference rates), docs https://frankfurter.dev")
    print(f"Request  : BRL -> USD, {start} to {end}")
    try:
        result = client.get_rates("BRL", "USD", start, end)
    except ApiError as exc:
        print(f"RESULT   : request failed after retries: {exc}")
        return 2

    print(f"Retrieved: {result.provenance['api_retrieved_at']}")
    print(f"URL      : {result.provenance['api_endpoint']}")
    print(f"HTTP     : {result.provenance['api_http_status']}   (served from local cache: {result.provenance['cache_hit']})")
    print(f"Rows     : {len(result.rows)}")
    print("Excerpt  :")
    for row in result.rows[:3]:
        print("   ", json.dumps(row))

    # Show the enrichment step on a few real order dates (a Saturday proves the weekday fallback).
    table = build_rate_table(result)
    print("\nEnrichment on sample purchase dates:")
    for purchase_date in (start, "2017-10-07", end):
        found = find_rate(table, purchase_date)
        print(f"   {purchase_date} -> {found}")
    fake_payload = {"purchase_ts": "2017-10-04T10:00:00", "payments_value_total": 100.0}
    print("   payload with 100.00 BRL ->", json.dumps(enrich_payload(fake_payload, table, result.provenance))[:260])
    return 0


if __name__ == "__main__":
    sys.exit(main())
