# API Integration: Frankfurter exchange rates (BRL → USD)

## 1. Why this API (selection, as `docs/inputs/api_info.md` named none)
The professor did not name an API (`docs/inputs/api_info.md` does not exist), so we chose one from the real dataset columns, following `docs/07_DATASET_ADDENDUM_OLIST.md` §7.

| Candidate | Fits real columns? | Calls needed | Verdict |
|---|---|---|---|
| **Frankfurter (ECB rates)** | Yes: every order has `order_purchase_timestamp` and a BRL payment value | **1 request** for the whole 2016–2018 span (≈ 544 daily rates) | **Chosen** |
| BrasilAPI / IBGE | Yes: `customer_state` | ~27 | Possible later add-on; lower business value |
| Open-Meteo weather | Needs lat/lng per order (only zip-prefix centroids available) | thousands | Rejected: high volume, weak link to the columns |

Business meaning: it gives every order a USD value, so revenue can be shown in a second currency.
**Public APIs do not contain Olist orders.** The API supplies *reference data* that enriches the replayed historical orders; it does not provide orders.

## 2. Provider facts (checked on the live docs, 2026-10-15)
- Docs: https://frankfurter.dev  ·  Base URL used: `https://api.frankfurter.dev/v2`
- Auth: **none** (no API key). `API_KEY` in `.env` stays empty.
- Limits: no daily/monthly quota; rate-limited against abuse. Caching rates locally is explicitly encouraged by the provider.
- Source: European Central Bank reference rates, via `/providers/ecb/rates` (official reference rates, not blended).
- v1 is deprecated; we use v2. (An earlier default in our config pointed at v1; it was corrected after reading the live docs.)

## 3. Endpoint and parameters
`GET /providers/ecb/rates?base=BRL&quotes=USD&from=<YYYY-MM-DD>&to=<YYYY-MM-DD>`

Response (list, real example): `[{"date": "2017-10-02", "base": "BRL", "quote": "USD", "rate": 0.31507}, ...]`
No rates exist for weekends/holidays, which the enrichment handles (§5).

## 4. Real request evidence
| What | File | Observed result |
|---|---|---|
| One-week sample | `docs/evidence/02_api/api_real_request.txt` | HTTP 200, 5 rows, retrieved 2026-10-15 |
| Full order span 2016-09-04 → 2018-10-17 | `docs/evidence/02_api/api_full_range_request.txt` | HTTP 200, 544 rows |
| Simulated failure of the *real* client (unreachable host) | `docs/evidence/02_api/api_retry_backoff_simulated.txt` | 3 attempts, backoff 1 s then 2 s, clean error. This is a simulated failure, not a mock API. |
| Real-call integration test | `tests/integration/test_api_real.py` | passed (output saved in Phase 9) |

Reproduce: `python scripts/api_demo_call.py --sample` (PowerShell, from the repo root).

## 5. Enrichment logic (`src/api/enrich.py`)
- Lookup key = purchase **date** (a few hundred unique dates, never one call per order: rule 10).
- Exact date → `fx_match_type = exact`. Weekend/holiday → latest earlier rate within 7 days → `previous_business_day`. Nothing usable → `api_enrichment = null` (counted, never hidden).
- Added to each order event under `api_enrichment`: `fx_rate_brl_usd`, `fx_rate_date`, `fx_match_type`, `payments_value_usd`, plus provenance `api_source_name`, `api_endpoint`, `api_retrieved_at`, `api_http_status`, `api_cache_key`.
- Source labels: the record stays `kafka_replayed`; the enriched part is labelled `api_enriched`.
- The match rate (orders with a rate / all orders) is measured in Phase 3 and shown on the dashboard.
- `payments_value_usd` is an **estimate** (payment value × ECB reference rate of the purchase day), not the real settlement amount.

## 6. Client behaviour (`src/api/client.py`)
Configurable base URL; connect 5 s / read 30 s timeouts; status validation; retry with exponential backoff on 429, 5xx and timeouts (honours `Retry-After`); 0.5 s minimum gap between calls; response schema validation; persistent JSON cache in `data/processed/api_cache/` (git-ignored); sanitized logging; proper `User-Agent`.
Unit tests (mocked HTTP, never counted as proof): success, 4xx, 429 + Retry-After, 5xx then fail, timeout, malformed JSON, 5 schema mismatches, cache hit, weekend fallback.

## 7. Storage
Rates and provenance are written to PostgreSQL `core.api_cache` and `core.fx_rates` in Phase 6 and shown on the dashboard. Until then the raw response is stored in the local JSON cache.

## 8. Limitations
- ECB reference rates are daily mid-market rates, not what Olist's customers actually paid in USD (they paid in BRL).
- If the API is down, ingestion continues and enrichment is simply missing for those records.
