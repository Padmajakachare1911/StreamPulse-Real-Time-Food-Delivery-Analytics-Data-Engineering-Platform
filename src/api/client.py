"""Client for the real Frankfurter exchange-rate API (ECB reference rates).

Why this API: the Olist orders are in Brazilian reais (BRL) and have a purchase
date, so the historical BRL -> USD rate on that date gives every order a USD value.
Docs: https://frankfurter.dev   (no API key, no quota, rate-limited against abuse)

The client is careful on purpose:
  * connect/read timeouts, status validation, response schema validation
  * retries with exponential backoff on 429 / 5xx / timeouts, honouring Retry-After
  * a persistent JSON cache, so each date range is requested only once
  * logs never contain secrets (this API has none, but the URL is still sanitized)
"""
import hashlib
import json
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import requests

log = logging.getLogger(__name__)

SOURCE_NAME = "frankfurter-ecb"
DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class ApiError(Exception):
    """The API could not give us a usable answer."""


class ApiSchemaError(ApiError):
    """The API answered, but not in the shape we expect."""


@dataclass
class ApiResult:
    """Rates plus the provenance fields required by docs/04 section 8."""
    rows: list[dict]
    provenance: dict = field(default_factory=dict)


def sanitize_url(url: str) -> str:
    """Hide anything that looks like a secret in a query string."""
    return re.sub(r"(?i)(key|token|secret|password)=[^&]+", r"\1=***", url)


def validate_rates(payload) -> list[dict]:
    """Check the response is a non-empty list of {date, base, quote, rate}."""
    if not isinstance(payload, list):
        raise ApiSchemaError(f"expected a JSON list, got {type(payload).__name__}")
    if not payload:
        raise ApiSchemaError("API returned an empty list (no rates for this range)")
    for item in payload:
        if not isinstance(item, dict) or not {"date", "base", "quote", "rate"} <= item.keys():
            raise ApiSchemaError(f"unexpected item shape: {str(item)[:120]}")
        if not DATE_PATTERN.match(str(item["date"])):
            raise ApiSchemaError(f"bad date value: {item['date']!r}")
        rate = item["rate"]
        if isinstance(rate, bool) or not isinstance(rate, (int, float)) or rate <= 0:
            raise ApiSchemaError(f"bad rate value: {rate!r}")
    return payload


class FrankfurterClient:
    """Fetch historical exchange rates, with retry, throttling and a file cache."""

    def __init__(self, base_url: str, user_agent: str, cache_dir: Path | None = None,
                 session: requests.Session | None = None, timeout: tuple[float, float] = (5, 30),
                 max_retries: int = 4, backoff_seconds: float = 1.0, min_interval_seconds: float = 0.5,
                 sleep=time.sleep):
        self.base_url = base_url.rstrip("/")
        self.cache_dir = Path(cache_dir) if cache_dir else None
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": user_agent, "Accept": "application/json"})
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds
        self.min_interval_seconds = min_interval_seconds
        self._sleep = sleep  # injectable so tests do not really wait
        self._last_call = 0.0

    # ---- public --------------------------------------------------------
    def get_rates(self, base: str, quote: str, start: str, end: str) -> ApiResult:
        """Daily `base`->`quote` rates from `start` to `end` (YYYY-MM-DD, both included)."""
        path = "/providers/ecb/rates"
        params = {"base": base.upper(), "quotes": quote.upper(), "from": start, "to": end}
        cache_key = hashlib.sha256(f"{SOURCE_NAME}|{path}|{sorted(params.items())}".encode()).hexdigest()[:32]

        cached = self._read_cache(cache_key)
        if cached:
            log.info("API cache hit for %s (%d rows)", cache_key, len(cached["rows"]))
            cached["provenance"]["cache_hit"] = True
            return ApiResult(rows=cached["rows"], provenance=cached["provenance"])

        response = self._request(self.base_url + path, params)
        try:
            rows = validate_rates(response.json())
        except ValueError as exc:  # response.json() failed: not JSON
            raise ApiSchemaError(f"response is not valid JSON: {exc}") from exc

        provenance = {
            "api_source_name": SOURCE_NAME,
            "api_endpoint": sanitize_url(response.url),
            "api_retrieved_at": datetime.now(timezone.utc).isoformat(),
            "api_http_status": response.status_code,
            "api_cache_key": cache_key,
            "cache_hit": False,
        }
        self._write_cache(cache_key, {"rows": rows, "provenance": provenance})
        return ApiResult(rows=rows, provenance=provenance)

    # ---- internals -----------------------------------------------------
    def _throttle(self) -> None:
        """Keep a small gap between calls to respect the provider's rate limit."""
        wait = self.min_interval_seconds - (time.monotonic() - self._last_call)
        if wait > 0:
            self._sleep(wait)
        self._last_call = time.monotonic()

    def _request(self, url: str, params: dict) -> requests.Response:
        """GET with retries. Raises ApiError when all attempts fail."""
        last_problem = "unknown"
        for attempt in range(1, self.max_retries + 2):
            self._throttle()
            try:
                response = self.session.get(url, params=params, timeout=self.timeout)
            except (requests.Timeout, requests.ConnectionError) as exc:
                last_problem = f"{type(exc).__name__}"
                log.warning("API attempt %d failed: %s", attempt, last_problem)
                delay = self.backoff_seconds * 2 ** (attempt - 1)
            else:
                log.info("API GET %s -> HTTP %d", sanitize_url(response.url), response.status_code)
                if response.status_code == 200:
                    return response
                if response.status_code == 429 or response.status_code >= 500:
                    last_problem = f"HTTP {response.status_code}"
                    retry_after = response.headers.get("Retry-After", "")
                    delay = float(retry_after) if retry_after.isdigit() else self.backoff_seconds * 2 ** (attempt - 1)
                else:  # 4xx other than 429: retrying will not help
                    raise ApiError(f"HTTP {response.status_code}: {response.text[:200]}")
            if attempt > self.max_retries:
                break
            log.warning("Retrying in %.1fs (attempt %d of %d)", delay, attempt, self.max_retries)
            self._sleep(delay)
        raise ApiError(f"API failed after {self.max_retries + 1} attempts: {last_problem}")

    def _cache_file(self, key: str) -> Path | None:
        return self.cache_dir / f"{key}.json" if self.cache_dir else None

    def _read_cache(self, key: str) -> dict | None:
        path = self._cache_file(key)
        if path and path.exists():
            try:
                return json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                log.warning("Ignoring unreadable cache file %s", path)
        return None

    def _write_cache(self, key: str, data: dict) -> None:
        path = self._cache_file(key)
        if path:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(data), encoding="utf-8")
