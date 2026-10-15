"""Data-quality rules for one mapped order payload.

The rules are listed in config/quality_rules.yaml (name, severity, reason code);
the checks themselves live here so a beginner can read exactly what each rule does.
`validate_payload` returns the list of problems found; it never raises.
"""
from datetime import datetime
from pathlib import Path

import yaml


def load_rules(path: Path) -> dict:
    with Path(path).open(encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def _ts(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


# Each check returns an error text, or None when the payload passes.
def _status_in_allowed_set(p: dict, cfg: dict) -> str | None:
    if p["order_status"] not in cfg["allowed_order_status"]:
        return f"order_status {p['order_status']!r} is not in the allowed set"


def _purchase_date_in_range(p: dict, cfg: dict) -> str | None:
    low, high = cfg["purchase_date_range"]["min"], cfg["purchase_date_range"]["max"]
    day = p["purchase_ts"][:10]
    if not low <= day <= high:
        return f"purchase date {day} is outside {low}..{high}"


def _amounts_not_negative(p: dict, cfg: dict) -> str | None:
    for name in ("items_price_total", "items_freight_total", "payments_value_total"):
        if p[name] is not None and p[name] < 0:
            return f"{name} is negative ({p[name]})"


def _approved_not_before_purchase(p: dict, cfg: dict) -> str | None:
    if p["approved_ts"] and _ts(p["approved_ts"]) < _ts(p["purchase_ts"]):
        return f"approved_ts {p['approved_ts']} is before purchase_ts {p['purchase_ts']}"


def _delivered_not_before_purchase(p: dict, cfg: dict) -> str | None:
    if p["delivered_customer_ts"] and _ts(p["delivered_customer_ts"]) < _ts(p["purchase_ts"]):
        return f"delivered_customer_ts {p['delivered_customer_ts']} is before purchase_ts {p['purchase_ts']}"


def _delivered_status_has_date(p: dict, cfg: dict) -> str | None:
    if p["order_status"] == "delivered" and not p["delivered_customer_ts"]:
        return "status is 'delivered' but delivered_customer_ts is empty"


def _delivered_date_matches_status(p: dict, cfg: dict) -> str | None:
    if p["order_status"] != "delivered" and p["delivered_customer_ts"]:
        return f"status is {p['order_status']!r} but delivered_customer_ts is set"


def _payment_total_positive(p: dict, cfg: dict) -> str | None:
    if p["payments_count"] > 0 and p["payments_value_total"] <= 0:
        return f"order has {p['payments_count']} payment row(s) but total value is {p['payments_value_total']}"


CHECKS = {
    "status_in_allowed_set": _status_in_allowed_set,
    "purchase_date_in_range": _purchase_date_in_range,
    "amounts_not_negative": _amounts_not_negative,
    "approved_not_before_purchase": _approved_not_before_purchase,
    "delivered_not_before_purchase": _delivered_not_before_purchase,
    "delivered_status_has_date": _delivered_status_has_date,
    "delivered_date_matches_status": _delivered_date_matches_status,
    "payment_total_positive": _payment_total_positive,
}


def validate_payload(payload: dict, config: dict) -> list[dict]:
    """Run every rule. Returns [{rule, severity, reason_code, detail}, ...] (empty = clean)."""
    problems = []
    for rule in config["rules"]:
        detail = CHECKS[rule["name"]](payload, config)
        if detail:
            problems.append({"rule": rule["name"], "severity": rule["severity"],
                             "reason_code": rule["reason_code"], "detail": detail})
    return problems


def split_by_severity(problems: list[dict]) -> tuple[list[dict], list[dict]]:
    """(rejections, warnings)."""
    return ([p for p in problems if p["severity"] == "reject"],
            [p for p in problems if p["severity"] == "warn"])
