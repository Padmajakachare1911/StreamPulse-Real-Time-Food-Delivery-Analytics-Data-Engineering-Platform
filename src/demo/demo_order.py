"""Place a demo order from the dashboard form and publish it to Kafka through the real pipeline.

A demo order is a NEW, user-submitted order. It uses exactly the same schema, mapping, quality rules,
Kafka topic, Spark job, Parquet layer and PostgreSQL table as the historical Olist replay, and it is
marked so nobody mistakes it for a historical record:
  order_id        starts with "demo-"
  source_category "user_demo"          (historical rows: "kafka_replayed")
  ingest_run_id   starts with "demo-order-"
No personal data is collected: the customer id is a random "demo-..." value.

The historical quality rules are NOT changed. The only rule that cannot hold for a new order is the
purchase-date window (it ends in 2018), so a demo-only copy of the rules moves that upper bound to today.
"""
import hashlib
import logging
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from src.api.client import ApiError, FrankfurterClient
from src.api.enrich import build_rate_table, enrich_payload
from src.common.config import (API_BASE_URL, API_USER_AGENT, FIELD_MAPPING_FILE, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC,
                               QUALITY_RULES_FILE)
from src.common.mapping import load_mapping
from src.demo.submissions import SUBMISSIONS_FILE, append_submission, read_submissions
from src.processing.classify import classify_record
from src.processing.validators import load_rules
from src.producer.dataset_producer import API_CACHE_DIR, SCHEMA_VERSION, build_envelope, clean_row

log = logging.getLogger("demo_order")

USER_DEMO = "user_demo"
ORDER_ID_PREFIX = "demo-"
RUN_ID_PREFIX = "demo-order-"
DEMO_SOURCE_FILE = "user_submitted_demo_order"
DEMO_SOURCE_SHA256 = hashlib.sha256(b"streampulse-user-submitted-demo-orders-v1").hexdigest()
ESTIMATED_DELIVERY_DAYS = 15

BRAZIL_STATES = ["AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE",
                 "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"]
PAYMENT_TYPES = ["credit_card", "boleto", "voucher", "debit_card"]
# 'delivered' and 'canceled' are left out on purpose: a brand-new order cannot already have a delivery date.
DEMO_STATUSES = ["created", "approved", "processing", "invoiced", "shipped"]


class DemoOrderError(Exception):
    """The order was not published. The message is safe to show to the user."""


@dataclass
class DemoOrderInput:
    customer_state: str = "SP"
    customer_city: str = "sao paulo"
    customer_zip_code_prefix: str = "01310"
    items_count: int = 1
    items_price_total: float = 100.0
    items_freight_total: float = 10.0
    payment_type: str = "credit_card"
    order_status: str = "created"


def check_input(inp: DemoOrderInput) -> None:
    """Simple form checks (before the real quality rules). Raises DemoOrderError."""
    problems = []
    if inp.customer_state not in BRAZIL_STATES:
        problems.append(f"customer_state must be one of {', '.join(BRAZIL_STATES)}")
    if not inp.customer_city or not inp.customer_city.strip() or len(inp.customer_city) > 60:
        problems.append("customer_city is required (at most 60 characters)")
    if not (isinstance(inp.customer_zip_code_prefix, str) and inp.customer_zip_code_prefix.isdigit()
            and len(inp.customer_zip_code_prefix) == 5):
        problems.append("customer_zip_code_prefix must be exactly 5 digits")
    if not 1 <= int(inp.items_count) <= 20:
        problems.append("items_count must be between 1 and 20")
    if not 0 < float(inp.items_price_total) <= 100000:
        problems.append("items_price_total must be greater than 0 and at most 100000")
    if not 0 <= float(inp.items_freight_total) <= 10000:
        problems.append("items_freight_total must be between 0 and 10000")
    if inp.payment_type not in PAYMENT_TYPES:
        problems.append(f"payment_type must be one of {', '.join(PAYMENT_TYPES)}")
    if inp.order_status not in DEMO_STATUSES:
        problems.append(f"order_status must be one of {', '.join(DEMO_STATUSES)}")
    if problems:
        raise DemoOrderError("; ".join(problems))


def demo_rules(rules: dict, today: str) -> dict:
    """A copy of the quality rules whose purchase-date window ends today. The original is not modified."""
    changed = dict(rules)
    changed["purchase_date_range"] = {"min": rules["purchase_date_range"]["min"], "max": today}
    return changed


def build_demo_row(inp: DemoOrderInput, now: datetime, order_id: str | None = None) -> dict:
    """One row in the SAME shape build_order_rows() produces for the Olist files (source column names)."""
    stamp = "%Y-%m-%d %H:%M:%S"
    order_id = order_id or f"{ORDER_ID_PREFIX}{uuid.uuid4().hex}"
    customer_id = f"{ORDER_ID_PREFIX}customer-{uuid.uuid4().hex[:12]}"
    price, freight = round(float(inp.items_price_total), 2), round(float(inp.items_freight_total), 2)
    return {
        "order_id": order_id,
        "customer_id": customer_id,
        "order_status": inp.order_status,
        "order_purchase_timestamp": now.strftime(stamp),
        "order_approved_at": None if inp.order_status == "created" else now.strftime(stamp),
        "order_delivered_carrier_date": None,
        "order_delivered_customer_date": None,
        "order_estimated_delivery_date": (now + timedelta(days=ESTIMATED_DELIVERY_DAYS)).strftime(stamp),
        "customer_unique_id": customer_id,
        "customer_zip_code_prefix": inp.customer_zip_code_prefix,
        "customer_city": inp.customer_city.strip().lower(),
        "customer_state": inp.customer_state,
        "items_count": int(inp.items_count),
        "items_price_total": price,
        "items_freight_total": freight,
        "payments_count": 1,
        "payments_value_total": round(price + freight, 2),
        "payment_types": inp.payment_type,
    }


def fetch_rate_table(purchase_date: str) -> tuple[dict, dict]:
    """Real Frankfurter rates for the 8 days up to the purchase date (so a weekend finds the previous business day).
    If the API is unavailable the order is still sent, without enrichment (same rule as the historical replay)."""
    try:
        start = (datetime.fromisoformat(purchase_date) - timedelta(days=7)).date().isoformat()
        client = FrankfurterClient(API_BASE_URL, API_USER_AGENT, cache_dir=API_CACHE_DIR)
        result = client.get_rates("BRL", "USD", start, purchase_date)
        return build_rate_table(result), result.provenance
    except (ApiError, ValueError) as exc:
        log.warning("API enrichment unavailable for the demo order: %s", exc)
        return {}, {}


def prepare_demo_order(inp: DemoOrderInput, *, now: datetime | None = None, rate_table: dict | None = None,
                       provenance: dict | None = None, order_id: str | None = None,
                       row_number: int | None = None) -> dict:
    """Validate with the real rules and return the Kafka envelope. Raises DemoOrderError when rejected.
    rate_table=None means "fetch from the real API"; pass {} to skip enrichment."""
    check_input(inp)
    now = now or datetime.now()
    row = clean_row(build_demo_row(inp, now, order_id))
    rules = demo_rules(load_rules(QUALITY_RULES_FILE), now.strftime("%Y-%m-%d"))
    payload, problems, _ = classify_record(row, load_mapping(FIELD_MAPPING_FILE), rules)
    if problems:
        raise DemoOrderError("rejected by the quality rules: " + "; ".join(f"{p['reason_code']} {p['detail']}" for p in problems))
    if rate_table is None:
        rate_table, provenance = fetch_rate_table(payload["purchase_ts"][:10])
    enrichment = enrich_payload(payload, rate_table, provenance or {}) if rate_table else None
    number = row_number if row_number is not None else time.time_ns() // 1000     # unique, fits BIGINT
    run_id = f"{RUN_ID_PREFIX}{now.strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:6]}"
    return build_envelope(payload, row, source_file=DEMO_SOURCE_FILE, file_sha=DEMO_SOURCE_SHA256, row_number=number,
                          run_id=run_id, api_enrichment=enrichment, source_category=USER_DEMO)


def _publish(envelope: dict, topic: str, bootstrap: str, producer=None) -> dict:
    """Send ONE message (same key and producer settings as the replay) and wait for the broker to confirm."""
    from src.producer.dataset_producer import create_producer
    own = producer is None
    try:
        producer = producer or create_producer(bootstrap)
        future = producer.send(topic, key=envelope["payload"]["order_id"], value=envelope,
                               headers=[("ingest_run_id", envelope["ingest_run_id"].encode()),
                                        ("schema_version", SCHEMA_VERSION.encode())])
        meta = future.get(timeout=20)
        producer.flush()
    except Exception as exc:
        log.error("demo publish failed: %s: %s", type(exc).__name__, exc)
        raise DemoOrderError(f"Kafka did not accept the message ({type(exc).__name__}). Is Kafka running?") from None
    finally:
        if own and producer is not None:
            producer.close()
    return {"partition": meta.partition, "offset": meta.offset}


def _log_record(kind: str, envelope: dict, topic: str, position: dict, path) -> dict:
    record = {"kind": kind, "submitted_at": datetime.now(timezone.utc).isoformat(), "topic": topic,
              "order_id": envelope["payload"]["order_id"], "record_uid": envelope["record_uid"],
              "ingest_run_id": envelope["ingest_run_id"], "partition": position["partition"], "offset": position["offset"],
              "api_enriched": envelope["api_enrichment"] is not None, "envelope": envelope}
    append_submission(record, path)
    return record


def submit_demo_order(inp: DemoOrderInput, *, topic: str = KAFKA_TOPIC, bootstrap: str = KAFKA_BOOTSTRAP_SERVERS,
                      log_path=SUBMISSIONS_FILE, producer=None, **prepare_kwargs) -> dict:
    """Validate, enrich, publish and log a new demo order. Returns the log record (with Kafka partition/offset)."""
    envelope = prepare_demo_order(inp, **prepare_kwargs)
    position = _publish(envelope, topic, bootstrap, producer)
    return _log_record("submit", envelope, topic, position, log_path)


def resend_demo_order(order_id: str, *, topic: str = KAFKA_TOPIC, bootstrap: str = KAFKA_BOOTSTRAP_SERVERS,
                      log_path=SUBMISSIONS_FILE, producer=None) -> dict:
    """Publish the IDENTICAL message again (same record_uid): a retry, to show duplicate handling downstream."""
    for record in read_submissions(topic, log_path):
        if record["order_id"] == order_id and record["kind"] == "submit":
            position = _publish(record["envelope"], topic, bootstrap, producer)
            return _log_record("resend", record["envelope"], topic, position, log_path)
    raise DemoOrderError(f"order {order_id} was not submitted from this dashboard on topic {topic}")
