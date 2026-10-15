"""Replay the real Olist orders into Kafka: a HISTORICAL DATASET REPLAY, not live orders.

Steps for every order: join -> map types -> validate -> (enrich with the real API)
-> wrap in an envelope (docs/04) -> publish to Kafka. Rejected records go to the
dead-letter folder with a reason; nothing is dropped silently.

Examples (PowerShell, from the repo root):
  python -m src.producer.dataset_producer --limit 100 --dry-run
  python -m src.producer.dataset_producer --limit 200 --run-id demo1
  python -m src.producer.dataset_producer --full --rate 2000
"""
import argparse
import hashlib
import json
import logging
import sys
import time
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from src.api.client import ApiError, FrankfurterClient
from src.api.enrich import build_rate_table, enrich_payload
from src.common.config import (API_BASE_URL, API_USER_AGENT, BATCH_SIZE, DATASET_DIR, DEAD_LETTER_DIR,
                               FIELD_MAPPING_FILE, INGESTION_RUNS_DIR, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC,
                               PROJECT_ROOT, QUALITY_RULES_FILE, REPLAY_RATE)
from src.common.mapping import load_mapping
from src.common.orders_source import ORDERS_FILE, build_order_rows, file_sha256
from src.processing.deadletter import DeadLetterWriter, make_dead_letter
from src.processing.classify import classify_record
from src.processing.validators import load_rules

log = logging.getLogger("dataset_producer")
SCHEMA_VERSION = "1.0"
API_CACHE_DIR = PROJECT_ROOT / "data" / "processed" / "api_cache"


def make_record_uid(file_sha256_value: str, row_number: int) -> str:
    """Same file + same row => same ID, so a re-run can be recognised downstream."""
    return hashlib.sha256(f"{file_sha256_value}:{row_number}".encode()).hexdigest()[:32]


def make_business_hash(order_id: str) -> str:
    """order_id is a verified natural key (no duplicates in the real orders file)."""
    return hashlib.sha256(f"order_id:{order_id}".encode()).hexdigest()[:32]


def clean_row(row: dict) -> dict:
    """Make a pandas row JSON-safe: NaN -> None, numpy numbers -> Python numbers."""
    cleaned = {}
    for key, value in row.items():
        if value is None or (isinstance(value, float) and value != value):
            cleaned[key] = None
        elif hasattr(value, "item"):
            cleaned[key] = value.item()
        else:
            cleaned[key] = value
    return cleaned


def build_envelope(payload: dict, raw: dict, *, source_file: str, file_sha: str, row_number: int,
                   run_id: str, api_enrichment: dict | None) -> dict:
    """The Kafka message value (contract in docs/04 section 1)."""
    return {
        "schema_version": SCHEMA_VERSION,
        "record_uid": make_record_uid(file_sha, row_number),
        "business_hash": make_business_hash(payload["order_id"]),
        "source_category": "kafka_replayed",
        "source_file": source_file,
        "source_file_sha256": file_sha,
        "source_row_number": row_number,
        "ingest_run_id": run_id,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
        "raw": raw,
        "api_enrichment": api_enrichment,
    }


def load_rate_table(rows: list[dict], enrich: bool) -> tuple[dict, dict]:
    """Fetch (or read from cache) BRL->USD rates for the date span of these orders."""
    if not enrich or not rows:
        return {}, {}
    days = sorted(r["order_purchase_timestamp"][:10] for r in rows if r.get("order_purchase_timestamp"))
    try:
        client = FrankfurterClient(API_BASE_URL, API_USER_AGENT, cache_dir=API_CACHE_DIR)
        result = client.get_rates("BRL", "USD", days[0], days[-1])
        return build_rate_table(result), result.provenance
    except ApiError as exc:
        log.warning("API enrichment unavailable, continuing without it: %s", exc)
        return {}, {}


def create_producer(bootstrap: str):
    """Producer settings explained in docs/implementation_plan.md (acks=all + idempotence)."""
    from kafka import KafkaProducer
    return KafkaProducer(
        bootstrap_servers=bootstrap,
        acks="all",                      # wait for the broker to confirm the write
        enable_idempotence=True,         # broker ignores duplicates caused by producer retries
        retries=5,
        retry_backoff_ms=200,
        linger_ms=20,                    # wait briefly so messages travel in batches
        batch_size=64 * 1024,
        compression_type="gzip",
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
    )


def run(args: argparse.Namespace) -> dict:
    """Process the input file and return the run summary."""
    started = datetime.now(timezone.utc)
    run_id = args.run_id or f"run-{started.strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:6]}"
    input_path = Path(args.input)
    mapping = load_mapping(FIELD_MAPPING_FILE)
    rules = load_rules(QUALITY_RULES_FILE)
    file_sha = file_sha256(input_path)

    log.info("Run %s | input=%s | sha256=%s", run_id, input_path.name, file_sha[:12])
    frame = build_order_rows(DATASET_DIR, input_path)
    records = frame.to_dict(orient="records")
    skip = getattr(args, "skip", 0)          # replay-order position to start from (disjoint slices for tests)
    if skip:
        records = records[skip:]
    if args.limit:
        records = records[: args.limit]

    rate_table, provenance = load_rate_table(records, enrich=not args.no_enrich)
    dead_letters = DeadLetterWriter(DEAD_LETTER_DIR, "ingest", run_id)
    producer = None if args.dry_run else create_producer(args.bootstrap)

    counts = Counter()
    partitions: Counter = Counter()
    reasons: Counter = Counter()
    seen_hashes: set[str] = set()
    interrupted = False

    def on_success(metadata) -> None:
        counts["published"] += 1
        partitions[metadata.partition] += 1

    def on_error(exc) -> None:
        counts["publish_failed"] += 1
        log.error("Kafka delivery failed: %s", exc)

    batch_started = time.monotonic()
    try:
        for record in records:
            counts["read"] += 1
            row = clean_row(record)
            row_number = int(row["source_row_number"])

            payload, problems, warnings = classify_record(row, mapping, rules)
            counts["warned"] += 1 if warnings else 0

            if problems:
                counts["rejected"] += 1
                reasons[problems[0]["reason_code"]] += 1
                dead_letters.write(make_dead_letter(
                    "ingest", problems[0]["reason_code"], "; ".join(p["detail"] for p in problems),
                    row, run_id, make_record_uid(file_sha, row_number)))
                continue

            counts["valid"] += 1
            business_hash = make_business_hash(payload["order_id"])
            if business_hash in seen_hashes:
                counts["source_duplicate_in_input"] += 1   # still published; the sink keeps the first
            seen_hashes.add(business_hash)

            enrichment = enrich_payload(payload, rate_table, provenance) if rate_table else None
            counts["enriched" if enrichment else "not_enriched"] += 1
            envelope = build_envelope(payload, row, source_file=input_path.name, file_sha=file_sha,
                                      row_number=row_number, run_id=run_id, api_enrichment=enrichment)

            if producer is not None:
                key = str(payload[args.key_field])
                future = producer.send(args.topic, key=key, value=envelope,
                                       headers=[("ingest_run_id", run_id.encode()), ("schema_version", SCHEMA_VERSION.encode())])
                future.add_callback(on_success)
                future.add_errback(on_error)

            if counts["valid"] % args.batch_size == 0:
                if producer is not None:
                    producer.flush()
                log.info("progress: read=%d valid=%d rejected=%d published=%d",
                         counts["read"], counts["valid"], counts["rejected"], counts["published"])
                if args.rate > 0:                       # pace the replay: records per second
                    wait = args.batch_size / args.rate - (time.monotonic() - batch_started)
                    if wait > 0:
                        time.sleep(wait)
                batch_started = time.monotonic()
    except KeyboardInterrupt:
        interrupted = True
        log.warning("Ctrl+C received: flushing and writing the summary")
    finally:
        if producer is not None:
            producer.flush()
            producer.close()

    summary = {
        "ingest_run_id": run_id,
        "started_at": started.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "status": "interrupted" if interrupted else "completed",
        "mode": "dry-run" if args.dry_run else ("full" if args.full else "limited"),
        "topic": args.topic,
        "key_field": args.key_field,
        "source_file": input_path.name,
        "source_file_sha256": file_sha,
        "skip_rows": getattr(args, "skip", 0),
        "rows_read": counts["read"],
        "rows_valid": counts["valid"],
        "rows_rejected": counts["rejected"],
        "rows_with_warnings": counts["warned"],
        "rows_published": counts["published"],
        "rows_publish_failed": counts["publish_failed"],
        "rows_enriched": counts["enriched"],
        "rows_not_enriched": counts["not_enriched"],
        "source_duplicates_in_input": counts["source_duplicate_in_input"],
        "rejection_reasons": dict(reasons),
        "partition_counts_from_delivery_callbacks": {str(k): v for k, v in sorted(partitions.items())},
        "dead_letter_file": str(dead_letters.path.relative_to(PROJECT_ROOT)) if dead_letters.count else None,
        "api": {k: provenance.get(k) for k in ("api_source_name", "api_http_status", "api_retrieved_at", "cache_hit")} if provenance else None,
    }
    INGESTION_RUNS_DIR.mkdir(parents=True, exist_ok=True)
    (INGESTION_RUNS_DIR / f"{run_id}.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input", default=str(DATASET_DIR / ORDERS_FILE), help="orders CSV (default: real orders file)")
    parser.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP_SERVERS, help="Kafka bootstrap servers")
    parser.add_argument("--topic", default=KAFKA_TOPIC, help="Kafka topic")
    parser.add_argument("--limit", type=int, default=0, help="send only the first N orders in replay order")
    parser.add_argument("--skip", type=int, default=0, help="skip the first N orders in replay order (use with --limit)")
    parser.add_argument("--full", action="store_true", help="send every order (needs this flag on purpose)")
    parser.add_argument("--dry-run", action="store_true", help="validate and count, publish nothing")
    parser.add_argument("--rate", type=float, default=REPLAY_RATE, help="records per second, 0 = as fast as possible")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE, help="flush/progress every N valid records")
    parser.add_argument("--run-id", default=None, help="name of this ingestion run (default: generated)")
    parser.add_argument("--key-field", default="order_id", help="canonical payload field used as the Kafka key")
    parser.add_argument("--no-enrich", action="store_true", help="skip the exchange-rate API enrichment")
    args = parser.parse_args(argv)
    if not args.limit and not args.full:
        parser.error("choose --limit N or --full (so a full replay is never started by accident)")
    return args


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    summary = run(parse_args())
    print(json.dumps(summary, indent=2))
    return 0 if summary["rows_publish_failed"] == 0 and summary["status"] == "completed" else 1


if __name__ == "__main__":
    sys.exit(main())
