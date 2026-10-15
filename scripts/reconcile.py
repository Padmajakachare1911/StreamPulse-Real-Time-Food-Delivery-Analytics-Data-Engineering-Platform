"""Count reconciliation across every layer (docs/04 section 7):

  1 rows read -> 2 valid -> 3 rejected -> 4 published to Kafka -> 5 consumed by Spark
  -> 6 written to Parquet -> 7 inserted into PostgreSQL -> 8 unique analytics-ready orders

EXPECTED values are computed from the source file and the quality rules (never guessed).
ACTUAL values are read from the run summaries, Kafka, Spark batch logs, Parquet and PostgreSQL.
A difference that is not explained makes the script exit with code 1.

  python scripts/reconcile.py                          # all non-dry-run ingestion runs
  python scripts/reconcile.py --runs limited-100 full-run-01
"""
import argparse
import json
import sys
from pathlib import Path

import pyarrow.dataset as ds

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.common.config import (DATASET_DIR, DEAD_LETTER_DIR, FIELD_MAPPING_FILE, INGESTION_RUNS_DIR,  # noqa: E402
                               KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, PARQUET_OUTPUT_DIR, PROJECT_ROOT,
                               QUALITY_RULES_FILE)
from src.common.mapping import load_mapping  # noqa: E402
from src.common.orders_source import ORDERS_FILE, build_order_rows  # noqa: E402
from src.processing.classify import classify_record  # noqa: E402
from src.processing.validators import load_rules  # noqa: E402
from src.warehouse.db import get_connection  # noqa: E402

BATCH_LOG_DIR = PROJECT_ROOT / "data" / "processed" / "spark_batches"


def load_runs(selected: list[str] | None) -> list[dict]:
    runs = [json.loads(p.read_text(encoding="utf-8")) for p in sorted(INGESTION_RUNS_DIR.glob("*.json"))]
    runs = [r for r in runs if r["mode"] != "dry-run" and r["topic"] == KAFKA_TOPIC]
    if selected:
        runs = [r for r in runs if r["ingest_run_id"] in selected]
    return sorted(runs, key=lambda r: r["started_at"])


def source_expectations(input_file: Path) -> dict:
    """Re-derive valid/rejected for the source with the SAME rules the producer uses."""
    mapping, rules = load_mapping(FIELD_MAPPING_FILE), load_rules(QUALITY_RULES_FILE)
    frame = build_order_rows(DATASET_DIR, input_file)
    accepted, order_ids = [], []
    for record in frame.to_dict(orient="records"):
        payload, problems, _ = classify_record(record, mapping, rules)
        accepted.append(not problems)
        order_ids.append(payload["order_id"] if payload else None)
    return {"rows": len(frame), "accepted": accepted, "order_ids": order_ids}


def kafka_topic_messages() -> int | None:
    try:
        from kafka import KafkaConsumer, TopicPartition
        consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, request_timeout_ms=10000)
        partitions = [TopicPartition(KAFKA_TOPIC, p) for p in consumer.partitions_for_topic(KAFKA_TOPIC)]
        total = sum(consumer.end_offsets(partitions).values()) - sum(consumer.beginning_offsets(partitions).values())
        consumer.close()
        return int(total)
    except Exception as exc:  # report, never hide
        print(f"WARNING: Kafka not reachable: {type(exc).__name__}", file=sys.stderr)
        return None


def spark_batch_totals(stream: str) -> dict:
    path = BATCH_LOG_DIR / f"{stream}.jsonl"
    if not path.exists():
        return {}
    by_id = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            record = json.loads(line)
            by_id[record["batch_id"]] = record  # a re-run batch replaces its earlier log line
    keys = ("input_rows", "valid_rows", "invalid_rows", "in_batch_duplicates", "parquet_rows_written")
    totals = {k: sum(b[k] for b in by_id.values()) for k in keys}
    pg = [b["postgres"] for b in by_id.values() if b.get("postgres")]
    for k in ("inserted", "retry_duplicates", "source_duplicates"):
        totals[f"pg_{k}"] = sum(p[k] for p in pg)
    totals["batches"] = len(by_id)
    return totals


def parquet_counts() -> tuple[int, int] | None:
    files = list(PARQUET_OUTPUT_DIR.rglob("*.parquet"))
    if not files:
        return None
    table = ds.dataset(str(PARQUET_OUTPUT_DIR), format="parquet", partitioning="hive").to_table(columns=["record_uid"])
    return table.num_rows, len(set(table.column("record_uid").to_pylist()))


def postgres_counts() -> dict | None:
    try:
        conn = get_connection()
    except Exception as exc:
        print(f"WARNING: PostgreSQL not reachable: {type(exc).__name__}", file=sys.stderr)
        return None
    with conn, conn.cursor() as cur:
        cur.execute("SELECT count(*), count(DISTINCT record_uid), count(DISTINCT order_id) FROM core.orders")
        rows, uids, orders = cur.fetchone()
        cur.execute("SELECT count(*) FROM core.duplicate_records WHERE duplicate_type = 'source_duplicate'")
        source_dups = cur.fetchone()[0]
    conn.close()
    return {"rows": rows, "distinct_record_uid": uids, "distinct_order_id": orders, "source_duplicates_logged": source_dups}


def dead_letter_lines(runs: list[dict]) -> int:
    total = 0
    for run in runs:
        path = DEAD_LETTER_DIR / "ingest" / f"{run['ingest_run_id']}.jsonl"
        if path.exists():
            total += sum(1 for line in path.read_text(encoding="utf-8").splitlines() if line.strip())
    return total


def build_rows(runs: list[dict], stream: str) -> list[dict]:
    """Each row: stage, expected, actual, explained (dict label -> amount), note."""
    rows = []
    source = source_expectations(DATASET_DIR / ORDERS_FILE)
    exp_read = exp_valid = 0
    seen_order_ids: set[str] = set()
    for run in runs:
        n = source["rows"] if run["mode"] == "full" else run["rows_read"]  # limited runs: rows replayed in order
        exp_read += n
        exp_valid += sum(source["accepted"][:n])
        seen_order_ids |= {o for o, ok in zip(source["order_ids"][:n], source["accepted"][:n]) if ok}
    exp_rejected = exp_read - exp_valid

    act = lambda key: sum(r[key] for r in runs)  # noqa: E731
    rows.append({"stage": "1 rows read from source", "expected": exp_read, "actual": act("rows_read"),
                 "note": "full runs: rows in the orders file; limited runs: rows replayed"})
    rows.append({"stage": "2 valid (accepted)", "expected": exp_valid, "actual": act("rows_valid"),
                 "note": "expected re-derived from the source with the same quality rules"})
    rows.append({"stage": "3 rejected (producer summary)", "expected": exp_rejected, "actual": act("rows_rejected"),
                 "note": "rejected = read - valid"})
    rows.append({"stage": "3b rejected (dead-letter file lines)", "expected": exp_rejected, "actual": dead_letter_lines(runs),
                 "note": "every rejected row is stored with its reason"})
    rows.append({"stage": "4a published (producer callbacks)", "expected": exp_valid, "actual": act("rows_published"),
                 "note": f"failed publishes: {act('rows_publish_failed')}"})
    topic_total = kafka_topic_messages()
    if topic_total is not None:
        rows.append({"stage": "4b messages in Kafka topic", "expected": act("rows_published"), "actual": topic_total,
                     "note": "end offsets minus start offsets, all partitions"})
    batches = spark_batch_totals(stream)
    if batches:
        rows.append({"stage": "5 consumed by Spark", "expected": topic_total if topic_total is not None else act("rows_published"),
                     "actual": batches["input_rows"], "note": f"sum of input_rows over {batches['batches']} micro-batches"})
        rows.append({"stage": "5b rejected by Spark (malformed)", "expected": 0, "actual": batches["invalid_rows"],
                     "note": "messages the stream could not parse (dead letter, stage=stream)"})
        parquet = parquet_counts()
        if parquet:
            rows.append({"stage": "6 rows in Parquet", "expected": batches["input_rows"] - batches["invalid_rows"] - batches["in_batch_duplicates"],
                         "actual": parquet[0],
                         "note": f"consumed - malformed - in-batch duplicates ({batches['in_batch_duplicates']})"})
            retry_in_parquet = parquet[0] - parquet[1]
            pg = postgres_counts()
            if pg:
                rows.append({"stage": "7 rows in PostgreSQL core.orders", "expected": parquet[1], "actual": pg["rows"],
                             "explained": {"source duplicates logged": -pg["source_duplicates_logged"]},
                             "note": f"distinct record_uid in Parquet; {retry_in_parquet} Parquet rows are retry duplicates (same record_uid, ignored by the upsert)"})
                rows.append({"stage": "7b retry duplicates ignored", "expected": retry_in_parquet, "actual": batches.get("pg_retry_duplicates", 0),
                             "note": "same record_uid seen again (repeated runs); counted by the PostgreSQL writer"})
                rows.append({"stage": "8 unique analytics-ready orders", "expected": len(seen_order_ids), "actual": pg["distinct_order_id"],
                             "note": f"distinct order_id; count(DISTINCT record_uid) = {pg['distinct_record_uid']}"})
    return rows


def render(rows: list[dict]) -> tuple[str, bool]:
    lines = [f"{'Stage':40} {'Expected':>10} {'Actual':>10} {'Difference':>11}  Explanation", "-" * 120]
    ok = True
    for r in rows:
        diff = r["actual"] - r["expected"]
        explained = sum(r.get("explained", {}).values())
        status = "OK" if diff == 0 else ("EXPLAINED" if diff == explained else "UNEXPLAINED")
        ok &= status != "UNEXPLAINED"
        lines.append(f"{r['stage']:40} {r['expected']:>10,} {r['actual']:>10,} {diff:>+11,}  [{status}] {r['note']}")
    lines.append("-" * 120)
    lines.append("RESULT: " + ("all differences are zero or explained" if ok else "UNEXPLAINED MISMATCH FOUND"))
    return "\n".join(lines), ok


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--runs", nargs="*", help="ingestion run ids to include (default: all non-dry-run runs)")
    parser.add_argument("--stream", default="orders", help="Spark stream name (batch log file)")
    args = parser.parse_args()
    runs = load_runs(args.runs)
    if not runs:
        print("No ingestion runs found in", INGESTION_RUNS_DIR)
        return 1
    print(f"Runs included: {', '.join(r['ingest_run_id'] for r in runs)}   topic: {KAFKA_TOPIC}\n")
    text, ok = render(build_rows(runs, args.stream))
    print(text)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
