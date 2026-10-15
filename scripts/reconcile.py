"""Count reconciliation across every layer (docs/04 section 7), with explicit accounting rules.

TWO KINDS OF NUMBERS ARE KEPT APART
  run-level    : what each ingestion run read / accepted / rejected / published, checked against the
                 slice of the source file that run covered. Runs can overlap (a 100-order test run
                 and a full run both replay the first 100 orders), so run totals are NOT added up
                 as if they were unique records.
  source-level : unique records. Expected values are computed from the source file and the quality
                 rules over the UNION of the slices the runs covered.

  pipeline stages (topic -> Spark -> Parquet -> PostgreSQL) combine both: the topic holds the SUM of
  what the runs published (repeats included); the unique counts are the source-level numbers.

An UNEXPLAINED difference makes the script exit with code 1.

  python scripts/reconcile.py                                      # all non-dry-run runs on KAFKA_TOPIC
  python scripts/reconcile.py --runs limited-100 full-run-01
  python scripts/reconcile.py --topic recovery_test_r2 --stream recovery_r2 \\
        --parquet-dir data/test_runs/recovery_r2/parquet --database streampulse_recovery_r2
"""
import argparse
import json
import sys
from pathlib import Path

import psycopg2
import pyarrow.dataset as ds

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.common.config import (DATASET_DIR, DEAD_LETTER_DIR, FIELD_MAPPING_FILE, INGESTION_RUNS_DIR,  # noqa: E402
                               KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, PARQUET_OUTPUT_DIR, POSTGRES_DB, POSTGRES_HOST,
                               POSTGRES_PASSWORD, POSTGRES_PORT, POSTGRES_USER, PROJECT_ROOT, QUALITY_RULES_FILE)
from src.common.mapping import load_mapping  # noqa: E402
from src.common.orders_source import ORDERS_FILE, build_order_rows  # noqa: E402
from src.processing.classify import classify_record  # noqa: E402
from src.processing.validators import load_rules  # noqa: E402

BATCH_LOG_DIR = PROJECT_ROOT / "data" / "processed" / "spark_batches"


# ------------------------------------------------------------------ pure accounting helpers (unit-tested)
def run_slice(run: dict) -> tuple[int, int]:
    """Replay-order positions [start, end) that a run covered."""
    start = run.get("skip_rows", 0)
    return start, start + run["rows_read"]


def union_positions(runs: list[dict], source_rows: int) -> set[int]:
    covered: set[int] = set()
    for run in runs:
        start, end = run_slice(run)
        covered.update(range(start, min(end, source_rows)))
    return covered


def status_of(row: dict) -> str:
    diff = row["actual"] - row["expected"]
    if diff == 0:
        return "OK"
    return "EXPLAINED" if diff == sum(row.get("explained", {}).values()) else "UNEXPLAINED"


def render(title_rows: list[tuple[str, list[dict]]]) -> tuple[str, bool]:
    """Text table for sections of rows. Returns (text, all_ok)."""
    lines, ok = [], True
    for title, rows in title_rows:
        lines += ["", f"== {title} ==", f"{'Stage':44} {'Expected':>10} {'Actual':>10} {'Difference':>11}  Explanation", "-" * 125]
        for r in rows:
            status = status_of(r)
            ok &= status != "UNEXPLAINED"
            lines.append(f"{r['stage']:44} {r['expected']:>10,} {r['actual']:>10,} {r['actual'] - r['expected']:>+11,}  [{status}] {r['note']}")
    lines += ["", "-" * 125, "RESULT: " + ("all differences are zero or explained" if ok else "UNEXPLAINED MISMATCH FOUND")]
    return "\n".join(lines), ok


# ------------------------------------------------------------------ readers of the real layers
def load_runs(selected, topic) -> list[dict]:
    runs = [json.loads(p.read_text(encoding="utf-8")) for p in sorted(INGESTION_RUNS_DIR.glob("*.json"))]
    # Only runs that replayed the real orders file can be checked against it (a test CSV has other rows).
    runs = [r for r in runs if r["mode"] != "dry-run" and r["topic"] == topic and r["source_file"] == ORDERS_FILE]
    if selected:
        runs = [r for r in runs if r["ingest_run_id"] in selected]
    return sorted(runs, key=lambda r: r["started_at"])


def classify_source() -> dict:
    """Accept/reject every source row with the same rules the producer uses."""
    mapping, rules = load_mapping(FIELD_MAPPING_FILE), load_rules(QUALITY_RULES_FILE)
    frame = build_order_rows(DATASET_DIR, DATASET_DIR / ORDERS_FILE)
    accepted, order_ids = [], []
    for record in frame.to_dict(orient="records"):
        payload, problems, _ = classify_record(record, mapping, rules)
        accepted.append(not problems)
        order_ids.append(payload["order_id"] if payload else None)
    return {"rows": len(frame), "accepted": accepted, "order_ids": order_ids}


def kafka_topic_messages(topic) -> int | None:
    try:
        from kafka import KafkaConsumer, TopicPartition
        consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, request_timeout_ms=10000)
        partitions = [TopicPartition(topic, p) for p in consumer.partitions_for_topic(topic)]
        total = sum(consumer.end_offsets(partitions).values()) - sum(consumer.beginning_offsets(partitions).values())
        consumer.close()
        return int(total)
    except Exception as exc:
        print(f"WARNING: Kafka not reachable: {type(exc).__name__}", file=sys.stderr)
        return None


def spark_batch_totals(stream) -> dict:
    path = BATCH_LOG_DIR / f"{stream}.jsonl"
    if not path.exists():
        return {}
    by_id = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rec = json.loads(line)
            by_id[rec["batch_id"]] = rec  # a re-run batch replaces its earlier log line
    totals = {k: sum(b[k] for b in by_id.values()) for k in ("input_rows", "invalid_rows", "in_batch_duplicates")}
    pg = [b["postgres"] for b in by_id.values() if b.get("postgres")]
    totals.update({f"pg_{k}": sum(p[k] for p in pg) for k in ("inserted", "retry_duplicates", "source_duplicates")})
    totals["batches"] = len(by_id)
    return totals


def parquet_counts(parquet_dir: Path) -> tuple[int, int] | None:
    if not list(parquet_dir.rglob("*.parquet")):
        return None
    uids = ds.dataset(str(parquet_dir), format="parquet", partitioning="hive").to_table(columns=["record_uid"]).column("record_uid").to_pylist()
    return len(uids), len(set(uids))


def postgres_counts(database) -> dict | None:
    if not POSTGRES_PASSWORD:
        print("WARNING: POSTGRES_PASSWORD not set", file=sys.stderr)
        return None
    try:
        conn = psycopg2.connect(host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=database, user=POSTGRES_USER,
                                password=POSTGRES_PASSWORD, connect_timeout=5)
    except Exception as exc:
        print(f"WARNING: PostgreSQL not reachable: {type(exc).__name__}", file=sys.stderr)
        return None
    with conn, conn.cursor() as cur:
        cur.execute("SELECT count(*), count(DISTINCT record_uid), count(DISTINCT order_id), count(DISTINCT business_hash) FROM core.orders")
        rows, uids, orders, hashes = cur.fetchone()
        cur.execute("SELECT count(*) FROM core.duplicate_records WHERE duplicate_type = 'source_duplicate'")
        source_dups = cur.fetchone()[0]
    conn.close()
    return {"rows": rows, "distinct_record_uid": uids, "distinct_order_id": orders, "distinct_business_hash": hashes,
            "source_duplicates_logged": source_dups}


def dead_letter_records(runs: list[dict], dlq_dir: Path) -> list[dict]:
    records = []
    for run in runs:
        path = dlq_dir / "ingest" / f"{run['ingest_run_id']}.jsonl"
        if path.exists():
            records += [json.loads(l) for l in path.read_text(encoding="utf-8").splitlines() if l.strip()]
    return records


# ------------------------------------------------------------------ build the report
def build_sections(runs, stream, topic, parquet_dir, database, dlq_dir) -> list[tuple[str, list[dict]]]:
    source = classify_source()
    accepted, order_ids = source["accepted"], source["order_ids"]
    sections = []

    # ---- run level: every run against ITS OWN slice of the source
    run_rows = []
    for run in runs:
        start, end = run_slice(run)
        end = min(end, source["rows"])
        valid_exp = sum(accepted[start:end])
        name = run["ingest_run_id"]
        full_pass = run["mode"] == "full" and start == 0 and run["status"] == "completed"
        run_rows += [
            {"stage": f"[{name}] rows read", "expected": source["rows"] if full_pass else run["rows_read"], "actual": run["rows_read"],
             "note": "whole orders file" if full_pass else f"slice {start}..{end - 1} (count taken from the run summary)"},
            {"stage": f"[{name}] valid", "expected": valid_exp, "actual": run["rows_valid"], "note": "re-derived from that slice with the same rules"},
            {"stage": f"[{name}] rejected", "expected": (end - start) - valid_exp, "actual": run["rows_rejected"], "note": "read - valid"},
            {"stage": f"[{name}] published = valid", "expected": valid_exp, "actual": run["rows_published"], "note": f"failed publishes: {run['rows_publish_failed']}"},
            {"stage": f"[{name}] dead-letter lines = rejected", "expected": run["rows_rejected"], "actual": len(dead_letter_records([run], dlq_dir)), "note": "every rejected row is stored with its reason"},
        ]
    sections.append(("RUN LEVEL (each run vs the source slice it covered; runs may overlap)", run_rows))

    # ---- source level: unique records over the union of the slices
    covered = union_positions(runs, source["rows"])
    unique_valid = sum(accepted[i] for i in covered)
    unique_rejected = len(covered) - unique_valid
    valid_order_ids = {order_ids[i] for i in covered if accepted[i]}
    sum_read, sum_valid = sum(r["rows_read"] for r in runs), sum(r["rows_valid"] for r in runs)
    all_dlq = dead_letter_records(runs, dlq_dir)
    src_rows = [
        {"stage": "unique source rows covered", "expected": len(covered), "actual": len(covered),
         "note": f"union of run slices; the runs read {sum_read:,} rows in total, so {sum_read - len(covered):,} rows were replayed more than once"},
        {"stage": "unique rejected records (distinct record_uid in DLQ)", "expected": unique_rejected,
         "actual": len({r["record_uid"] for r in all_dlq}),
         "note": f"{len(all_dlq)} dead-letter LINES exist because overlapping runs rejected the same record again; unique records are counted by record_uid"},
    ]
    sections.append(("SOURCE LEVEL (unique records over the union of covered rows)", src_rows))

    # ---- pipeline stages
    pipe = []
    topic_total = kafka_topic_messages(topic)
    published_sum = sum(r["rows_published"] for r in runs)
    repeats = sum_valid - unique_valid
    if topic_total is not None:
        pipe.append({"stage": "4 messages in Kafka topic", "expected": published_sum, "actual": topic_total,
                     "note": f"sum of run publishes; {repeats:,} of them are repeats of an already published record_uid"})
    batches = spark_batch_totals(stream)
    parquet = parquet_counts(parquet_dir)
    if batches:
        pipe.append({"stage": "5 consumed by Spark", "expected": topic_total if topic_total is not None else published_sum, "actual": batches["input_rows"],
                     "note": f"sum of input_rows over {batches['batches']} micro-batches"})
        pipe.append({"stage": "5b malformed/invalid in Spark", "expected": 0, "actual": batches["invalid_rows"], "note": "stage=stream dead letters"})
    if batches and parquet:
        pipe.append({"stage": "6 rows in Parquet (landing layer)", "expected": batches["input_rows"] - batches["invalid_rows"] - batches["in_batch_duplicates"],
                     "actual": parquet[0], "note": f"consumed - invalid - in-batch duplicates ({batches['in_batch_duplicates']}); repeats from later batches are kept here"})
        pipe.append({"stage": "6b distinct record_uid in Parquet", "expected": unique_valid, "actual": parquet[1], "note": "= unique valid source records"})
        pg = postgres_counts(database)
        if pg:
            pipe.append({"stage": "7 rows in PostgreSQL core.orders", "expected": parquet[1], "actual": pg["rows"],
                         "explained": {"source duplicates discarded": -pg["source_duplicates_logged"]},
                         "note": "distinct record_uid in Parquet minus discarded source duplicates"})
            pipe.append({"stage": "7b retry duplicates ignored by PostgreSQL", "expected": parquet[0] - parquet[1], "actual": batches.get("pg_retry_duplicates", 0),
                         "note": "Parquet rows - distinct record_uid = repeats that reached a later batch"})
            pipe.append({"stage": "8 distinct order_id in PostgreSQL", "expected": len(valid_order_ids), "actual": pg["distinct_order_id"],
                         "note": "unique valid order_ids computed from the source"})
            pipe.append({"stage": "8b rows = distinct business_hash", "expected": pg["rows"], "actual": pg["distinct_business_hash"], "note": "one row per natural key"})
    sections.append(("PIPELINE STAGES (Kafka -> Spark -> Parquet -> PostgreSQL)", pipe))
    return sections


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--runs", nargs="*", help="ingestion run ids to include (default: all non-dry-run runs on the topic)")
    parser.add_argument("--topic", default=KAFKA_TOPIC)
    parser.add_argument("--stream", default="orders", help="Spark stream name (batch log file)")
    parser.add_argument("--parquet-dir", default=str(PARQUET_OUTPUT_DIR))
    parser.add_argument("--database", default=POSTGRES_DB)
    parser.add_argument("--dead-letter-dir", default=str(DEAD_LETTER_DIR))
    args = parser.parse_args()
    runs = load_runs(args.runs, args.topic)
    if not runs:
        print("No matching ingestion runs found in", INGESTION_RUNS_DIR)
        return 1
    print(f"Topic: {args.topic}   Database: {args.database}   Runs: {', '.join(r['ingest_run_id'] for r in runs)}")
    text, ok = render(build_sections(runs, args.stream, args.topic, Path(args.parquet_dir), args.database, Path(args.dead_letter_dir)))
    print(text)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
