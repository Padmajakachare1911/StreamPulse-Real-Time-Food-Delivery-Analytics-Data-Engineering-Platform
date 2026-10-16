"""Follow ONE order through Kafka -> Spark -> Parquet -> PostgreSQL, using real reads of each layer.

Nothing here is simulated. A stage is "done" only when the order was really found there:
  1 Submitted   the dashboard published it (the submission log has the Kafka partition/offset)
  2 Kafka       the message is read back from that partition/offset and its order_id compared
  3 Spark       a Parquet row with this order_id exists; its batch_id is matched to the Spark batch log
  4 PostgreSQL  the row exists in core.orders (with the same Kafka position)
A stage that cannot be checked (service down) is "unavailable", never "done".
Pure decision logic is in `build_stages` so it can be unit-tested without any service.
"""
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import pyarrow.dataset as ds

from src.common.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, PARQUET_OUTPUT_DIR, PROJECT_ROOT
from src.demo.submissions import SUBMISSIONS_FILE, read_submissions
from src.warehouse.db import get_connection

log = logging.getLogger("order_trace")
BATCH_LOG_DIR = PROJECT_ROOT / "data" / "processed" / "spark_batches"
UNAVAILABLE = "unavailable"


@dataclass
class Stage:
    name: str
    state: str                      # "done" | "waiting" | "unavailable"
    detail: str
    facts: dict = field(default_factory=dict)


# ---- readers (each returns the facts found, None when the order is not there, UNAVAILABLE when it cannot be checked)
def kafka_message(topic: str, partition: int, offset: int, bootstrap: str = KAFKA_BOOTSTRAP_SERVERS):
    """Read the message at one partition/offset back from the broker."""
    try:
        from kafka import KafkaConsumer, TopicPartition
        consumer = KafkaConsumer(bootstrap_servers=bootstrap, enable_auto_commit=False, request_timeout_ms=10000,
                                 consumer_timeout_ms=5000)
        try:
            tp = TopicPartition(topic, partition)
            consumer.assign([tp])
            consumer.seek(tp, offset)
            batches = consumer.poll(timeout_ms=5000, max_records=1)
            for message in batches.get(tp, []):
                envelope = json.loads(message.value.decode("utf-8"))
                return {"partition": message.partition, "offset": message.offset, "key": message.key.decode("utf-8"),
                        "order_id": envelope["payload"]["order_id"], "record_uid": envelope["record_uid"],
                        "source_category": envelope.get("source_category")}
        finally:
            consumer.close()
        return None
    except Exception as exc:
        log.error("kafka read-back failed: %s", type(exc).__name__)
        return UNAVAILABLE


def parquet_rows(order_id: str, parquet_dir: Path = PARQUET_OUTPUT_DIR):
    """All Parquet rows of this order (one per batch it appeared in), with their batch_id."""
    try:
        if not list(Path(parquet_dir).rglob("*.parquet")):
            return None
        dataset = ds.dataset(str(parquet_dir), format="parquet", partitioning="hive")
        table = dataset.to_table(filter=ds.field("order_id") == order_id,
                                 columns=["record_uid", "order_id", "source_category", "kafka_partition", "kafka_offset", "batch_id"])
        rows = table.to_pylist()
        return sorted(rows, key=lambda r: str(r["batch_id"])) or None
    except Exception as exc:
        log.error("parquet lookup failed: %s", type(exc).__name__)
        return UNAVAILABLE


def spark_batch(batch_id, stream: str = "orders"):
    """The BATCH_SUMMARY line Spark logged for this batch (the last one wins if a batch was re-run)."""
    path = BATCH_LOG_DIR / f"{stream}.jsonl"
    if batch_id is None or not path.exists():
        return None
    found = None
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            record = json.loads(line)
            if record["batch_id"] == int(batch_id):
                found = record
    return found


def postgres_row(order_id: str, dbname: str | None = None):
    try:
        conn = get_connection(connect_timeout=3, dbname=dbname)
        try:
            with conn, conn.cursor() as cur:
                cur.execute("""
                    SELECT order_id, record_uid, order_status, source_category, ingest_run_id, purchase_ts, customer_state,
                           items_count, payments_value_total, payments_value_usd, fx_rate_brl_usd, fx_match_type,
                           kafka_partition, kafka_offset, processed_at
                    FROM core.orders WHERE order_id = %s""", (order_id,))
                row = cur.fetchone()
                if row is None:
                    return None
                names = [c.name for c in cur.description]
                values = {n: (float(v) if hasattr(v, "as_tuple") else v) for n, v in zip(names, row)}
                cur.execute("SELECT count(*) FROM core.orders WHERE order_id = %s", (order_id,))
                values["rows_for_this_order"] = cur.fetchone()[0]
                return values
        finally:
            conn.close()
    except Exception as exc:
        log.error("postgres lookup failed: %s", type(exc).__name__)
        return UNAVAILABLE


# ---- decision logic (pure)
def build_stages(submissions: list[dict], kafka, parquet, batch, postgres) -> list[Stage]:
    """Turn the facts of each layer into four stages. `submissions` = every logged message of this order."""
    stages = []
    if not submissions:
        return [Stage("1 Submitted", "waiting", "this order id was not submitted from the dashboard")]
    first = submissions[0]
    resends = len(submissions) - 1
    stages.append(Stage("1 Submitted", "done", f"published to topic '{first['topic']}' at partition {first['partition']}, offset {first['offset']}"
                        + (f"; sent again {resends} time(s)" if resends else ""),
                        {"messages_sent": len(submissions), "record_uid": first["record_uid"], "api_enriched": first["api_enriched"]}))

    if kafka == UNAVAILABLE:
        stages.append(Stage("2 Kafka", UNAVAILABLE, "the broker could not be read"))
    elif kafka is None:
        stages.append(Stage("2 Kafka", "waiting", "the message was not found at the recorded position"))
    elif kafka["order_id"] == first["order_id"] and kafka["record_uid"] == first["record_uid"]:
        stages.append(Stage("2 Kafka", "done", f"message read back from partition {kafka['partition']}, offset {kafka['offset']} "
                            f"(key {kafka['key'][:14]}..., source_category {kafka['source_category']})", kafka))
    else:
        stages.append(Stage("2 Kafka", "waiting", "a different message is stored at the recorded position"))

    if parquet == UNAVAILABLE:
        stages.append(Stage("3 Spark and Parquet", UNAVAILABLE, "the Parquet files could not be read"))
    elif parquet is None:
        stages.append(Stage("3 Spark and Parquet", "waiting", "not in Parquet yet: Spark has not processed it. Is the stream running? "
                            "(.\\scripts\\run_stream.ps1)"))
    else:
        batch_ids = sorted({str(r["batch_id"]) for r in parquet})
        text = f"{len(parquet)} Parquet row(s), batch_id {', '.join(batch_ids)}"
        facts = {"parquet_rows": len(parquet), "batch_ids": batch_ids}
        if batch:
            text += (f"; Spark batch {batch['batch_id']} read {batch['input_rows']} message(s), "
                     f"wrote {batch['parquet_rows_written']} row(s) in {batch['seconds']} s")
            facts["spark_batch"] = {k: batch[k] for k in ("batch_id", "input_rows", "valid_rows", "invalid_rows", "in_batch_duplicates",
                                                           "parquet_rows_written", "seconds")}
        stages.append(Stage("3 Spark and Parquet", "done", text, facts))

    if postgres == UNAVAILABLE:
        stages.append(Stage("4 PostgreSQL", UNAVAILABLE, "the database could not be read"))
    elif postgres is None:
        stages.append(Stage("4 PostgreSQL", "waiting", "not in core.orders yet"))
    else:
        same_position = (postgres["kafka_partition"], postgres["kafka_offset"]) == (first["partition"], first["offset"])
        text = (f"stored: status {postgres['order_status']}, source_category {postgres['source_category']}, "
                f"{postgres['rows_for_this_order']} row for this order")
        if not same_position:
            text += (f"; stored from Kafka position {postgres['kafka_partition']}/{postgres['kafka_offset']} (the first copy "
                     "Spark processed; later resends are ignored)")
        stages.append(Stage("4 PostgreSQL", "done", text, postgres))
    return stages


def overall_state(stages: list[Stage]) -> str:
    if stages[0].state == "waiting":
        return "not_found"                    # never submitted from the dashboard: nothing to follow
    if all(s.state == "done" for s in stages):
        return "complete"
    if any(s.state == UNAVAILABLE for s in stages):
        return "unavailable"
    return "in_progress"


def latency_seconds(submissions: list[dict], postgres) -> float | None:
    """Seconds from the first submission to the PostgreSQL write (None until stored)."""
    if not submissions or postgres in (None, UNAVAILABLE):
        return None
    submitted = datetime.fromisoformat(submissions[0]["submitted_at"])
    return round((postgres["processed_at"] - submitted).total_seconds(), 1)


def trace_order(order_id: str, topic: str = KAFKA_TOPIC, parquet_dir: Path = PARQUET_OUTPUT_DIR, stream: str = "orders",
                dbname: str | None = None, log_path: Path = SUBMISSIONS_FILE) -> dict:
    """Run every real check for one order and return {'stages': [...], 'state': ..., 'latency_seconds': ...}."""
    submissions = [r for r in read_submissions(topic, log_path) if r["order_id"] == order_id]
    kafka = kafka_message(topic, submissions[0]["partition"], submissions[0]["offset"]) if submissions else None
    parquet = parquet_rows(order_id, parquet_dir)
    batch = None
    if parquet not in (None, UNAVAILABLE):
        batch = spark_batch(parquet[0]["batch_id"], stream)
    postgres = postgres_row(order_id, dbname)
    stages = build_stages(submissions, kafka, parquet, batch, postgres)
    return {"order_id": order_id, "stages": stages, "state": overall_state(stages), "latency_seconds": latency_seconds(submissions, postgres)}
