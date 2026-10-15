"""Spark Structured Streaming job: Kafka -> parse + validate -> Parquet (+ PostgreSQL).

Runs inside the Spark container (host Spark needs winutils.exe on Windows):
  powershell> .\scripts\run_stream.ps1                       # continuous, Ctrl+C or stop_stream.ps1 to stop
  powershell> .\scripts\run_stream.ps1 -Trigger availableNow  # finite: process what is in Kafka, then exit

Why foreachBatch: one place per micro-batch to split valid/invalid, remove duplicates,
write files and log the counts of every stage.

Delivery guarantee (docs/04 section 3): Kafka offsets + Spark checkpoint give at-least-once
processing. A batch that is re-run after a crash overwrites its own Parquet folder
(batch_id=NNNNNNNN) and its own dead-letter file, so the results stay the same.
"""
import argparse
import json
import logging
import signal
import sys
import time
from pathlib import Path

from pyspark.errors import StreamingQueryException
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common.config import (CHECKPOINT_DIR, DEAD_LETTER_DIR, FIELD_MAPPING_FILE, KAFKA_BOOTSTRAP_SERVERS,
                               KAFKA_TOPIC, PARQUET_OUTPUT_DIR, PROJECT_ROOT, SPARK_APP_NAME,
                               SPARK_MAX_OFFSETS_PER_TRIGGER)
from src.common.mapping import load_mapping
from src.processing.deadletter import DeadLetterWriter, make_dead_letter
from src.streaming.transforms import dedup_in_batch, flatten_valid, parse_messages, split_valid_invalid

log = logging.getLogger("order_stream")
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"  # must match PySpark 4.2.0 / Scala 2.13
BATCH_LOG_DIR = PROJECT_ROOT / "data" / "processed" / "spark_batches"

# Shared state for the safe stop. Spark has no public "stop between two micro-batches" call, and
# query.stop() interrupts the stream thread at ANY moment (even the instant a batch starts). So the stop
# works in two steps instead: (1) "stopping" is set, a batch already running finishes normally, and any
# batch that STARTS afterwards raises StopRequested before it does any work; Spark then ends the query
# by itself at that boundary. (2) Only when the stream is idle is query.stop() used.
STATE = {"in_batch": False, "stopping": False}


class StopRequested(Exception):
    """Raised at the start of a batch after a stop was requested. Nothing of that batch was done or committed;
    its offsets are processed after the restart."""


def make_batch_processor(mapping: dict, stream_name: str, parquet_dir: Path, dead_letter_dir: Path,
                         postgres_writer=None):
    """Return the function Spark calls for every micro-batch."""
    BATCH_LOG_DIR.mkdir(parents=True, exist_ok=True)

    def process_batch(batch_df, batch_id: int) -> None:
        started = time.time()
        # Order matters: raise "in batch" FIRST, then read "stopping". Either the batch sees the stop request and
        # does nothing, or the stopper sees "in batch" and waits for it: there is no gap in between.
        STATE["in_batch"] = True
        if STATE["stopping"]:
            STATE["in_batch"] = False
            raise StopRequested(f"stop requested: batch {batch_id} was not started and will run after the restart")
        batch_df.persist()
        try:
            input_rows = batch_df.count()
            if input_rows == 0:
                log.info("batch %d is empty", batch_id)
                return

            offsets = [{"partition": r["partition"], "first_offset": r["lo"], "last_offset": r["hi"]}
                       for r in batch_df.groupBy("partition").agg(F.min("offset").alias("lo"), F.max("offset").alias("hi"))
                       .orderBy("partition").collect()]

            valid, invalid = split_valid_invalid(parse_messages(batch_df, mapping))
            flat = flatten_valid(valid, mapping)
            unique = dedup_in_batch(flat).persist()
            try:
                valid_rows = flat.count()
                unique_rows = unique.count()

                # 1) bad messages -> dead-letter file (one file per batch, replaced if the batch is re-run)
                bad = invalid.select("reason_code", "json_str", "kafka_partition", "kafka_offset").collect()
                if bad:
                    writer = DeadLetterWriter(dead_letter_dir, "stream", f"{stream_name}-batch-{batch_id:08d}")
                    for row in bad:
                        writer.write(make_dead_letter(
                            "stream", row["reason_code"],
                            f"could not use message at partition {row['kafka_partition']} offset {row['kafka_offset']}",
                            row["json_str"], stream_name))

                # 2) valid unique rows -> Parquet. Each batch owns one folder, so a re-run overwrites it.
                unique.coalesce(1).write.mode("overwrite").parquet(str(parquet_dir / f"batch_id={batch_id:08d}"))

                # 3) optional PostgreSQL upsert (added in Phase 6)
                postgres_result = postgres_writer(unique.collect(), batch_id) if postgres_writer else None

                summary = {
                    "batch_id": batch_id, "input_rows": input_rows, "valid_rows": valid_rows,
                    "invalid_rows": len(bad), "in_batch_duplicates": valid_rows - unique_rows,
                    "parquet_rows_written": unique_rows, "postgres": postgres_result,
                    "offsets": offsets, "seconds": round(time.time() - started, 2),
                }
                log.info("BATCH_SUMMARY %s", json.dumps(summary))
                with (BATCH_LOG_DIR / f"{stream_name}.jsonl").open("a", encoding="utf-8") as handle:
                    handle.write(json.dumps(summary) + "\n")
            finally:
                unique.unpersist()
        finally:
            batch_df.unpersist()
            STATE["in_batch"] = False

    return process_batch


def build_spark() -> SparkSession:
    return (SparkSession.builder.appName(SPARK_APP_NAME).master("local[2]")
            .config("spark.jars.packages", KAFKA_PACKAGE)
            .config("spark.sql.shuffle.partitions", "4")   # small data: the default 200 would create tiny tasks
            .config("spark.ui.enabled", "false")
            .config("spark.driver.memory", "2g")
            .getOrCreate())


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--stream-name", default="orders", help="names the checkpoint and log files")
    parser.add_argument("--topic", default=KAFKA_TOPIC)
    parser.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP_SERVERS)
    parser.add_argument("--starting-offsets", default="earliest", choices=["earliest", "latest"],
                        help="used only on the FIRST run; later runs continue from the checkpoint")
    parser.add_argument("--max-offsets-per-trigger", type=int, default=SPARK_MAX_OFFSETS_PER_TRIGGER,
                        help="limit messages per micro-batch (0 = no limit)")
    parser.add_argument("--trigger", default="5 seconds",
                        help="'availableNow' (finite run) or a processing interval such as '5 seconds'")
    parser.add_argument("--parquet-dir", default=str(PARQUET_OUTPUT_DIR))
    parser.add_argument("--checkpoint-dir", default=str(CHECKPOINT_DIR))
    parser.add_argument("--dead-letter-dir", default=str(DEAD_LETTER_DIR))
    parser.add_argument("--postgres", action="store_true", help="also upsert into PostgreSQL (core.orders)")
    return parser.parse_args(argv)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = parse_args()
    mapping = load_mapping(FIELD_MAPPING_FILE)
    checkpoint = Path(args.checkpoint_dir) / args.stream_name
    stop_file = Path(args.checkpoint_dir) / f"STOP_{args.stream_name}"
    stop_file.unlink(missing_ok=True)  # a stale stop request from an earlier run must not stop this run
    stop_requested = {"flag": False}
    signal.signal(signal.SIGTERM, lambda *_: stop_requested.update(flag=True))

    postgres_writer = None
    if args.postgres:
        from src.warehouse.load import write_batch  # imported only when needed
        postgres_writer = write_batch

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    log.info("Starting stream '%s': topic=%s startingOffsets=%s (first run only) trigger=%s checkpoint=%s",
             args.stream_name, args.topic, args.starting_offsets, args.trigger, checkpoint)

    reader = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", args.bootstrap)
              .option("subscribe", args.topic)
              .option("startingOffsets", args.starting_offsets))
    if args.max_offsets_per_trigger > 0:
        reader = reader.option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)

    writer = (reader.load().writeStream
              .foreachBatch(make_batch_processor(mapping, args.stream_name, Path(args.parquet_dir),
                                                 Path(args.dead_letter_dir), postgres_writer))
              .option("checkpointLocation", str(checkpoint)))
    writer = writer.trigger(availableNow=True) if args.trigger == "availableNow" else writer.trigger(processingTime=args.trigger)
    query = writer.start()

    try:
        # Check every 2 seconds for Ctrl+C / SIGTERM / the stop file written by stop_stream.ps1.
        while query.isActive:
            if stop_requested["flag"] or stop_file.exists():
                log.info("Stop requested: the running batch (if any) finishes; no new batch will start")
                STATE["stopping"] = True
                wait_until = time.time() + 120
                while STATE["in_batch"] and time.time() < wait_until:
                    time.sleep(0.2)
                if STATE["in_batch"]:
                    log.warning("Batch still running after 120 s: stopping anyway; it will be re-run after a restart")
                # With data waiting, the next batch raises StopRequested and Spark ends the query by itself.
                for _ in range(20):
                    if not query.isActive:
                        break
                    time.sleep(0.25)
                if query.isActive:                  # idle between batches: nothing is running, so stop() is safe
                    query.stop()
                break
            try:
                query.awaitTermination(2)
            except StreamingQueryException as exc:
                if "StopRequested" not in str(exc):
                    raise                           # a real failure: do not hide it
                break
        if STATE["stopping"]:
            log.info("STOPPED_AT_BATCH_BOUNDARY: all started batches are committed; the rest runs after the restart")
    except KeyboardInterrupt:
        log.info("Ctrl+C received: stopping the stream")
        STATE["stopping"] = True
        query.stop()
    finally:
        stop_file.unlink(missing_ok=True)
        spark.stop()
    log.info("Stream stopped.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
