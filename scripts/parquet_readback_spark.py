"""Read the Parquet output back WITH SPARK (run inside the Spark container) and compare with the batch logs.

  docker compose --profile spark run --rm --no-deps -T spark python scripts/parquet_readback_spark.py
"""
import json
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[1]
PARQUET = ROOT / "data" / "processed" / "parquet"
BATCH_LOG = ROOT / "data" / "processed" / "spark_batches" / "orders.jsonl"


def main() -> int:
    spark = SparkSession.builder.master("local[2]").appName("parquet-readback").config("spark.ui.enabled", "false").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    df = spark.read.parquet(str(PARQUET))

    per_batch = {r["batch_id"]: r["n"] for r in df.groupBy("batch_id").agg(F.count("*").alias("n")).collect()}
    logged = {}
    for line in BATCH_LOG.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rec = json.loads(line)
            logged[rec["batch_id"]] = rec["parquet_rows_written"]  # a re-run batch replaces its older line

    print(f"Spark read total rows        : {df.count():,}")
    print(f"Distinct record_uid          : {df.select('record_uid').distinct().count():,}")
    print(f"Distinct order_id            : {df.select('order_id').distinct().count():,}")
    print("batch_id | parquet rows | batch-log parquet_rows_written | match")
    ok = True
    for batch_id in sorted(set(per_batch) | set(logged)):
        a, b = per_batch.get(batch_id, 0), logged.get(batch_id, 0)
        ok &= a == b
        print(f"{batch_id:>8} | {a:>12,} | {b:>30,} | {'OK' if a == b else 'MISMATCH'}")
    print("RESULT:", "every batch folder matches its batch log" if ok else "MISMATCH FOUND")
    df.printSchema()
    spark.stop()
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
