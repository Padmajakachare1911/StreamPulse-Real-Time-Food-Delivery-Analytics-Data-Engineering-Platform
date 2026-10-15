"""Read the Parquet output back and print counts, files and a sample.

Reads with pyarrow, so it works on the Windows host without Spark.
  python scripts/parquet_readback.py
  python scripts/parquet_readback.py --sample 5
"""
import argparse
import sys
from pathlib import Path

import pyarrow.dataset as ds

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.common.config import PARQUET_OUTPUT_DIR  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dir", default=str(PARQUET_OUTPUT_DIR))
    parser.add_argument("--sample", type=int, default=3, help="number of sample rows to print")
    args = parser.parse_args()

    root = Path(args.dir)
    files = sorted(root.rglob("*.parquet"))
    if not files:
        print(f"No Parquet files under {root}")
        return 1

    dataset = ds.dataset(str(root), format="parquet", partitioning="hive")
    table = dataset.to_table()
    frame = table.to_pandas()

    print(f"Parquet folder        : {root.name}")
    print(f"Parquet files         : {len(files)} in {frame['batch_id'].nunique()} batch folders")
    print(f"Total size            : {sum(f.stat().st_size for f in files):,} bytes")
    print(f"Rows (count)          : {len(frame):,}")
    print(f"Distinct record_uid   : {frame['record_uid'].nunique():,}")
    print(f"Distinct order_id     : {frame['order_id'].nunique():,}")
    print(f"Rows with API enrichment: {int(frame['api_enriched'].sum()):,}")
    print(f"Purchase time range   : {frame['purchase_ts'].min()} .. {frame['purchase_ts'].max()}")
    print("Rows per ingest_run_id:", frame["ingest_run_id"].value_counts().to_dict())
    print("Schema:")
    for field in table.schema:
        print(f"   {field.name}: {field.type}")
    print(f"First {args.sample} rows (selected columns):")
    cols = ["record_uid", "order_id", "order_status", "purchase_ts", "customer_state", "payments_value_total",
            "payments_value_usd", "kafka_partition", "kafka_offset"]
    print(frame[cols].head(args.sample).to_string(index=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
