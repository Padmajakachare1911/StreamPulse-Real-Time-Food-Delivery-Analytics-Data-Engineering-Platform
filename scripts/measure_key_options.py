"""Compare candidate Kafka keys BEFORE publishing (how evenly would 3 partitions fill?).

This uses kafka-python's own default partitioner (murmur2 hash) on the real orders.
It is a COMPUTATION, not a broker measurement. The real distribution is measured
afterwards with scripts/kafka_partition_counts.ps1.
Run:  python scripts/measure_key_options.py
"""
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from kafka.partitioner.default import murmur2  # noqa: E402

from src.common.config import DATASET_DIR  # noqa: E402
from src.common.orders_source import build_order_rows  # noqa: E402

PARTITIONS = [0, 1, 2]


def partition_for(key: str) -> int:
    """Same formula as Kafka's default partitioner: murmur2(key) -> positive -> modulo partitions."""
    return (murmur2(key.encode("utf-8")) & 0x7FFFFFFF) % len(PARTITIONS)


def main() -> int:
    rows = build_order_rows(DATASET_DIR)
    print(f"orders: {len(rows):,}   partitions: {len(PARTITIONS)}   (computed offline, murmur2)\n")
    for key_column in ("order_id", "customer_id", "customer_state"):
        counts = Counter(partition_for(str(k)) for k in rows[key_column])
        total = sum(counts.values())
        cells = "  ".join(f"p{p}={counts[p]:>6,} ({counts[p] / total:5.1%})" for p in PARTITIONS)
        print(f"key={key_column:15} distinct keys={rows[key_column].nunique():>6,}   {cells}")
    print("\nOrdering: a key keeps all messages with the same key in ONE partition, in order.")
    print("order_id: every order is one event, so no per-order ordering is needed -> best spread.")
    print("customer_state: ordering per state, but only 27 keys and SP alone is ~42% -> skewed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
