"""Run the order join + field mapping over the whole dataset and print the counts.

Shows how many orders map cleanly and how many are rejected (with reasons), so the
numbers in docs/data_quality_report.md come from a real run.
Run:  python scripts/check_join_mapping.py
"""
import json
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.common.config import DATASET_DIR, FIELD_MAPPING_FILE  # noqa: E402
from src.common.mapping import apply_mapping, load_mapping  # noqa: E402
from src.common.orders_source import ORDERS_FILE, build_order_rows, join_report  # noqa: E402


def main() -> int:
    mapping = load_mapping(FIELD_MAPPING_FILE)
    rows = build_order_rows(DATASET_DIR)
    orders_in_file = sum(1 for _ in open(DATASET_DIR / ORDERS_FILE, encoding="utf-8")) - 1

    print("JOIN REPORT")
    print(json.dumps(join_report(rows, orders_in_file), indent=2))

    ok, rejected, reasons = 0, 0, Counter()
    for record in rows.to_dict(orient="records"):
        payload, errors = apply_mapping(record, mapping)
        if payload is None:
            rejected += 1
            for e in errors:
                reasons[f"{e['reason_code']}:{e['field']}"] += 1
        else:
            ok += 1

    print("\nMAPPING RESULT")
    print(f"rows mapped ok: {ok}")
    print(f"rows rejected by mapping: {rejected}")
    print(f"reasons: {dict(reasons)}")
    print(f"first replayed order purchase time: {rows['order_purchase_timestamp'].iloc[0]}")
    print(f"last replayed order purchase time:  {rows['order_purchase_timestamp'].iloc[-1]}")
    print(f"customer_state counts (top 5): {rows['customer_state'].value_counts().head(5).to_dict()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
