"""Run the data-quality checks on core.orders and exit 1 if any check finds problem rows.

Same checks as sql/quality_checks.sql, but as a task that FAILS (for Airflow) instead of only printing.
  python scripts/run_quality_checks.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.warehouse.db import get_connection  # noqa: E402

PROBLEM_CHECKS = {
    "duplicate order_id": "SELECT count(*) FROM (SELECT order_id FROM core.orders GROUP BY order_id HAVING count(*) > 1) d",
    "duplicate record_uid": "SELECT count(*) FROM (SELECT record_uid FROM core.orders GROUP BY record_uid HAVING count(*) > 1) d",
    "delivered without delivery date": "SELECT count(*) FROM core.orders WHERE order_status = 'delivered' AND delivered_customer_ts IS NULL",
    "delivery date but status not delivered": "SELECT count(*) FROM core.orders WHERE order_status <> 'delivered' AND delivered_customer_ts IS NOT NULL",
    "delivered before purchase": "SELECT count(*) FROM core.orders WHERE delivered_customer_ts < purchase_ts",
    "negative amounts": "SELECT count(*) FROM core.orders WHERE items_price_total < 0 OR items_freight_total < 0 OR payments_value_total < 0",
    "status outside the allowed set": "SELECT count(*) FROM core.orders WHERE order_status NOT IN ('delivered','shipped','canceled','unavailable','invoiced','processing','created','approved')",
}


def main() -> int:
    conn = get_connection()
    problems = 0
    with conn, conn.cursor() as cur:
        cur.execute("SELECT count(*), count(DISTINCT record_uid) FROM core.orders")
        rows, uids = cur.fetchone()
        print(f"core.orders rows={rows:,} distinct record_uid={uids:,}")
        if rows == 0:
            print("FAILED: core.orders is empty")
            return 1
        for name, sql in PROBLEM_CHECKS.items():
            cur.execute(sql)
            count = cur.fetchone()[0]
            problems += count
            print(f"{'OK     ' if count == 0 else 'PROBLEM'} {name}: {count}")
    conn.close()
    print("RESULT: all quality checks passed" if problems == 0 else f"RESULT: {problems} problem rows found")
    return 0 if problems == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
