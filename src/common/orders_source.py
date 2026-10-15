"""Build one denormalized order row per order_id from the Olist CSV files.

Join design (docs/07 section 4):
  orders  LEFT JOIN customers            on customer_id   (1 customer row per customer_id)
  orders  LEFT JOIN items aggregated     on order_id      (count, sum price, sum freight)
  orders  LEFT JOIN payments aggregated  on order_id      (count, sum value, payment types)

LEFT joins are used on purpose: orders without items or payments are NOT dropped.
They get count 0 and are reported by `join_report` so nobody loses rows silently.

Rows are returned in historical order (purchase timestamp), which is what the
"historical dataset replay" follows.
"""
import hashlib
from pathlib import Path

import pandas as pd

ORDERS_FILE = "olist_orders_dataset.csv"
CUSTOMERS_FILE = "olist_customers_dataset.csv"
ITEMS_FILE = "olist_order_items_dataset.csv"
PAYMENTS_FILE = "olist_order_payments_dataset.csv"


def file_sha256(path: Path) -> str:
    """SHA-256 of a file, read in chunks. Used for record identity (docs/04)."""
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _read(dataset_dir: Path, name: str) -> pd.DataFrame:
    # dtype=str keeps zip codes like "01037" and avoids silent type guessing.
    return pd.read_csv(Path(dataset_dir) / name, dtype=str)


def build_order_rows(dataset_dir: Path, orders_path: Path | None = None) -> pd.DataFrame:
    """Return the denormalized order rows (all values still text) in replay order.

    `orders_path` defaults to the real orders file inside `dataset_dir`. A different
    file with the same columns (for example the isolated Demo 3 test file) can be
    given; customers, items and payments are always read from `dataset_dir`.

    `source_row_number` is the 1-based position of the order in the orders file
    (header not counted). It never changes, even though rows are sorted for replay.
    """
    orders = pd.read_csv(orders_path or Path(dataset_dir) / ORDERS_FILE, dtype=str)
    orders["source_row_number"] = range(1, len(orders) + 1)

    customers = _read(dataset_dir, CUSTOMERS_FILE)
    items = _read(dataset_dir, ITEMS_FILE)
    payments = _read(dataset_dir, PAYMENTS_FILE)

    items["price"] = pd.to_numeric(items["price"], errors="coerce")
    items["freight_value"] = pd.to_numeric(items["freight_value"], errors="coerce")
    item_totals = items.groupby("order_id").agg(
        items_count=("order_item_id", "count"),
        items_price_total=("price", "sum"),
        items_freight_total=("freight_value", "sum"),
    )

    payments["payment_value"] = pd.to_numeric(payments["payment_value"], errors="coerce")
    payment_totals = payments.groupby("order_id").agg(
        payments_count=("payment_sequential", "count"),
        payments_value_total=("payment_value", "sum"),
        payment_types=("payment_type", lambda s: ",".join(sorted(set(s)))),
    )

    rows = orders.merge(customers, on="customer_id", how="left")
    rows = rows.merge(item_totals, left_on="order_id", right_index=True, how="left")
    rows = rows.merge(payment_totals, left_on="order_id", right_index=True, how="left")

    # Orders without child rows: counts and totals become 0 (reported, not dropped).
    for col in ("items_count", "items_price_total", "items_freight_total",
                "payments_count", "payments_value_total"):
        rows[col] = rows[col].fillna(0)
    rows["items_count"] = rows["items_count"].astype(int)
    rows["payments_count"] = rows["payments_count"].astype(int)

    # Replay order: purchase time, ties broken by file position (stable and repeatable).
    rows = rows.sort_values(["order_purchase_timestamp", "source_row_number"], kind="mergesort")
    return rows.reset_index(drop=True)


def join_report(rows: pd.DataFrame, orders_row_count: int) -> dict:
    """Counts that prove the join did not lose or duplicate orders."""
    return {
        "orders_in_file": orders_row_count,
        "order_rows_after_join": int(len(rows)),
        "join_changed_row_count": int(len(rows)) != orders_row_count,
        "orders_without_items": int((rows["items_count"] == 0).sum()),
        "orders_without_payments": int((rows["payments_count"] == 0).sum()),
        "orders_without_customer_row": int(rows["customer_unique_id"].isna().sum()),
    }
