"""Profile every Olist CSV in data/raw and write a reproducible text + JSON report.

Why: the pipeline must be built from the REAL columns and counts, never from
assumptions. Run:  python scripts/profile_dataset.py            (full report)
                   python scripts/profile_dataset.py --summary  (row/column counts only)

The raw files are only read, never modified.
"""
import argparse
import hashlib
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.common.config import DATASET_DIR, EVIDENCE_DIR  # noqa: E402

log = logging.getLogger("profile_dataset")

# HYPOTHESES from the Kaggle documentation (docs/07 section 3). The profiler checks
# whether each key is really unique; the real data wins if it differs.
CANDIDATE_KEYS = {
    "olist_orders_dataset": ["order_id"],
    "olist_order_items_dataset": ["order_id", "order_item_id"],
    "olist_order_payments_dataset": ["order_id", "payment_sequential"],
    "olist_order_reviews_dataset": ["review_id"],
    "olist_customers_dataset": ["customer_id"],
    "olist_products_dataset": ["product_id"],
    "olist_sellers_dataset": ["seller_id"],
    "product_category_name_translation": ["product_category_name"],
}

# Columns with few distinct values are reported as categorical value counts.
MAX_CATEGORICAL = 30


def sha256_of(path: Path) -> str:
    """Hash the file in chunks so large files do not fill memory."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def looks_like_timestamp(name: str) -> bool:
    """Timestamp columns are detected by name, then verified by parsing."""
    return any(token in name for token in ("date", "timestamp", "_at"))


def profile_table(path: Path) -> dict:
    """Return a dictionary of facts about one CSV file."""
    df = pd.read_csv(path, dtype=str, keep_default_na=True, encoding="utf-8")
    info = {
        "file": path.name,
        "size_bytes": path.stat().st_size,
        "sha256": sha256_of(path),
        "rows": int(len(df)),
        "columns": list(df.columns),
        "column_count": int(df.shape[1]),
        "nulls": {c: int(df[c].isna().sum()) for c in df.columns},
        "full_row_duplicates": int(df.duplicated().sum()),
    }

    # Candidate key uniqueness (only if all key columns really exist).
    key = CANDIDATE_KEYS.get(path.stem)
    if key and all(k in df.columns for k in key):
        info["candidate_key"] = key
        info["candidate_key_duplicate_rows"] = int(df.duplicated(subset=key).sum())
        info["candidate_key_distinct"] = int(df.drop_duplicates(subset=key).shape[0])

    # Timestamp columns: parse rate, min, max.
    info["timestamps"] = {}
    for col in df.columns:
        if looks_like_timestamp(col):
            parsed = pd.to_datetime(df[col], errors="coerce", format="%Y-%m-%d %H:%M:%S")
            non_null = int(df[col].notna().sum())
            info["timestamps"][col] = {
                "non_null": non_null,
                "parsed": int(parsed.notna().sum()),
                "unparseable": non_null - int(parsed.notna().sum()),
                "min": str(parsed.min()),
                "max": str(parsed.max()),
            }

    # Numeric columns: try to convert, report min/max/invalid.
    info["numeric"] = {}
    for col in df.columns:
        if col in info["timestamps"] or col.endswith("_id") or "zip" in col:
            continue
        number = pd.to_numeric(df[col], errors="coerce")
        non_null = int(df[col].notna().sum())
        if non_null and number.notna().sum() >= 0.99 * non_null:
            info["numeric"][col] = {
                "non_numeric": non_null - int(number.notna().sum()),
                "min": float(number.min()),
                "max": float(number.max()),
                "negative_values": int((number < 0).sum()),
                "zero_values": int((number == 0).sum()),
            }

    # Categorical columns: value counts for low-cardinality text columns.
    info["categorical"] = {}
    for col in df.columns:
        if col in info["timestamps"] or col in info["numeric"]:
            continue
        distinct = df[col].nunique(dropna=True)
        if 0 < distinct <= MAX_CATEGORICAL:
            info["categorical"][col] = {k: int(v) for k, v in df[col].value_counts(dropna=False).items()}
        else:
            info["categorical"][col] = f"{distinct} distinct values (not listed)"

    info["sample_rows"] = df.head(5).fillna("").to_dict(orient="records")
    info["_df"] = df  # kept only for cross-table checks, removed before saving
    return info


def cross_table_checks(tables: dict) -> dict:
    """Orphan keys, orders without children and timestamp anomalies (docs/07 section 5)."""
    t = {name: info["_df"] for name, info in tables.items()}
    orders = t["olist_orders_dataset"]
    items = t["olist_order_items_dataset"]
    payments = t["olist_order_payments_dataset"]
    reviews = t["olist_order_reviews_dataset"]
    customers = t["olist_customers_dataset"]
    products = t["olist_products_dataset"]
    sellers = t["olist_sellers_dataset"]

    ids = set(orders["order_id"])
    checks = {
        "orders_without_items": len(ids - set(items["order_id"])),
        "orders_without_payments": len(ids - set(payments["order_id"])),
        "orders_without_reviews": len(ids - set(reviews["order_id"])),
        "orders_with_unknown_customer_id": int((~orders["customer_id"].isin(customers["customer_id"])).sum()),
        "items_with_unknown_order_id": int((~items["order_id"].isin(ids)).sum()),
        "items_with_unknown_product_id": int((~items["product_id"].isin(products["product_id"])).sum()),
        "items_with_unknown_seller_id": int((~items["seller_id"].isin(sellers["seller_id"])).sum()),
        "payments_with_unknown_order_id": int((~payments["order_id"].isin(ids)).sum()),
        "reviews_with_unknown_order_id": int((~reviews["order_id"].isin(ids)).sum()),
        "customer_unique_id_repeats_across_customer_ids": int(customers["customer_unique_id"].duplicated().sum()),
    }

    # Timestamp consistency on orders.
    ts = {c: pd.to_datetime(orders[c], errors="coerce") for c in orders.columns if looks_like_timestamp(c)}
    purchase = ts["order_purchase_timestamp"]
    delivered = ts["order_delivered_customer_date"]
    status = orders["order_status"]
    checks["delivered_before_purchase"] = int((delivered < purchase).sum())
    checks["approved_before_purchase"] = int((ts["order_approved_at"] < purchase).sum())
    checks["delivered_status_without_delivered_date"] = int(((status == "delivered") & delivered.isna()).sum())
    checks["non_delivered_status_with_delivered_date"] = int(((status != "delivered") & delivered.notna()).sum())
    checks["delivered_after_estimated_date_(late)"] = int((delivered > ts["order_estimated_delivery_date"]).sum())
    checks["order_status_values"] = {k: int(v) for k, v in status.value_counts().items()}

    # Money checks.
    price = pd.to_numeric(items["price"], errors="coerce")
    freight = pd.to_numeric(items["freight_value"], errors="coerce")
    pay = pd.to_numeric(payments["payment_value"], errors="coerce")
    checks["items_price_le_0"] = int((price <= 0).sum())
    checks["items_negative_freight"] = int((freight < 0).sum())
    checks["payments_value_le_0"] = int((pay <= 0).sum())
    checks["payments_value_zero_by_type"] = {
        k: int(v) for k, v in payments[pay <= 0]["payment_type"].value_counts().items()
    }
    checks["orders_in_orders_file_distinct_order_id"] = int(orders["order_id"].nunique())
    return checks


def render_text(report: dict) -> str:
    """Human-readable version of the report."""
    lines = [
        f"Olist dataset profile - generated {report['generated_at_utc']}",
        f"Dataset directory: {report['dataset_dir']}",
        "",
    ]
    for name, info in report["tables"].items():
        lines += [
            "=" * 78,
            f"{info['file']}  ({info['size_bytes']:,} bytes)",
            f"sha256: {info['sha256']}",
            f"rows: {info['rows']:,}   columns: {info['column_count']}",
            f"columns: {', '.join(info['columns'])}",
            f"full-row duplicates: {info['full_row_duplicates']:,}",
        ]
        if "candidate_key" in info:
            lines.append(
                f"candidate key {info['candidate_key']}: distinct={info['candidate_key_distinct']:,} "
                f"duplicate rows={info['candidate_key_duplicate_rows']:,}"
            )
        lines.append("null counts (non-zero only): " + json.dumps({k: v for k, v in info["nulls"].items() if v}))
        for col, d in info["timestamps"].items():
            lines.append(f"timestamp {col}: non-null={d['non_null']:,} parsed={d['parsed']:,} "
                         f"unparseable={d['unparseable']:,} min={d['min']} max={d['max']}")
        for col, d in info["numeric"].items():
            lines.append(f"numeric {col}: min={d['min']} max={d['max']} non_numeric={d['non_numeric']} "
                         f"negative={d['negative_values']} zero={d['zero_values']}")
        for col, d in info["categorical"].items():
            lines.append(f"categorical {col}: {json.dumps(d, ensure_ascii=False)}")
        lines.append("sample rows (first 5):")
        lines += ["  " + json.dumps(r, ensure_ascii=False) for r in info["sample_rows"]]
        lines.append("")
    lines += ["=" * 78, "CROSS-TABLE CHECKS"]
    lines += [f"{k}: {json.dumps(v)}" for k, v in report["cross_table_checks"].items()]
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input-dir", default=str(DATASET_DIR), help="folder with the Olist CSV files")
    parser.add_argument("--out-dir", default=str(EVIDENCE_DIR / "01_dataset"), help="where to write the report")
    parser.add_argument("--summary", action="store_true", help="print only file/row/column counts")
    parser.add_argument("--no-write", action="store_true", help="print only, do not write report files")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    input_dir = Path(args.input_dir)
    files = sorted(input_dir.glob("*.csv"))
    if not files:
        log.error("No CSV files in %s. See data/raw/README.md", input_dir)
        return 1

    tables = {}
    for path in files:
        log.info("Profiling %s", path.name)
        tables[path.stem] = profile_table(path)

    if args.summary:
        print(f"{'file':45} {'rows':>10} {'cols':>5}")
        for info in tables.values():
            print(f"{info['file']:45} {info['rows']:>10,} {info['column_count']:>5}")
        return 0

    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "dataset_dir": input_dir.name,
        "cross_table_checks": cross_table_checks(tables),
        "tables": tables,
    }
    text = render_text(report)  # needs the tables, so build before dropping dataframes
    for info in report["tables"].values():
        info.pop("_df")

    print(text)
    if not args.no_write:
        out_dir = Path(args.out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "dataset_profile.json").write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
        log.info("Wrote %s", out_dir / "dataset_profile.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
