"""Check that the Olist files are present and unchanged (used by the Airflow DAG and for Demo 1).

  python scripts/check_dataset_fingerprint.py --files-only   # only check the 9 files exist
  python scripts/check_dataset_fingerprint.py                # also compare SHA-256 + row count with the Phase 1 profile

The reference values come from docs/evidence/01_dataset/dataset_profile.json (a real profiling run).
Exit code 1 means a file is missing or differs from what was profiled.
"""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.common.config import DATASET_DIR, EVIDENCE_DIR  # noqa: E402
from src.common.orders_source import file_sha256  # noqa: E402

EXPECTED_FILES = [
    "olist_customers_dataset.csv", "olist_geolocation_dataset.csv", "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv", "olist_order_reviews_dataset.csv", "olist_orders_dataset.csv",
    "olist_products_dataset.csv", "olist_sellers_dataset.csv", "product_category_name_translation.csv",
]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--files-only", action="store_true", help="only check that the files exist")
    args = parser.parse_args()

    missing = [name for name in EXPECTED_FILES if not (DATASET_DIR / name).exists()]
    for name in EXPECTED_FILES:
        print(f"{'OK     ' if name not in missing else 'MISSING'} {name}")
    if missing:
        print(f"FAILED: {len(missing)} file(s) missing in {DATASET_DIR.name}/ (see data/raw/README.md)")
        return 1
    if args.files_only:
        print("All 9 files are present.")
        return 0

    profile = json.loads((EVIDENCE_DIR / "01_dataset" / "dataset_profile.json").read_text(encoding="utf-8"))
    bad = 0
    for table in profile["tables"].values():
        path = DATASET_DIR / table["file"]
        sha = file_sha256(path)
        same = sha == table["sha256"] and path.stat().st_size == table["size_bytes"]
        bad += not same
        print(f"{'MATCH   ' if same else 'DIFFERS '} {table['file']}  sha256={sha[:16]}...  size={path.stat().st_size:,}")
    if bad:
        print(f"FAILED: {bad} file(s) differ from the profiled dataset")
        return 1
    print("All 9 files match the fingerprints recorded when the dataset was profiled.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
