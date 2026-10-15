"""Batch loaders for everything that is NOT streamed: reference tables, run summaries,
dead letters and the API cache. Each loader is safe to run again (idempotent).

  python -m src.warehouse.load_reference                 # everything
  python -m src.warehouse.load_reference --only api dlq  # selected parts

Reference tables (docs/07 section 4) are loaded in batch, not streamed: products, sellers,
order items and reviews. Geolocation (~1,000,000 rows) is reduced to one row per zip prefix.
"""
import argparse
import json
import logging
import sys
from collections import Counter
from pathlib import Path

import pandas as pd
from psycopg2.extras import Json, execute_values

from src.common.config import DATASET_DIR, DEAD_LETTER_DIR, INGESTION_RUNS_DIR, PROJECT_ROOT
from src.warehouse.db import get_connection

log = logging.getLogger("load_reference")
API_CACHE_DIR = PROJECT_ROOT / "data" / "processed" / "api_cache"
# Rough bounding box of Brazil. Geolocation rows outside it are ignored for centroids (reported, not hidden).
BRAZIL_LAT = (-34.0, 6.0)
BRAZIL_LNG = (-74.5, -34.0)


def _none(value):
    """pandas NaN -> None so PostgreSQL stores NULL."""
    return None if pd.isna(value) else value


def _insert(cur, sql: str, rows: list[tuple], page_size: int = 5000) -> None:
    if rows:
        execute_values(cur, sql, rows, page_size=page_size)


def load_products(conn, dataset_dir: Path) -> dict:
    products = pd.read_csv(dataset_dir / "olist_products_dataset.csv", dtype=str)
    translation = pd.read_csv(dataset_dir / "product_category_name_translation.csv", dtype=str)
    merged = products.merge(translation, on="product_category_name", how="left")
    number_cols = ["product_name_lenght", "product_description_lenght", "product_photos_qty", "product_weight_g",
                   "product_length_cm", "product_height_cm", "product_width_cm"]
    for col in number_cols:
        merged[col] = pd.to_numeric(merged[col], errors="coerce")
    rows = [(r.product_id, _none(r.product_category_name), _none(r.product_category_name_english),
             *[None if pd.isna(getattr(r, c)) else int(getattr(r, c)) for c in number_cols])
            for r in merged.itertuples(index=False)]
    with conn.cursor() as cur:
        _insert(cur, """INSERT INTO core.dim_products (product_id, product_category_name, product_category_name_english,
                        product_name_lenght, product_description_lenght, product_photos_qty, product_weight_g,
                        product_length_cm, product_height_cm, product_width_cm) VALUES %s
                        ON CONFLICT (product_id) DO NOTHING""", rows)
    return {"products_in_file": len(products), "untranslated_category_rows": int(merged["product_category_name_english"].isna().sum())}


def load_sellers(conn, dataset_dir: Path) -> dict:
    sellers = pd.read_csv(dataset_dir / "olist_sellers_dataset.csv", dtype=str)
    rows = [tuple(_none(v) for v in r) for r in sellers.itertuples(index=False)]
    with conn.cursor() as cur:
        _insert(cur, """INSERT INTO core.dim_sellers (seller_id, seller_zip_code_prefix, seller_city, seller_state)
                        VALUES %s ON CONFLICT (seller_id) DO NOTHING""", rows)
    return {"sellers_in_file": len(sellers)}


def load_order_items(conn, dataset_dir: Path) -> dict:
    items = pd.read_csv(dataset_dir / "olist_order_items_dataset.csv", dtype=str)
    rows = [(r.order_id, int(r.order_item_id), r.product_id, r.seller_id, _none(r.shipping_limit_date),
             float(r.price), float(r.freight_value)) for r in items.itertuples(index=False)]
    with conn.cursor() as cur:
        _insert(cur, """INSERT INTO core.order_items (order_id, order_item_id, product_id, seller_id,
                        shipping_limit_date, price, freight_value) VALUES %s
                        ON CONFLICT (order_id, order_item_id) DO NOTHING""", rows)
    return {"order_items_in_file": len(items)}


def load_reviews(conn, dataset_dir: Path) -> dict:
    reviews = pd.read_csv(dataset_dir / "olist_order_reviews_dataset.csv", dtype=str)
    unique = reviews.drop_duplicates(subset=["review_id", "order_id"], keep="first")
    rows = [(r.review_id, r.order_id, None if pd.isna(r.review_score) else int(r.review_score),
             _none(r.review_comment_title), _none(r.review_comment_message),
             _none(r.review_creation_date), _none(r.review_answer_timestamp))
            for r in unique.itertuples(index=False)]
    with conn.cursor() as cur:
        _insert(cur, """INSERT INTO core.order_reviews (review_id, order_id, review_score, review_comment_title,
                        review_comment_message, review_creation_date, review_answer_timestamp) VALUES %s
                        ON CONFLICT (review_id, order_id) DO NOTHING""", rows)
    return {"reviews_in_file": len(reviews), "repeated_review_id_order_id_pairs_skipped": len(reviews) - len(unique)}


def load_geolocation(conn, dataset_dir: Path) -> dict:
    """One centroid per zip prefix, computed chunk by chunk (the file has ~1,000,000 rows)."""
    sums: dict[str, list] = {}   # zip -> [sum_lat, sum_lng, n]
    names: dict[str, Counter] = {}
    total = outside = 0
    for chunk in pd.read_csv(dataset_dir / "olist_geolocation_dataset.csv", dtype={"geolocation_zip_code_prefix": str},
                             chunksize=200_000):
        total += len(chunk)
        inside = chunk["geolocation_lat"].between(*BRAZIL_LAT) & chunk["geolocation_lng"].between(*BRAZIL_LNG)
        outside += int((~inside).sum())
        for zip_code, group in chunk[inside].groupby("geolocation_zip_code_prefix"):
            acc = sums.setdefault(zip_code, [0.0, 0.0, 0])
            acc[0] += group["geolocation_lat"].sum()
            acc[1] += group["geolocation_lng"].sum()
            acc[2] += len(group)
            names.setdefault(zip_code, Counter()).update(zip(group["geolocation_city"], group["geolocation_state"]))
    rows = []
    for zip_code, (lat_sum, lng_sum, n) in sums.items():
        city, state = names[zip_code].most_common(1)[0][0]
        rows.append((zip_code, lat_sum / n, lng_sum / n, city, state, n))
    with conn.cursor() as cur:
        _insert(cur, """INSERT INTO core.geo_zip (zip_code_prefix, lat, lng, city, state, source_rows) VALUES %s
                        ON CONFLICT (zip_code_prefix) DO NOTHING""", rows)
    return {"geolocation_rows_read": total, "rows_outside_brazil_ignored": outside, "zip_prefixes_loaded": len(rows)}


def load_ingestion_runs(conn, runs_dir: Path = INGESTION_RUNS_DIR) -> dict:
    files = sorted(Path(runs_dir).glob("*.json"))
    rows = []
    for path in files:
        run = json.loads(path.read_text(encoding="utf-8"))
        rows.append((run["ingest_run_id"], run["started_at"], run["finished_at"], run["source_file"],
                     run["source_file_sha256"], run["rows_read"], run["rows_valid"], run["rows_rejected"],
                     run["rows_published"], run["rows_publish_failed"], run["mode"]))
    with conn.cursor() as cur:
        _insert(cur, """INSERT INTO core.ingestion_runs (ingest_run_id, started_at, finished_at, source_file,
                        source_file_sha256, rows_read, rows_valid, rows_rejected, rows_published,
                        rows_publish_failed, mode) VALUES %s
                        ON CONFLICT (ingest_run_id) DO UPDATE SET started_at = EXCLUDED.started_at,
                        finished_at = EXCLUDED.finished_at, rows_read = EXCLUDED.rows_read,
                        rows_valid = EXCLUDED.rows_valid, rows_rejected = EXCLUDED.rows_rejected,
                        rows_published = EXCLUDED.rows_published, rows_publish_failed = EXCLUDED.rows_publish_failed,
                        mode = EXCLUDED.mode""", rows)
    return {"ingestion_run_files": len(files)}


def load_dead_letters(conn, dlq_dir: Path = DEAD_LETTER_DIR) -> dict:
    """Replace-per-run: for every (stage, run) found on disk, its rows are deleted and re-inserted,
    so reloading never duplicates and never touches other runs."""
    records = []
    for path in sorted(Path(dlq_dir).rglob("*.jsonl")):
        records += [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    groups = {(r["stage"], r["ingest_run_id"]) for r in records}
    with conn.cursor() as cur:
        for stage, run_id in groups:
            cur.execute("DELETE FROM core.dead_letter_records WHERE stage = %s AND ingest_run_id = %s", (stage, run_id))
        _insert(cur, """INSERT INTO core.dead_letter_records (dlq_id, stage, reason_code, reason_detail, record_uid,
                        ingest_run_id, raw_payload, rejected_at) VALUES %s ON CONFLICT (dlq_id) DO NOTHING""",
                [(r["dlq_id"], r["stage"], r["reason_code"], r["reason_detail"], r.get("record_uid"),
                  r["ingest_run_id"], Json(r["raw_payload"]), r["rejected_at"]) for r in records])
    return {"dead_letter_records_loaded": len(records), "run_groups_replaced": len(groups)}


def load_api_cache(conn, cache_dir: Path = API_CACHE_DIR) -> dict:
    files = sorted(Path(cache_dir).glob("*.json"))
    fx_rows = 0
    with conn.cursor() as cur:
        for path in files:
            data = json.loads(path.read_text(encoding="utf-8"))
            prov = data["provenance"]
            cur.execute("""INSERT INTO core.api_cache (cache_key, api_source_name, api_endpoint, response_json,
                           http_status, retrieved_at) VALUES (%s, %s, %s, %s, %s, %s)
                           ON CONFLICT (cache_key) DO NOTHING""",
                        (prov["api_cache_key"], prov["api_source_name"], prov["api_endpoint"], Json(data["rows"]),
                         prov["api_http_status"], prov["api_retrieved_at"]))
            rates = [(r["date"], r["base"], r["quote"], r["rate"], prov["api_source_name"], prov["api_cache_key"])
                     for r in data["rows"]]
            _insert(cur, """INSERT INTO core.fx_rates (rate_date, base_currency, quote_currency, rate, api_source_name,
                            api_cache_key) VALUES %s ON CONFLICT (rate_date, base_currency, quote_currency) DO NOTHING""", rates)
            fx_rows += len(rates)
    return {"api_cache_files": len(files), "fx_rate_rows_seen": fx_rows}


LOADERS = {
    "products": lambda c: load_products(c, DATASET_DIR),
    "sellers": lambda c: load_sellers(c, DATASET_DIR),
    "items": lambda c: load_order_items(c, DATASET_DIR),
    "reviews": lambda c: load_reviews(c, DATASET_DIR),
    "geo": lambda c: load_geolocation(c, DATASET_DIR),
    "runs": load_ingestion_runs,
    "dlq": load_dead_letters,
    "api": load_api_cache,
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--only", nargs="*", choices=sorted(LOADERS), help="load only these parts")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    result = {}
    for name in args.only or LOADERS:
        conn = get_connection()
        try:
            with conn:  # one transaction per loader
                result[name] = LOADERS[name](conn)
        finally:
            conn.close()
        log.info("%s -> %s", name, result[name])
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
