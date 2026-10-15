"""Data-quality, duplicate and idempotency experiment (Phase 5 / Demo 3).

  python scripts/quality_experiment.py --tag q1

Uses the isolated test file data/test_inputs/demo3_records.csv, written in the REAL Olist orders schema:
  row 1  valid order            (dedicated test id demo3_order_valid_001, real customer)
  row 2  invalid order          (delivered date BEFORE the purchase date)
  row 3  exact copy of row 1    (same order_id -> a source duplicate)
The real Kaggle files are never modified. The experiment is isolated like spark_experiments.py
(own topic, checkpoint, Parquet, dead-letter copy and PostgreSQL database).
Every check prints PASS or FAIL; any FAIL makes the script exit with code 1.
"""
import argparse
import json
import shutil
import sys
from datetime import datetime
from pathlib import Path

from spark_experiments import (INGESTION_RUNS_DIR, RESULTS, ROOT, SparkRun, batch_log, check, create_database, create_topic,  # noqa: F401
                               fail_now, finish, parquet_stats, pg_connect, pg_counts, publish, say)

SOURCE = "data/test_inputs/demo3_records.csv"


def read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--tag", default=datetime.now().strftime("%m%d%H%M"), help="unique name of this attempt")
    tag = parser.parse_args().tag
    topic, stream, test_dir, db = f"quality_test_{tag}", f"quality_{tag}", f"quality_{tag}", f"streampulse_quality_{tag}"
    base = ROOT / "data" / "test_runs" / test_dir
    if base.exists():
        fail_now(f"{base} already exists; choose another --tag")
    say(f"DATA-QUALITY / IDEMPOTENCY EXPERIMENT tag={tag} input={SOURCE} topic={topic} database={db}\n")
    create_topic(topic)
    create_database(db)

    say("Step 1: first ingestion of the Demo 3 file")
    run1 = publish(topic, f"q-run1-{tag}", 0, 10, SOURCE)
    check("producer: 3 read = 2 valid + 1 rejected, 2 published",
          (run1["rows_read"], run1["rows_valid"], run1["rows_rejected"], run1["rows_published"]) == (3, 2, 1, 2),
          f"read={run1['rows_read']} valid={run1['rows_valid']} rejected={run1['rows_rejected']} published={run1['rows_published']}")
    check("producer: exact duplicate counted as a source duplicate, not dropped",
          run1["source_duplicates_in_input"] == 1 and run1["rejection_reasons"] == {"RANGE_ERROR": 1},
          f"source_duplicates_in_input={run1['source_duplicates_in_input']} rejection_reasons={run1['rejection_reasons']}")
    dlq_file = ROOT / "data" / "dead_letter" / "ingest" / f"q-run1-{tag}.jsonl"
    dlq = read_jsonl(dlq_file)
    required = {"dlq_id", "stage", "reason_code", "reason_detail", "record_uid", "ingest_run_id", "raw_payload", "rejected_at"}
    check("dead-letter record has every required field", len(dlq) == 1 and required <= set(dlq[0]),
          f"fields={sorted(dlq[0]) if dlq else None}")
    check("dead-letter reason is specific and names the rule",
          dlq[0]["reason_code"] == "RANGE_ERROR" and "delivered_not_before_purchase" in dlq[0]["reason_detail"]
          and dlq[0]["stage"] == "ingest" and dlq[0]["raw_payload"]["order_id"] == "demo3_order_invalid_002",
          f"reason_code={dlq[0]['reason_code']} detail={dlq[0]['reason_detail'][:120]}")

    spark_args = (topic, stream, test_dir, db, "availableNow", 500)
    s1 = SparkRun(f"quality_spark1_{tag}", *spark_args)
    if not s1.wait_exit(600) or s1.proc.returncode != 0:
        s1.kill()
        fail_now("first Spark run failed or timed out: " + " | ".join(s1.key_lines()[-3:]))
    s1.save_log(base / "spark_run1_full.log")
    pq, pg, logs = parquet_stats(base / "parquet"), pg_counts(db), batch_log(stream)
    check("Parquet landing layer keeps both valid rows (different record_uid)", pq["rows"] == 2 and pq["distinct"] == 2,
          f"rows={pq['rows']} distinct record_uid={pq['distinct']}")
    check("PostgreSQL keeps ONE row for the valid order", pg["rows"] == 1 and pg["distinct_order_id"] == 1,
          f"rows={pg['rows']} distinct order_id={pg['distinct_order_id']}")
    check("second copy is classified as a SOURCE duplicate and logged once",
          pg["duplicate_records_logged"] == 1 and logs[0]["postgres"]["source_duplicates"] == 1 and logs[0]["postgres"]["retry_duplicates"] == 0,
          f"duplicate_records={pg['duplicate_records_logged']} batch postgres result={logs[0]['postgres']}")

    say("\nStep 2: load run summaries and dead letters into the test database")
    from src.warehouse.load_reference import load_dead_letters, load_ingestion_runs
    stage_dir = base / "loader_input"
    (stage_dir / "dlq" / "ingest").mkdir(parents=True)
    (stage_dir / "runs").mkdir()
    shutil.copy(dlq_file, stage_dir / "dlq" / "ingest")
    shutil.copy(INGESTION_RUNS_DIR / f"q-run1-{tag}.json", stage_dir / "runs")
    conn = pg_connect(db)
    with conn:
        load_dead_letters(conn, stage_dir / "dlq")
        load_ingestion_runs(conn, stage_dir / "runs")
    with conn, conn.cursor() as cur:
        cur.execute("SELECT stage, reason_code, record_uid IS NOT NULL, raw_payload ->> 'order_id' FROM core.dead_letter_records")
        rows = cur.fetchall()
    check("core.dead_letter_records holds the rejected order with its reason",
          rows == [("ingest", "RANGE_ERROR", True, "demo3_order_invalid_002")], f"rows={rows}")

    say("\nStep 3: re-ingest the SAME file (new run id) and process it again with the same stream and checkpoint")
    publish(topic, f"q-run2-{tag}", 0, 10, SOURCE)
    dlq2 = read_jsonl(ROOT / "data" / "dead_letter" / "ingest" / f"q-run2-{tag}.jsonl")
    check("same row gets the same record_uid in the re-run (deterministic identity)",
          dlq2[0]["record_uid"] == dlq[0]["record_uid"], f"run1={dlq[0]['record_uid']} run2={dlq2[0]['record_uid']}")
    s2 = SparkRun(f"quality_spark2_{tag}", *spark_args)
    if not s2.wait_exit(600) or s2.proc.returncode != 0:
        s2.kill()
        fail_now("second Spark run failed or timed out")
    s2.save_log(base / "spark_run2_full.log")
    logs = batch_log(stream)
    pg2, pq2 = pg_counts(db), parquet_stats(base / "parquet")
    second = logs[-1]["postgres"]
    check("re-ingestion leaves the PostgreSQL unique count unchanged", pg2["rows"] == 1 and pg2["distinct_record_uid"] == 1,
          f"rows={pg2['rows']} (was {pg['rows']})")
    check("both replayed records are retry duplicates; nothing inserted or newly classified",
          second["retry_duplicates"] == 2 and second["inserted"] == 0 and second["source_duplicates"] == 0, f"second batch postgres result={second}")
    check("the discarded copy is not logged a second time", pg2["duplicate_records_logged"] == 1,
          f"duplicate_records={pg2['duplicate_records_logged']}")
    check("Parquet landing layer is append-oriented: 4 rows, still 2 distinct record_uid",
          pq2["rows"] == 4 and pq2["distinct"] == 2, f"rows={pq2['rows']} distinct={pq2['distinct']}")

    shutil.copy(ROOT / "data" / "dead_letter" / "ingest" / f"q-run2-{tag}.jsonl", stage_dir / "dlq" / "ingest")
    shutil.copy(INGESTION_RUNS_DIR / f"q-run2-{tag}.json", stage_dir / "runs")
    with conn:
        load_dead_letters(conn, stage_dir / "dlq")
        load_dead_letters(conn, stage_dir / "dlq")          # loading twice must not duplicate anything
        load_ingestion_runs(conn, stage_dir / "runs")
    with conn, conn.cursor() as cur:
        cur.execute("SELECT count(*), count(DISTINCT record_uid) FROM core.dead_letter_records")
        dl_rows, dl_distinct = cur.fetchone()
        cur.execute("SELECT count(*) FROM core.ingestion_runs")
        runs_rows = cur.fetchone()[0]
    conn.close()
    check("dead letters: 1 row per run (2) for 1 distinct rejected record; loading twice adds nothing",
          (dl_rows, dl_distinct) == (2, 1), f"rows={dl_rows} distinct record_uid={dl_distinct}")
    check("both ingestion runs are recorded in core.ingestion_runs", runs_rows == 2, f"rows={runs_rows}")
    say(f"\nFinal: Kafka messages=4  Parquet rows={pq2['rows']} (distinct {pq2['distinct']})  PostgreSQL orders={pg2['rows']}  "
        f"duplicate_records={pg2['duplicate_records_logged']}")
    finish()


if __name__ == "__main__":
    main()
