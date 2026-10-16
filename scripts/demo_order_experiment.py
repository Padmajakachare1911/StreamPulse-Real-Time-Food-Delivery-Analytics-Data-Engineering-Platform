"""End-to-end proof for user-submitted demo orders (real Kafka, real Spark in Docker, real Parquet, real PostgreSQL).

  python scripts/demo_order_experiment.py --tag d1

Isolated like the other experiments: own topic, checkpoint, Parquet, dead-letter folders, submission log and database
(streampulse_demoorder_<tag>). The production checkpoint, topic and database are never touched.

What it proves (every check prints PASS or FAIL; any FAIL exits 1):
  1. a demo order is published through the same producer settings and is visible in Kafka BEFORE Spark has run, and the
     trace honestly says Spark/Parquet and PostgreSQL are still "waiting"
  2. an invalid form input publishes nothing
  3. after one real Spark run, the same order is found in Kafka, Parquet (with its batch_id) and PostgreSQL
  4. demo orders are marked user_demo; real Olist orders sent into the same topic stay kafka_replayed
  5. the historical analytics views exclude the demo orders
  6. sending the identical message again is a retry duplicate: PostgreSQL unchanged, Parquet gets a second row
  7. the real reconciliation passes, listing the demo messages as explained differences
"""
import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from spark_experiments import (INGESTION_RUNS_DIR, PYTHON, ROOT, SparkRun, batch_log, check, create_database, create_topic,  # noqa: F401
                               fail_now, finish, parquet_stats, pg_connect, publish, say, topic_offsets)

from src.demo import demo_order as d
from src.demo import order_trace as t
from src.demo.submissions import read_submissions


def pg_query(db: str, sql: str):
    conn = pg_connect(db)
    with conn, conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    conn.close()
    return rows


def run_spark(name: str, topic: str, stream: str, test_dir: str, db: str) -> SparkRun:
    run = SparkRun(name, topic, stream, test_dir, db, "availableNow", 500)
    if not run.wait_exit(600) or run.proc.returncode != 0:
        run.kill()
        fail_now(f"Spark run {name} failed or timed out: " + " | ".join(run.key_lines()[-3:]))
    return run


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--tag", default=datetime.now().strftime("%m%d%H%M"), help="unique name of this attempt")
    tag = parser.parse_args().tag
    topic, stream, test_dir, db = f"demoorder_test_{tag}", f"demoorder_{tag}", f"demoorder_{tag}", f"streampulse_demoorder_{tag}"
    base = ROOT / "data" / "test_runs" / test_dir
    if base.exists():
        fail_now(f"{base} already exists; choose another --tag")
    base.mkdir(parents=True)
    log_path, parquet_dir = base / "submissions.jsonl", base / "parquet"
    say(f"DEMO-ORDER END-TO-END EXPERIMENT tag={tag} topic={topic} database={db}\n")
    create_topic(topic)
    create_database(db)

    say("Step 1: place two demo orders (real producer settings, real exchange-rate API) while Spark is NOT running")
    orders = []
    for city, state in (("curitiba", "PR"), ("recife", "PE")):
        record = d.submit_demo_order(d.DemoOrderInput(customer_state=state, customer_city=city, customer_zip_code_prefix="80010",
                                                      items_count=2, items_price_total=150.0, items_freight_total=20.0,
                                                      payment_type="credit_card", order_status="approved"),
                                     topic=topic, log_path=log_path)
        orders.append(record)
    first, second = orders[0]["order_id"], orders[1]["order_id"]
    _, end = topic_offsets(topic)
    check("both demo orders are in Kafka (topic end offset 2)", end == 2 and [o["offset"] for o in orders] == [0, 1],
          f"end offset={end} offsets={[o['offset'] for o in orders]}")
    check("demo orders are marked: order_id prefix, user_demo, run id prefix",
          all(o["order_id"].startswith("demo-") and o["envelope"]["source_category"] == "user_demo"
              and o["ingest_run_id"].startswith("demo-order-") for o in orders), f"order ids={first[:12]}..., {second[:12]}...")
    check("the exchange-rate API enriched the orders (real API call)", all(o["api_enriched"] for o in orders),
          f"api_enriched={[o['api_enriched'] for o in orders]} (False means the API was unreachable)")

    before = t.trace_order(first, topic, parquet_dir, stream, db, log_path)
    states = [s.state for s in before["stages"]]
    check("before Spark runs, the trace says: submitted + Kafka done; Parquet and PostgreSQL waiting (not faked)",
          states == ["done", "done", "waiting", "waiting"] and before["state"] == "in_progress", f"stage states={states}")

    say("\nStep 2: an invalid form input publishes nothing")
    try:
        d.submit_demo_order(d.DemoOrderInput(items_price_total=-5.0), topic=topic, log_path=log_path)
        rejected = False
    except d.DemoOrderError as exc:
        rejected = "items_price_total" in str(exc)
    _, end_after = topic_offsets(topic)
    check("negative price is refused with a clear reason; Kafka offset and log unchanged",
          rejected and end_after == 2 and len(read_submissions(topic, log_path)) == 2, f"end offset={end_after}")

    say("\nStep 3: mix in real historical Olist orders (replay rows 0..49) in the same topic, then run Spark once")
    hist = publish(topic, f"demoorder-hist-{tag}", 0, 50)
    spark1 = run_spark(f"demoorder_spark1_{tag}", topic, stream, test_dir, db)
    spark1.save_log(base / "spark_run1_full.log")
    logs = batch_log(stream)
    consumed = sum(b["input_rows"] for b in logs)
    check("Spark consumed demo + historical messages", consumed == 2 + hist["rows_published"],
          f"consumed={consumed} expected={2 + hist['rows_published']}")

    say("\nStep 4: the same order is visible at every stage")
    for label, oid in (("first", first), ("second", second)):
        result = t.trace_order(oid, topic, parquet_dir, stream, db, log_path)
        check(f"{label} demo order: all four stages done", result["state"] == "complete" and all(s.state == "done" for s in result["stages"]),
              " | ".join(f"{s.name}={s.state}" for s in result["stages"]))
    pg = t.postgres_row(first, db)
    pq = t.parquet_rows(first, parquet_dir)
    kafka = t.kafka_message(topic, orders[0]["partition"], orders[0]["offset"])
    check("Kafka position == Parquet position == PostgreSQL position for the same order",
          (kafka["partition"], kafka["offset"]) == (pq[0]["kafka_partition"], pq[0]["kafka_offset"]) == (pg["kafka_partition"], pg["kafka_offset"]),
          f"kafka={kafka['partition']}/{kafka['offset']} parquet={pq[0]['kafka_partition']}/{pq[0]['kafka_offset']} postgres={pg['kafka_partition']}/{pg['kafka_offset']}")
    check("the Parquet batch_id matches a real Spark BATCH_SUMMARY line", t.spark_batch(pq[0]["batch_id"], stream) is not None,
          f"batch_id={pq[0]['batch_id']} summary={t.spark_batch(pq[0]['batch_id'], stream)}")

    say("\nStep 5: demo orders are distinguishable from historical records")
    mix = dict(pg_query(db, "SELECT source_category, count(*) FROM core.orders GROUP BY 1"))
    check("PostgreSQL holds 2 user_demo rows and the historical rows as kafka_replayed",
          mix == {"user_demo": 2, "kafka_replayed": hist["rows_published"]}, f"source_category counts={mix}")
    prefixed = pg_query(db, "SELECT count(*) FROM core.orders WHERE order_id LIKE 'demo-%' AND source_category <> 'user_demo'")[0][0]
    check("no historical row carries a demo- id, no demo row lacks it", prefixed == 0 and
          pg_query(db, "SELECT count(*) FROM core.orders WHERE source_category = 'user_demo' AND order_id NOT LIKE 'demo-%'")[0][0] == 0,
          f"mismarked rows={prefixed}")
    hist_view = pg_query(db, "SELECT orders FROM analytics.revenue_summary")[0][0]
    check("historical KPI view excludes the demo orders", hist_view == hist["rows_published"],
          f"analytics.revenue_summary.orders={hist_view} (core.orders has {hist['rows_published'] + 2})")

    say("\nStep 6: send the identical message again (a retry) and run Spark again")
    again = d.resend_demo_order(first, topic=topic, log_path=log_path)
    spark2 = run_spark(f"demoorder_spark2_{tag}", topic, stream, test_dir, db)
    spark2.save_log(base / "spark_run2_full.log")
    last = batch_log(stream)[-1]["postgres"]
    check("the resend is a retry duplicate: nothing inserted", last["retry_duplicates"] == 1 and last["inserted"] == 0,
          f"batch postgres result={last}; resend offset={again['offset']}")
    total = pg_query(db, "SELECT count(*), count(*) FILTER (WHERE order_id = %s) FROM core.orders" % repr(first))[0]
    check("PostgreSQL still has ONE row for the order and the same total", total == (2 + hist["rows_published"], 1), f"total, this order={total}")
    check("Parquet (append-only landing layer) now has TWO rows for the order",
          len(t.parquet_rows(first, parquet_dir)) == 2, f"batch_ids={[r['batch_id'] for r in t.parquet_rows(first, parquet_dir)]}")
    pq_stats = parquet_stats(parquet_dir)
    check("Parquet distinct record_uid equals the PostgreSQL row count", pq_stats["distinct"] == 2 + hist["rows_published"],
          f"parquet rows={pq_stats['rows']} distinct={pq_stats['distinct']}")

    say("\nStep 7: the real reconciliation accounts for the demo messages")
    cmd = [PYTHON, "scripts/reconcile.py", "--runs", f"demoorder-hist-{tag}", "--topic", topic, "--stream", stream,
           "--parquet-dir", str(parquet_dir), "--database", db, "--demo-log", str(log_path)]
    out = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, env={"PYTHONUTF8": "1", **__import__("os").environ}, timeout=300)
    (base / "reconcile_output.txt").write_text(out.stdout, encoding="utf-8")
    check("reconcile.py exits 0: all differences zero or explained", out.returncode == 0 and "all differences are zero or explained" in out.stdout,
          f"exit code {out.returncode}")
    check("the demo messages are listed as explained, not hidden",
          "user-submitted demo message(s)" in out.stdout and "8c demo orders in PostgreSQL" in out.stdout, "see reconcile_output.txt")
    say("\n" + "\n".join(line for line in out.stdout.splitlines() if line.startswith(("4 ", "6b", "7 ", "8", "RESULT"))))
    finish()


if __name__ == "__main__":
    main()
