"""Isolated Spark experiments that PROVE recovery and dead-letter behaviour.

  python scripts/spark_experiments.py recovery  [--tag t1]   # stop mid-backlog, publish more, restart, verify
  python scripts/spark_experiments.py malformed [--tag t1]   # malformed messages reach the stream dead-letter sink

Isolation (the production checkpoint data/checkpoints/orders is never touched):
  * own Kafka topic (1 partition, so offsets are easy to read), own stream name
  * own checkpoint / Parquet / dead-letter folders under data/test_runs/<tag>/
  * own PostgreSQL database streampulse_<experiment>_<tag>
Data: real Olist orders sent by the real producer (slices chosen with --skip/--limit).
Every check prints PASS or FAIL; any FAIL (or a timeout) makes the script exit with code 1.
"""
import argparse
import json
import os
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

import psycopg2
import pyarrow.dataset as ds

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.common.config import (INGESTION_RUNS_DIR, KAFKA_BOOTSTRAP_SERVERS, POSTGRES_HOST, POSTGRES_PASSWORD,  # noqa: E402
                               POSTGRES_PORT, POSTGRES_USER)

PYTHON = sys.executable
RESULTS: list[tuple[str, bool, str]] = []


def say(text: str = "") -> None:
    print(text, flush=True)


def check(name: str, ok: bool, detail: str) -> bool:
    RESULTS.append((name, ok, detail))
    say(f"[{'PASS' if ok else 'FAIL'}] {name}: {detail}")
    return ok


def fail_now(reason: str) -> None:
    check("experiment aborted", False, reason)
    finish()


def finish() -> None:
    bad = [r for r in RESULTS if not r[1]]
    say("")
    say(f"SUMMARY: {len(RESULTS) - len(bad)} passed, {len(bad)} failed")
    say("RESULT: " + ("EXPERIMENT PASSED" if not bad else "EXPERIMENT FAILED"))
    sys.exit(1 if bad else 0)


# ---------------------------------------------------------------- Kafka helpers
def create_topic(topic: str) -> None:
    out = subprocess.run(["docker", "exec", "streampulse-kafka", "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server",
                          "localhost:9092", "--create", "--if-not-exists", "--topic", topic, "--partitions", "1",
                          "--replication-factor", "1"], capture_output=True, text=True)
    if out.returncode != 0:
        fail_now(f"could not create topic {topic}: {out.stderr.strip()[:200]}")


def topic_offsets(topic: str) -> tuple[int, int]:
    """(start offset, end offset) of partition 0."""
    from kafka import KafkaConsumer, TopicPartition
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, request_timeout_ms=10000)
    tp = TopicPartition(topic, 0)
    start, end = consumer.beginning_offsets([tp])[tp], consumer.end_offsets([tp])[tp]
    consumer.close()
    return start, end


def publish(topic: str, run_id: str, skip: int, limit: int) -> dict:
    """Publish real orders with the real producer; return its run summary."""
    cmd = [PYTHON, "-m", "src.producer.dataset_producer", "--topic", topic, "--skip", str(skip), "--limit", str(limit),
           "--run-id", run_id]
    out = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, env={**os.environ, "PYTHONUTF8": "1"})
    summary_file = INGESTION_RUNS_DIR / f"{run_id}.json"
    if out.returncode != 0 or not summary_file.exists():
        fail_now(f"producer run {run_id} failed: {out.stderr[-300:]}")
    summary = json.loads(summary_file.read_text(encoding="utf-8"))
    say(f"  published run={run_id} skip={skip} limit={limit}: read={summary['rows_read']} valid={summary['rows_valid']} "
        f"rejected={summary['rows_rejected']} published={summary['rows_published']} failed={summary['rows_publish_failed']}")
    return summary


# ---------------------------------------------------------------- Spark container runner
class SparkRun:
    """Starts the Spark container and reads stdout AND stderr (merged) line by line."""

    def __init__(self, name: str, topic: str, stream: str, test_dir: str, db: str | None, trigger: str,
                 max_offsets: int):
        cmd = ["docker", "compose", "--profile", "spark", "run", "--rm", "--no-deps", "-T", "--name", name,
               "-e", f"KAFKA_TOPIC={topic}"]
        if db:
            cmd += ["-e", f"POSTGRES_DB={db}"]
        cmd += ["spark", "python", "-m", "src.streaming.order_stream", "--stream-name", stream, "--topic", topic,
                "--checkpoint-dir", f"/app/data/test_runs/{test_dir}/checkpoints",
                "--parquet-dir", f"/app/data/test_runs/{test_dir}/parquet",
                "--dead-letter-dir", f"/app/data/test_runs/{test_dir}/dead_letter",
                "--max-offsets-per-trigger", str(max_offsets), "--trigger", trigger]
        if db:
            cmd.append("--postgres")
        self.name = name
        self.lines: list[str] = []
        self._lock = threading.Lock()
        self.proc = subprocess.Popen(cmd, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                                     encoding="utf-8", errors="replace")
        threading.Thread(target=self._read, daemon=True).start()

    def _read(self) -> None:
        for line in self.proc.stdout:
            with self._lock:
                self.lines.append(line.rstrip())

    def count(self, text: str) -> int:
        with self._lock:
            return sum(1 for line in self.lines if text in line)

    def wait_until(self, condition, timeout: float) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if condition():
                return True
            if self.proc.poll() is not None and not condition():
                return False
            time.sleep(0.5)
        return False

    def wait_exit(self, timeout: float) -> bool:
        try:
            self.proc.wait(timeout=timeout)
            time.sleep(1)
            return True
        except subprocess.TimeoutExpired:
            return False

    def kill(self) -> None:
        subprocess.run(["docker", "stop", "-t", "5", self.name], capture_output=True)

    def save_log(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock:
            path.write_text("\n".join(self.lines), encoding="utf-8")

    def problem_lines(self) -> list[str]:
        with self._lock:
            return [l[:230] for l in self.lines if "ERROR" in l or "Traceback" in l or "Exception" in l]

    def key_lines(self) -> list[str]:
        with self._lock:
            return [l[:230] for l in self.lines if any(k in l for k in ("Starting stream", "BATCH_SUMMARY", "Stop requested",
                                                                       "Stream stopped", "ERROR", "Traceback"))]


# ---------------------------------------------------------------- checkpoint / logs / stores
def committed_batches(ckpt: Path) -> list[int]:
    commits = ckpt / "commits"
    return sorted(int(p.name) for p in commits.iterdir() if p.name.isdigit()) if commits.exists() else []


def offset_after_batch(ckpt: Path, topic: str, batch_id: int | None) -> int | None:
    """Next offset to read after `batch_id`: the END offset stored in that batch's offsets file.

    The newest offsets file may belong to a batch that was planned but NOT committed when Spark
    stopped; that batch is simply run again after a restart. So the resume point is taken from
    the last COMMITTED batch."""
    if batch_id is None:
        return None
    text = (ckpt / "offsets" / str(batch_id)).read_text(encoding="utf-8").strip().splitlines()[-1]
    return int(json.loads(text)[topic]["0"])


def batch_log(stream: str) -> list[dict]:
    path = ROOT / "data" / "processed" / "spark_batches" / f"{stream}.jsonl"
    by_id = {}
    for line in path.read_text(encoding="utf-8").splitlines() if path.exists() else []:
        if line.strip():
            rec = json.loads(line)
            by_id[rec["batch_id"]] = rec
    return [by_id[k] for k in sorted(by_id)]


def pg_connect(dbname: str):
    return psycopg2.connect(host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=dbname, user=POSTGRES_USER,
                            password=POSTGRES_PASSWORD, connect_timeout=5)


def create_database(dbname: str) -> None:
    admin = pg_connect("postgres")
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS {dbname} WITH (FORCE)")
        cur.execute(f"CREATE DATABASE {dbname}")
    admin.close()
    out = subprocess.run([PYTHON, "scripts/init_db.py"], cwd=ROOT, capture_output=True, text=True,
                         env={**os.environ, "POSTGRES_DB": dbname})
    if "schema OK" not in out.stdout:
        fail_now(f"could not initialise schema in {dbname}: {out.stderr[-300:]}")


def pg_counts(dbname: str) -> dict:
    conn = pg_connect(dbname)
    with conn, conn.cursor() as cur:
        cur.execute("SELECT count(*), count(DISTINCT record_uid), count(DISTINCT order_id) FROM core.orders")
        rows, uids, orders = cur.fetchone()
        cur.execute("SELECT count(*) FROM core.duplicate_records")
        dups = cur.fetchone()[0]
    conn.close()
    return {"rows": rows, "distinct_record_uid": uids, "distinct_order_id": orders, "duplicate_records_logged": dups}


def parquet_stats(parquet_dir: Path) -> dict:
    table = ds.dataset(str(parquet_dir), format="parquet", partitioning="hive").to_table(columns=["record_uid", "batch_id"])
    uids = table.column("record_uid").to_pylist()
    per_batch: dict[int, int] = {}
    for b in table.column("batch_id").to_pylist():
        per_batch[int(b)] = per_batch.get(int(b), 0) + 1
    return {"rows": len(uids), "distinct": len(set(uids)), "per_batch": per_batch}


# ---------------------------------------------------------------- experiment 1: recovery
def recovery(tag: str) -> None:
    topic, stream, test_dir, db = f"recovery_test_{tag}", f"recovery_{tag}", f"recovery_{tag}", f"streampulse_recovery_{tag}"
    base = ROOT / "data" / "test_runs" / test_dir
    ckpt = base / "checkpoints" / stream
    if base.exists():
        fail_now(f"{base} already exists; choose another --tag (existing checkpoints are never reused or deleted)")
    say(f"RECOVERY EXPERIMENT tag={tag} topic={topic} stream={stream} checkpoint={ckpt.relative_to(ROOT)} database={db}")
    say(f"Started {datetime.now().isoformat(timespec='seconds')}\n")

    say("Step 1: create isolated topic and database, publish set A (real orders, replay rows 0..5999)")
    create_topic(topic)
    create_database(db)
    start0, end0 = topic_offsets(topic)
    check("topic starts empty", start0 == end0 == 0, f"start={start0} end={end0}")
    pub_a = publish(topic, f"rec-A-{tag}", skip=0, limit=6000)
    _, end_a = topic_offsets(topic)
    check("set A published completely", end_a == pub_a["rows_published"] and pub_a["rows_publish_failed"] == 0,
          f"topic end offset {end_a} vs producer published {pub_a['rows_published']}")

    say("\nStep 2: start Spark (maxOffsetsPerTrigger=500); stop it cleanly after the first completed batch")
    run1 = SparkRun(f"rec_spark1_{tag}", topic, stream, test_dir, db, "3 seconds", 500)
    if not run1.wait_until(lambda: run1.count("BATCH_SUMMARY") >= 1, 300):
        run1.kill()
        say("\n".join(run1.key_lines()))
        fail_now("no completed BATCH_SUMMARY within 300 s (timeout); Spark did not start processing")
    stop_file = base / "checkpoints" / f"STOP_{stream}"
    stop_file.touch()
    say(f"  batch summaries seen when stop was requested: {run1.count('BATCH_SUMMARY')}")
    if not run1.wait_exit(120):
        run1.kill()
        fail_now("Spark did not stop within 120 s of the stop request")
    for line in run1.key_lines():
        say("  [run1] " + line)
    run1.save_log(base / "run1_full.log")
    check("first run stopped cleanly (no ERROR/Traceback in its log)", not run1.problem_lines(),
          f"{len(run1.problem_lines())} problem lines; full log kept in data/test_runs/{test_dir}/run1_full.log")
    for line in run1.problem_lines()[:10]:
        say("  [run1 problem] " + line)

    committed1 = committed_batches(ckpt)
    last_committed = max(committed1) if committed1 else None
    resume_offset = offset_after_batch(ckpt, topic, last_committed)
    next_batch_id = (last_committed + 1) if last_committed is not None else 0
    pending = end_a - (resume_offset or 0)
    say(f"  checkpoint after stop: committed batch ids={committed1} next offset to read={resume_offset} topic end={end_a}")
    if not check("stop happened mid-backlog (some batches committed, others still pending)",
                 len(committed1) >= 1 and pending > 0, f"{len(committed1)} committed, {pending} messages still unread"):
        finish()
    log_before = batch_log(stream)
    check("each committed batch has a log line", len(log_before) >= len(committed1),
          f"batch log lines={len(log_before)} committed={len(committed1)}")

    say("\nStep 3: while Spark is DOWN publish set B (new real orders, rows 6000..7999) and a DUPLICATE slice (rows 0..99 again)")
    pub_b = publish(topic, f"rec-B-{tag}", skip=6000, limit=2000)
    pub_d = publish(topic, f"rec-dup-{tag}", skip=0, limit=100)
    _, end_final = topic_offsets(topic)
    total_published = pub_a["rows_published"] + pub_b["rows_published"] + pub_d["rows_published"]
    check("all publishes accounted for", end_final == total_published,
          f"topic end offset {end_final} = A {pub_a['rows_published']} + B {pub_b['rows_published']} + dup {pub_d['rows_published']}")

    say("\nStep 4: restart the SAME logical stream with the SAME checkpoint (availableNow: drain the backlog, then exit)")
    run2 = SparkRun(f"rec_spark2_{tag}", topic, stream, test_dir, db, "availableNow", 500)
    if not run2.wait_exit(900):
        run2.kill()
        fail_now("restarted Spark did not finish within 900 s")
    for line in run2.key_lines():
        say("  [run2] " + line)
    run2.save_log(base / "run2_full.log")
    check("restarted run finished without ERROR/Traceback", not run2.problem_lines(), f"{len(run2.problem_lines())} problem lines")
    if run2.proc.returncode != 0:
        fail_now(f"restarted Spark exited with code {run2.proc.returncode}")

    say("\nStep 5: verify")
    logs = batch_log(stream)
    ids = [b["batch_id"] for b in logs]
    check("batch ids are contiguous (none skipped, none repeated)", ids == list(range(len(ids))), f"ids={ids[0]}..{ids[-1]} ({len(ids)} batches)")
    first_after = next((b for b in logs if b["batch_id"] == next_batch_id), None)
    first_off = first_after["offsets"][0]["first_offset"] if first_after else None
    check("restart resumed exactly at the stored offset", first_off == resume_offset,
          f"first batch after restart (id {next_batch_id}) starts at offset {first_off}; checkpoint said {resume_offset}")
    contiguous = all(logs[i]["offsets"][0]["first_offset"] == logs[i - 1]["offsets"][0]["last_offset"] + 1 for i in range(1, len(logs)))
    check("no offset range read twice or skipped", contiguous, "each batch starts right after the previous batch ended")
    consumed = sum(b["input_rows"] for b in logs)
    check("Spark consumed every Kafka message exactly once", consumed == end_final, f"sum(input_rows)={consumed} vs topic messages={end_final}")
    after_restart = [b for b in logs if b["batch_id"] >= next_batch_id]
    check("restart processed the pending A offsets and the new B/dup messages",
          sum(b["input_rows"] for b in after_restart) == end_final - resume_offset,
          f"{len(after_restart)} batches after restart read {sum(b['input_rows'] for b in after_restart)} messages; expected {end_final - resume_offset}")

    unique_expected = pub_a["rows_published"] + pub_b["rows_published"]
    pq = parquet_stats(base / "parquet")
    in_batch = sum(b["in_batch_duplicates"] for b in logs)
    check("Parquet rows = consumed - in-batch duplicates", pq["rows"] == consumed - in_batch, f"{pq['rows']} rows (consumed {consumed}, in-batch duplicates {in_batch})")
    check("Parquet distinct record_uid = unique published records", pq["distinct"] == unique_expected, f"{pq['distinct']} vs {unique_expected}")
    mism = {b["batch_id"]: (pq["per_batch"].get(b["batch_id"]), b["parquet_rows_written"]) for b in logs
            if pq["per_batch"].get(b["batch_id"]) != b["parquet_rows_written"]}
    check("every Parquet batch folder matches its batch log", not mism, f"mismatches={mism}")

    pg = pg_counts(db)
    check("PostgreSQL has one row per unique record", pg["rows"] == pg["distinct_record_uid"] == unique_expected,
          f"rows={pg['rows']} distinct record_uid={pg['distinct_record_uid']} distinct order_id={pg['distinct_order_id']} expected={unique_expected}")
    retry = sum((b.get("postgres") or {}).get("retry_duplicates", 0) for b in logs)
    source = sum((b.get("postgres") or {}).get("source_duplicates", 0) for b in logs)
    inserted = sum((b.get("postgres") or {}).get("inserted", 0) for b in logs)
    check("cross-batch duplicates were detected by the PostgreSQL writer as retry duplicates",
          retry == pub_d["rows_published"] and in_batch == 0,
          f"retry_duplicates={retry} (duplicate slice published {pub_d['rows_published']}); in-batch duplicates={in_batch}")
    check("no source duplicates and counts add up", source == 0 and inserted + retry + source == pq["rows"],
          f"inserted={inserted} retry={retry} source={source} total={inserted + retry + source} Parquet rows={pq['rows']}")

    say("\nBatch summaries (batch_id, input, parquet_written, pg inserted/retry, first..last offset):")
    for b in logs:
        pgb = b.get("postgres") or {}
        o = b["offsets"][0]
        say(f"  batch {b['batch_id']:>2}  input={b['input_rows']:>4}  parquet={b['parquet_rows_written']:>4}  "
            f"pg_inserted={pgb.get('inserted')}  pg_retry={pgb.get('retry_duplicates')}  offsets {o['first_offset']}..{o['last_offset']}")
    say(f"Checkpoint committed batch ids after restart: {committed_batches(ckpt)}; next offset={offset_after_batch(ckpt, topic, max(committed_batches(ckpt)))}")
    say(f"Kafka offsets: start={start0} end after A={end_a} end final={end_final}")
    finish()


# ---------------------------------------------------------------- experiment 2: malformed messages
def malformed(tag: str) -> None:
    topic, stream, test_dir = f"dlq_test_{tag}", f"dlqtest_{tag}", f"dlq_{tag}"
    base = ROOT / "data" / "test_runs" / test_dir
    if base.exists():
        fail_now(f"{base} already exists; choose another --tag")
    say(f"MALFORMED-MESSAGE EXPERIMENT tag={tag} topic={topic} stream={stream}\n")
    create_topic(topic)
    pub = publish(topic, f"dlq-valid-{tag}", skip=0, limit=5)

    from kafka import KafkaProducer
    sent = {
        "MALFORMED_JSON": '{"record_uid": "x1", this is not valid json',
        "SCHEMA_MISMATCH:no record_uid": '{"schema_version": "1.0", "payload": {"order_id": "abc"}}',
        "SCHEMA_MISMATCH:no order_id": '{"schema_version": "1.0", "record_uid": "uid-no-order", "payload": {"customer_id": "c"}}',
    }
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    for text in sent.values():
        producer.send(topic, value=text.encode("utf-8")).get(timeout=10)
    producer.flush()
    producer.close()
    _, end = topic_offsets(topic)
    check("5 real valid + 3 crafted bad messages are in the topic", end == pub["rows_published"] + 3, f"end offset {end}")

    run = SparkRun(f"dlq_spark_{tag}", topic, stream, test_dir, None, "availableNow", 1000)
    if not run.wait_exit(600):
        run.kill()
        fail_now("Spark did not finish within 600 s")
    for line in run.key_lines():
        say("  [spark] " + line)
    if run.proc.returncode != 0:
        fail_now(f"Spark exited with code {run.proc.returncode}")

    logs = batch_log(stream)
    check("batch summary counts the bad messages", sum(b["invalid_rows"] for b in logs) == 3 and sum(b["valid_rows"] for b in logs) == pub["rows_published"],
          f"valid={sum(b['valid_rows'] for b in logs)} invalid={sum(b['invalid_rows'] for b in logs)}")
    pq = parquet_stats(base / "parquet")
    check("only the valid real orders reached Parquet", pq["rows"] == pub["rows_published"], f"Parquet rows={pq['rows']}")

    files = sorted((base / "dead_letter" / "stream").glob("*.jsonl"))
    records = [json.loads(l) for f in files for l in f.read_text(encoding="utf-8").splitlines() if l.strip()]
    check("dead-letter sink received 3 records", len(records) == 3, f"{len(records)} records in {[f.name for f in files]}")
    by_raw = {r["raw_payload"]: r for r in records}
    expected = {"MALFORMED_JSON": "MALFORMED_JSON", "SCHEMA_MISMATCH:no record_uid": "SCHEMA_MISMATCH", "SCHEMA_MISMATCH:no order_id": "SCHEMA_MISMATCH"}
    for label, text in sent.items():
        rec = by_raw.get(text)
        check(f"'{label}' routed with reason {expected[label]}", bool(rec) and rec["reason_code"] == expected[label] and rec["stage"] == "stream",
              f"found={bool(rec)} reason={rec['reason_code'] if rec else None} stage={rec['stage'] if rec else None}")
    say("\nDead-letter records saved by Spark:")
    for r in records:
        say("  " + json.dumps({k: r[k] for k in ("dlq_id", "stage", "reason_code", "reason_detail", "ingest_run_id", "raw_payload")}))
    finish()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("experiment", choices=["recovery", "malformed"])
    parser.add_argument("--tag", default=datetime.now().strftime("%m%d%H%M"), help="unique name of this attempt (lowercase letters/digits)")
    args = parser.parse_args()
    {"recovery": recovery, "malformed": malformed}[args.experiment](args.tag)


if __name__ == "__main__":
    main()
