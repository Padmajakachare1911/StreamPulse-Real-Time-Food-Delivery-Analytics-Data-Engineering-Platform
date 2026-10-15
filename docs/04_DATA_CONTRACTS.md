# 04 — Data Contracts

> Field names inside `payload` come from the **real dataset mapping** (`config/field_mapping.yaml`). Nothing here assumes specific business columns. Envelope fields are pipeline metadata and are safe to define now.

## 1. Kafka message envelope (topic `food_orders`)
```json
{
  "schema_version": "1.0",
  "record_uid": "<sha256(source_file_sha256 + ':' + source_row_number)[:32]>",
  "business_hash": "<hash of canonical payload fields, or natural key if the data has one>",
  "source_category": "kafka_replayed",
  "source_file": "<filename only, not full path>",
  "source_file_sha256": "<hash>",
  "source_row_number": 12345,
  "ingest_run_id": "<uuid or timestamp-based id>",
  "ingested_at": "<ISO-8601 UTC>",
  "payload": { "<canonical_field>": "<typed value>" },
  "raw": { "<original_source_column>": "<original value>" },
  "api_enrichment": null
}
```
- **Encoding:** UTF-8 JSON (document why; Avro/Schema Registry is out of scope unless justified).
- **Key:** deliberately chosen from real data (e.g., an entity ID where per-entity ordering matters) — if no such field exists, use `record_uid` (even-ish spread) and say so. Document trade-offs; **measure** the resulting partition distribution.
- **Headers (optional):** `ingest_run_id`, `schema_version`.

## 2. Identity and duplicates
| Concept | Field | Meaning |
|---|---|---|
| Record identity | `record_uid` | Deterministic per (file content, row). Same file replayed ⇒ same IDs. |
| Content identity | `business_hash` | Same business content across different rows ⇒ **source duplicate** |
| Run identity | `ingest_run_id` | Distinguishes ingestion executions |
| Delivery identity | Kafka `partition+offset` | Distinguishes broker positions (a producer retry may create a second offset with same `record_uid`) |

**Retry vs source duplicate:** same `record_uid` ⇒ retry/replay (idempotent no-op). Different `record_uid` but same `business_hash` ⇒ genuine source duplicate (policy: keep first, log the rest).

## 3. Guarantee statement (use this wording in docs/viva)
> Kafka provides durable, ordered-per-partition storage with offsets. Spark checkpoints track which offsets were processed. The PostgreSQL sink is idempotent (`ON CONFLICT (record_uid)`). Together this gives **at-least-once delivery with idempotent writes** — effectively-once results in the warehouse for this design. We do not claim exactly-once across all systems.

## 4. Dead-letter record (JSONL file and DB table)
```json
{
  "dlq_id": "<uuid>",
  "stage": "ingest | stream | db",
  "reason_code": "MISSING_REQUIRED | TYPE_ERROR | RANGE_ERROR | MALFORMED_JSON | SCHEMA_MISMATCH | DB_ERROR | ...",
  "reason_detail": "<human-readable>",
  "record_uid": "<if available>",
  "ingest_run_id": "<id>",
  "raw_payload": "<string or object>",
  "rejected_at": "<ISO-8601 UTC>"
}
```
Paths: `data/dead_letter/ingest/…jsonl`, `data/dead_letter/stream/…`; table `core.dead_letter_records`.

## 5. Parquet layout
`data/processed/parquet/` (valid records, typed columns + provenance). Checkpoint at `data/checkpoints/<stream_name>/` (separate, git-ignored). Optional partition column only if real data justifies it.

## 6. PostgreSQL skeleton (adapt columns to real schema)
```sql
CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.food_orders (
    record_uid          TEXT PRIMARY KEY,
    business_hash       TEXT,
    -- >>> typed columns generated from field_mapping.yaml <<<
    source_category     TEXT NOT NULL,
    source_file_sha256  TEXT NOT NULL,
    source_row_number   BIGINT NOT NULL,
    ingest_run_id       TEXT NOT NULL,
    kafka_partition     INT,
    kafka_offset        BIGINT,
    processed_at        TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS core.dead_letter_records (
    dlq_id UUID PRIMARY KEY, stage TEXT, reason_code TEXT, reason_detail TEXT,
    record_uid TEXT, ingest_run_id TEXT, raw_payload JSONB, rejected_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS core.ingestion_runs (
    ingest_run_id TEXT PRIMARY KEY, started_at TIMESTAMPTZ, finished_at TIMESTAMPTZ,
    source_file TEXT, source_file_sha256 TEXT,
    rows_read BIGINT, rows_valid BIGINT, rows_rejected BIGINT,
    rows_published BIGINT, rows_publish_failed BIGINT, mode TEXT
);

CREATE TABLE IF NOT EXISTS core.api_cache (
    cache_key TEXT PRIMARY KEY, api_source_name TEXT, api_endpoint TEXT,
    response_json JSONB, http_status INT, retrieved_at TIMESTAMPTZ
);
```
Write path: batch → temp/staging table → `INSERT … ON CONFLICT (record_uid) DO NOTHING` (or `DO UPDATE` if documented).

## 7. Reconciliation contract
| Stage | Source of the number |
|---|---|
| 1 rows read | ingestion run JSON |
| 2 valid / 3 rejected | ingestion run JSON + DLQ line count |
| 4 published | producer delivery callbacks + topic end offsets (sum over partitions, minus pre-run offsets) |
| 5 consumed by Spark | batch logs (sum of `input_rows`) |
| 6 in Parquet | Spark read of Parquet, count |
| 7 in PostgreSQL | `SELECT count(*) FROM core.food_orders` |
| 8 unique analytics-ready | `count(DISTINCT record_uid)` (+ `business_hash` unique count) |

Explain all differences: rejected rows, source duplicates, retry duplicates, runs that were repeated, failed publishes.

## 8. API provenance fields (when API data is used)
`api_source_name`, `api_endpoint`, `api_retrieved_at`, `api_http_status`, `api_resource_id`, `api_cache_key`. Enrichment joins on a deduplicated lookup key (document the matching logic and match rate).
