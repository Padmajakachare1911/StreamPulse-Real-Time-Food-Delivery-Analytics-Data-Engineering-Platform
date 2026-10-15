# Data Quality Rules

Rules live in `config/quality_rules.yaml`; the checks are plain Python in `src/processing/validators.py`. The same function (`src/processing/classify.py`) is used by the producer and by the reconciliation script, so they can never disagree.

**Principles**
1. Nothing is dropped silently: a rejected row goes to a dead-letter file (`data/dead_letter/ingest/<run>.jsonl`) and later to `core.dead_letter_records`, with a reason code and the original row.
2. Empty `approved`, `carrier` or `delivered` timestamps are **valid**: they are normal for orders that are not approved, shipped or delivered yet.
3. A rule rejects only with a documented reason. Weaker problems are `warn` (counted, not rejected).

## Rules and justification
| Rule | Severity | Reason code | Why | Real data result (full run, 99,441 orders) |
|---|---|---|---|---:|
| status_in_allowed_set | reject | SCHEMA_MISMATCH | `order_status` has a closed set of 8 values in the real data | 0 |
| purchase_date_in_range | reject | RANGE_ERROR | real orders span 2016-09-04 … 2018-10-17; far outside means a corrupt date | 0 |
| amounts_not_negative | reject | RANGE_ERROR | price, freight and payment totals cannot be below 0 (none are in the real data) | 0 |
| approved_not_before_purchase | reject | RANGE_ERROR | an order cannot be approved before it was placed | 0 |
| delivered_not_before_purchase | reject | RANGE_ERROR | an order cannot arrive before it was placed | 0 |
| delivered_status_has_date | reject | INCONSISTENT | status `delivered` without a delivery date breaks delivery-time KPIs | **8** |
| delivered_date_matches_status | reject | INCONSISTENT | a delivery date on an order whose status is not `delivered` contradicts itself (all 6 real cases are `canceled`) | **6** |
| payment_total_positive | warn | RANGE_ERROR | order has payment rows but total ≤ 0 (voucher / not_defined cases exist in the real data) | 3 warnings |

Type and required-field checks (`MISSING_REQUIRED`, `TYPE_ERROR`) come from `config/field_mapping.yaml`; 0 real rows failed them.

**Result of the full real run:** 99,441 read = 99,427 valid + 14 rejected (`docs/evidence/03_kafka/producer_full_run.txt`). The 14 rejected orders are real source records that contradict themselves; they are preserved in the dead-letter file, not deleted.

## Duplicate handling
| Type | Detection | Handling |
|---|---|---|
| **Retry / replay duplicate** | same `record_uid` (file hash + row number) | write is a no-op; counted as `retry_duplicate` |
| **Source duplicate** | different `record_uid`, same `business_hash` (hash of the natural key `order_id`) | first copy kept, the other logged in `core.duplicate_records` (`source_duplicate`) |
| **Re-ingestion** | same file, new `ingest_run_id` | same `record_uid`, so idempotent |

The real orders file contains **no** duplicate `order_id`; duplicates in this project come from re-running ingestion (retry duplicates) or from the isolated Demo 3 test file (source duplicate).

## Where each layer applies the rules
| Layer | What it does |
|---|---|
| Producer (`stage = ingest`) | maps types, runs all rules, dead-letters rejected rows before they reach Kafka |
| Spark (`stage = stream`) | rejects messages that are not valid JSON (`MALFORMED_JSON`) or lack `record_uid`/`order_id` (`SCHEMA_MISMATCH`); removes duplicates inside a batch |
| PostgreSQL | `PRIMARY KEY (record_uid)` and `UNIQUE (order_id)`; the writer classifies retry vs source duplicates |

## Limits (honest notes)
- Rules check one order at a time; they do not compare against other orders except for duplicates.
- Orphan keys (e.g. an item whose product is missing) were checked once during profiling (0 found) but are not re-checked at stream time.
