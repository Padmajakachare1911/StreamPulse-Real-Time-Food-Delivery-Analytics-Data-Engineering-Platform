# Viva Questions and Honest Answers

Format of every answer: **Short answer → Simple analogy → Where it is in our project → Honest limitation.**
All numbers below were measured in this project (see `docs/evidence_register.md`).

---
### 1. What does StreamPulse do?
**Short:** It replays about 100,000 real historical e-commerce orders through a streaming pipeline (Kafka → Spark → Parquet + PostgreSQL), checks their quality, adds a USD value from a real API, and shows analytics in a dashboard.
**Analogy:** A factory conveyor belt: raw orders go in, quality inspectors reject broken ones, the rest are stored and counted.
**Where:** `README.md`, `docs/architecture.md`.
**Limitation:** The data is historical, not live.

### 2. Why a Kaggle dataset?
**Short:** Real data has real problems (missing dates, orders without items) that show why validation matters, and it is free and documented.
**Where:** `docs/data_source.md` (Olist, license CC BY-NC-SA 4.0), `docs/data_quality_report.md`.
**Limitation:** The dataset is e-commerce, not food delivery, so we retitled the project (to be confirmed with the professor).

### 3. Why replay historical data into Kafka? Is that "real-time"?
**Short:** Replay lets us test a streaming architecture with real data at a speed we control. It is a streaming architecture processing a historical dataset; it is **not** a live customer feed.
**Analogy:** Playing an old match recording through a live-score system to test the system.
**Where:** `src/producer/dataset_producer.py` (`--limit`, `--full`, `--rate`).
**Limitation:** We never claim live orders.

### 4. What is the external API and what is its role?
**Short:** Frankfurter, which serves European Central Bank exchange rates. We fetch BRL→USD rates once for the whole date range (one request, 544 daily rates) and attach the rate of each order's purchase day, so every order has an estimated USD value.
**Where:** `src/api/client.py`, `docs/api_integration.md`, `core.fx_rates`.
**Limitation:** The API gives reference data, not orders. USD values are estimates; weekends use the previous business day (24,112 orders).

### 5. Why Kafka? Topics, partitions, offsets, keys, consumers?
**Short:** Kafka stores messages durably in a topic. A topic is split into partitions (ours: 3). Each message gets an **offset** (its position in a partition). The **key** decides the partition: same key → same partition. A consumer (Spark) remembers the offsets it has processed.
**Analogy:** A library with 3 shelves; the key picks the shelf; the offset is the book's position on the shelf.
**Where:** `scripts\kafka_describe_topic.ps1`, `docs/evidence/03_kafka/`.
**Limitation:** Single broker, replication factor 1 (no fault tolerance).

### 6. Why `order_id` as the key? Is the spread even?
**Short:** Every order is one event, so no ordering between orders is needed; hashing `order_id` spreads them evenly. **Measured** on the broker: 33,199 / 33,255 / 32,973 messages (33.4 / 33.4 / 33.2 %). Keying by `customer_state` would have put 66.9 % in one partition (São Paulo dominates).
**Where:** `docs/evidence/03_kafka/key_options_computed_offline.txt`, `partition_counts_after_full_run.txt`.

### 7. Spark Structured Streaming: micro-batches and triggers?
**Short:** Spark reads new Kafka messages in small batches. A trigger says when: every 5 seconds (continuous) or `availableNow` (process what exists, then stop). We cap each batch with `maxOffsetsPerTrigger`.
**Where:** `src/streaming/order_stream.py`, `scripts\run_stream.ps1`.
**Limitation:** It runs in local mode inside Docker (one machine).

### 8. What is a checkpoint and what happens after a crash or stop?
**Short:** The checkpoint is Spark's progress notebook (which offsets are done). After a restart Spark continues from there. **Proved:** we stopped Spark after 2 of 12+ batches, published 2,000 new orders plus 99 repeats while it was down, restarted, and it resumed exactly at offset 1000, consumed 8,094 messages in total and stored 7,995 unique orders.
**Where:** `docs/evidence/04_spark/recovery_test.txt`.
**Limitation:** A batch interrupted mid-way is run again; that is safe only because writes are idempotent.

### 9. Parquet vs PostgreSQL?
**Short:** Parquet is our append-only **landing layer** (cheap files, keeps everything Spark consumed, including repeats). PostgreSQL is the **deduplicated warehouse** used for SQL and the dashboard. After re-ingesting 200 orders: Parquet 99,626 rows (99,427 distinct), PostgreSQL 99,427.
**Where:** `docs/data_quality_rules.md`.

### 10. ETL or ELT?
**Short:** Mostly ETL: we validate and transform (join, type-convert, enrich) before loading into the warehouse; SQL views then do the analytics (the "T" of ELT for KPIs).

### 11. Data quality and dead letters?
**Short:** Rules reject impossible records (e.g. delivered before purchase). A rejected record is never dropped silently: it goes to a dead-letter file with a reason code and the original data. In the real data 14 orders contradict themselves (8 "delivered" without a delivery date, 6 "canceled" with one) and sit in the dead-letter queue.
**Where:** `config/quality_rules.yaml`, `docs/data_quality_rules.md`, `docs/evidence/05_data_quality/`.
**Limitation:** Empty delivery dates are *valid* for orders not yet delivered; we only reject contradictions.

### 12. Deduplication and idempotency?
**Short:** `record_uid` = hash(file hash + row number), so replaying the same file gives the same IDs and PostgreSQL ignores them (`ON CONFLICT`). **Retry duplicate** = same `record_uid`; **source duplicate** = different `record_uid` but same `order_id`.
**Proof:** replaying the first 200 orders into the production topic inserted 0 rows and counted 199 retry duplicates; the warehouse stayed at 99,427.
**Limitation:** The real file has no source duplicates; they are demonstrated with the Demo 3 test file.

### 13. Is it exactly-once?
**Short:** **No.** It is at-least-once delivery with idempotent database writes, so repeated records do not create duplicate rows. Kafka offsets, Spark checkpoints and database upserts each help; none alone gives exactly-once across all systems.

### 14. Airflow and DAGs? Why isn't the stream an Airflow task?
**Short:** A DAG is a graph of tasks with dependencies, retries and logs. A task must finish; the continuous stream never finishes, so Airflow runs finite steps (bounded replay, Spark `availableNow`, checks, reconciliation).
**Where:** `airflow/dags/streampulse_batch_dag.py`, `docs/evidence/08_airflow/`.
**Limitation:** The first real run failed (a file-permission problem on the shared checkpoint); it is kept as evidence and was fixed.

### 15. What is Streamlit used for and how do you know its status lights are not fake?
**Short:** A dashboard on top of PostgreSQL/Parquet/Kafka. Each light comes from a real check (SQL query, Kafka metadata request, file listing, HTTP call). When a service is down the light is red and the page says so. A bug where Kafka showed red while running was found from a screenshot and fixed.
**Where:** `src/dashboard/status.py`, `docs/evidence/09_dashboard/`.

### 16. Lineage and provenance?
**Short:** Every warehouse row keeps `source_file_sha256`, `source_row_number`, `ingest_run_id`, Kafka partition/offset and the original source columns (`raw`). API-enriched values carry endpoint, retrieval time, HTTP status and cache key.

### 17. How do you know the numbers are right (reconciliation)?
**Short:** `scripts/reconcile.py` compares counts layer by layer, with expected values **computed from the source file and the rules**. Run-level counts are kept apart from unique counts, because runs can overlap. Example: rejected = 14 unique records, but 15 dead-letter lines in two runs, because one test run replayed the same rejected order. The script exits non-zero on an unexplained difference (demonstrated).

### 18. Limitations and future work?
- One machine, one broker, no replication; local Spark.
- Historical replay, not a live source; no schema registry (JSON is used); no monitoring/alerting stack.
- The graceful stop has a tiny race (safe because of idempotency).
- Future: a real live source, Avro + Schema Registry, Kubernetes, Prometheus/Grafana, Delta/Iceberg tables.

### Model answers
- **"Is this real-time?"** "It is a streaming architecture processing a historical dataset replayed at a controlled rate. The API provides real external reference data. We do not claim live customer orders."
- **"Is it exactly-once?"** "No. It is at-least-once delivery with idempotent database writes, so repeated records do not create duplicates in the warehouse."
