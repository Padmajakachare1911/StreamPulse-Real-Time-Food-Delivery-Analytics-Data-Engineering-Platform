# Evidence Register

Every row points to a real saved file. Status uses the same vocabulary as `docs/task_tracker.md` (+ `PENDING_HUMAN`).

| Task ID | Task | Evidence location | Actual result | Status |
|---|---|---|---|---|
| ENV-01 | Verify Python/Java/Docker versions | `docs/evidence/00_environment/env_check.txt` | Python 3.11.9, OpenJDK 17.0.20.1, Docker 29.8.2, Compose v5.6.0, PySpark 4.2.0, kafka-python 3.0.11; Kafka port 9092 reachable | VERIFIED_WITH_EVIDENCE |
| KFK-00 | Kafka produce+consume smoke test | `docs/evidence/00_environment/kafka_smoke_test.txt` | 1 message produced and consumed on topic `smoke_test`; `food_orders` has 3 partitions, RF 1 | VERIFIED_WITH_EVIDENCE |
| SPK-00 | Existing Spark stream baseline | `docs/evidence/00_environment/spark_baseline_existing_stream.txt` | `SparkContext` failed on host: HADOOP_HOME/winutils missing; no Parquet written | VERIFIED_WITH_EVIDENCE (failure recorded) |
| GIT-00 | `.gitignore` audit | `docs/evidence/00_environment/gitignore_audit.txt` | `.env`, `.venv`, raw data, Parquet, checkpoints, DLQ ignored; `.env.example` and `.gitkeep` not ignored | VERIFIED_WITH_EVIDENCE |
| DEMO-1..4 | Four manual demos | `docs/evidence/11_manual_demos/` | to be performed by the student | PENDING_HUMAN |
