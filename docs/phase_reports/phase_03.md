Phase: 3 - Kafka ingestion
Completed: compose updated (healthcheck, internal listener, topic-init job) and recreated; topic ecommerce_orders (3 partitions, RF 1); replay producer (--limit/--full/--dry-run/--rate/--batch-size/--run-id/--key-field/--no-enrich); quality rules + dead-letter writer; envelope with record_uid/business_hash/enrichment; 6 Kafka PowerShell scripts; key options compared; limited (100) and full (99,441) runs; old fake order_producer.py removed.
Files changed: docker-compose.yml, config/quality_rules.yaml, src/producer/dataset_producer.py, src/processing/{validators,deadletter}.py, src/common/orders_source.py, scripts/{_common,kafka_*}.ps1, scripts/measure_key_options.py, tests/unit/test_{producer,validators}.py, docs/implementation_plan.md, docs/evidence/03_kafka/*
Commands executed: docker compose up -d; python -m src.producer.dataset_producer --full --dry-run; --limit 100 --run-id limited-100; --full --run-id full-run-01; scripts\kafka_describe_topic.ps1, kafka_partition_counts.ps1, kafka_consume_n.ps1, kafka_list_topics.ps1
Verification: read 99,441 = valid 99,427 + rejected 14; published 99,427, failed 0; broker offset delta 33,199 + 33,255 + 32,973 = 99,427 (matches producer callbacks); key=order_id spread 33.4/33.4/33.2 %. Evidence: docs/evidence/03_kafka/
Tests: 46 unit passed (producer 9, validators 14, mapping 8, api 15)
Git: commit "feat(producer): ..." for Phase 3 (hash in docs/task_tracker.md), pushed after commit
Blocked items: none. Caveat: the "before limited run" offsets were not captured (topic was created empty minutes earlier; end offsets sum 99 = callbacks 99).
Next phase: 4 - Spark Structured Streaming (in Docker because host Spark needs winutils)
