Phase: 8 - Streamlit dashboard
Completed: 7-page dashboard (overview + architecture + real service status, dataset and provenance, ingestion and live reconciliation, data quality, business KPIs, external API, recent records); strict separation (queries.py = database/Parquet access with sanitised errors, status.py = real service checks, app.py = presentation only); real screenshots of all pages and of the outage states.
Files changed: src/dashboard/{queries,status,app}.py, scripts/{run_dashboard.ps1,capture_screenshots.py}, tests/unit/test_dashboard.py, tests/integration/test_dashboard_queries.py, requirements.txt (streamlit==1.66.0), requirements-dev.txt (playwright), docs/evidence/09_dashboard/*
Commands executed: scripts\run_dashboard.ps1; python scripts\capture_screenshots.py dashboard ...; pytest tests/unit/test_dashboard.py tests/integration/test_dashboard_queries.py -v; services-down capture (docker stop kafka, then postgres, capture, restore)
Verification (docs/evidence/09_dashboard/): 17 tests pass. Screenshots show real numbers: 99,427 orders, revenue 16,006,745 BRL (est. 4,768,687 USD), late 8.11 %, reconciliation Kafka 99,725 / Parquet 99,626 (99,427 distinct) / PostgreSQL 99,427, API match rate 100 %. With Kafka and PostgreSQL really stopped: both red, API grey, KPI pages "Unavailable"; services restored, 99,427 rows intact.
Defects found by LOOKING at the screenshots (not by tests) and fixed:
  1. Dashboard showed "Kafka: broker not reachable" while Kafka was healthy: kafka-python 3 rejects the option api_version_auto_timeout_ms (KafkaConfigurationError, reported as "down"). My first "down" unit tests passed for the wrong reason; a positive test against the real broker was added.
  2. NUMERIC columns arrived as Decimal and were plotted as text labels (nonsense axis): converted to float in the query layer + tests.
  3. Screenshots of the outage were taken mid-render: the capture script now waits until Streamlit finishes running.
Tests: 17 passed (9 unit, 8 integration incl. real Kafka + real PostgreSQL test database).
Git: see commit "feat(dashboard): ..." (hash in docs/task_tracker.md)
Blocked items: none. Limitations: customer identifiers are never shown; the live reconciliation on the dashboard is the lightweight layer comparison (full source-derived reconciliation is scripts/reconcile.py).
Next phase: 9 - automated testing
