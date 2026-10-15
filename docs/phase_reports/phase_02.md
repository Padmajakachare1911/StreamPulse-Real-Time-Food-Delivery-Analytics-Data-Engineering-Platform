Phase: 2 - Real API selection and integration
Completed: API chosen and justified (no professor API: docs/inputs/api_info.md absent); live docs checked (v2 is current, v1 deprecated); client with retry/backoff/cache/schema validation; enrichment logic; demo script; real requests; unit + integration tests.
Files changed: src/api/{client,enrich}.py, scripts/api_demo_call.py, tests/unit/test_api_client.py, tests/integration/test_api_real.py, docs/api_integration.md, src/common/config.py + .env.example (API v2 default), docs/evidence/02_api/*
Commands executed: python scripts\api_demo_call.py --sample --no-cache; --full-range --no-cache; --simulate-failure; pytest tests/unit/test_api_client.py; pytest -m integration tests/integration/test_api_real.py
Verification: real HTTP 200 (5 rows one week; 544 rows 2016-09-04..2018-10-17); simulated unreachable host shows 3 attempts, backoff 1 s then 2 s. Evidence: docs/evidence/02_api/
Tests: 15 unit passed (mocked HTTP, not proof); 1 integration (real call) passed
Git: a9f479d, pushed to origin/main
Blocked items: none. Note: storing rates in PostgreSQL (core.api_cache, core.fx_rates) waits for Phase 6.
Next phase: 3 - Kafka ingestion
