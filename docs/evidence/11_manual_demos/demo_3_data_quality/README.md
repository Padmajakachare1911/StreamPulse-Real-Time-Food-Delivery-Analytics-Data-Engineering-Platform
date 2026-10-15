# DEMO 3 - Invalid data, data quality and duplicate handling

**Status: PENDING_HUMAN**: the student performs this demonstration and captures the files below.
Nothing in this folder has been captured yet; do not add simulated output.

Define once per terminal: `$py = ".\.venv\Scripts\python.exe"`

| File to save here | What it must show | How to produce it |
|---|---|---|
| `rejected_record.txt` | The dead-letter record with reason_code RANGE_ERROR and the PostgreSQL view of it. | `Get-Content data\dead_letter\ingest\demo3-run1.jsonl  (see docs\manual_demo_guide.md, Demo 3)` |
| `duplicate_proof.txt` | SQL showing ONE order and ONE source_duplicate row before AND after the re-run. | `docker exec streampulse-postgres psql -U streampulse -d streampulse_demo3 -c SELECT count(*) FROM core.orders -c SELECT duplicate_type, count(*) FROM core.duplicate_records GROUP BY 1` |
| `demo_3_dashboard.png` | Dashboard 'Data quality' page showing the demo database (started with -Demo 3). | `.\scripts\run_dashboard.ps1 -Demo 3  then screenshot http://localhost:8501` |

Before committing run `python scripts/secret_scan.py` (must print `RESULT: clean`). Never include passwords or tokens.
