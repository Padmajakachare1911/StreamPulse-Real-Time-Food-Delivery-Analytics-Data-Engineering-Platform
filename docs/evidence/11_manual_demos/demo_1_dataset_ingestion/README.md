# DEMO 1 - Real dataset ingestion and end-to-end flow

**Status: PENDING_HUMAN**: the student performs this demonstration and captures the files below.
Nothing in this folder has been captured yet; do not add simulated output.

Define once per terminal: `$py = ".\.venv\Scripts\python.exe"`

| File to save here | What it must show | How to produce it |
|---|---|---|
| `demo_1_full_pipeline.png` | Screenshot of the dashboard pages 'Ingestion and reconciliation' and 'Business KPIs' while the numbers equal the SQL counts. | `Browser at http://localhost:8501 (start: .\scripts\run_dashboard.ps1) - or: $py scripts\capture_screenshots.py dashboard --out docs\evidence\11_manual_demos\demo_1_dataset_ingestion --prefix demo_1` |
| `dataset_row_count.txt` | Row/column counts of the 9 files. | `.\scripts\save_evidence.ps1 -TaskId DEMO1 -OutFile docs\evidence\11_manual_demos\demo_1_dataset_ingestion\dataset_row_count.txt -Command '$py scripts\profile_dataset.py --summary'` |
| `reconciliation.txt` | Stage-by-stage reconciliation after the limited run. | `.\scripts\save_evidence.ps1 -TaskId DEMO1 -OutFile docs\evidence\11_manual_demos\demo_1_dataset_ingestion\reconciliation.txt -Command '$py scripts\reconcile.py'` |

Before committing run `python scripts/secret_scan.py` (must print `RESULT: clean`). Never include passwords or tokens.
