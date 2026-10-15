# DEMO 2 - Real API integration

**Status: PENDING_HUMAN**: the student performs this demonstration and captures the files below.
Nothing in this folder has been captured yet; do not add simulated output.

Define once per terminal: `$py = ".\.venv\Scripts\python.exe"`

| File to save here | What it must show | How to produce it |
|---|---|---|
| `api_real_request.txt` | The live request: timestamp, URL, HTTP 200, excerpt. | `.\scripts\save_evidence.ps1 -TaskId DEMO2 -OutFile docs\evidence\11_manual_demos\demo_2_real_api\api_real_request.txt -Command '$py scripts\api_demo_call.py --sample --no-cache'` |
| `demo_2_api_sql.png` | Screenshot of the SQL result on core.api_cache and core.orders (provenance columns). | `psql queries in docs\manual_demo_guide.md (Demo 2); take the screenshot of the terminal` |

Before committing run `python scripts/secret_scan.py` (must print `RESULT: clean`). Never include passwords or tokens.
