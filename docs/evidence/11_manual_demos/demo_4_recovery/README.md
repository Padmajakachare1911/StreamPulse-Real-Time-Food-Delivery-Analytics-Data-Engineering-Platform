# DEMO 4 - Recovery, checkpoints, Airflow and reconciliation

**Status: PENDING_HUMAN**: the student performs this demonstration and captures the files below.
Nothing in this folder has been captured yet; do not add simulated output.

Define once per terminal: `$py = ".\.venv\Scripts\python.exe"`

| File to save here | What it must show | How to produce it |
|---|---|---|
| `restart_log.txt` | Spark log of the RESTART: first BATCH_SUMMARY starts at the stored offset (offsets first_offset). | `copy the T2 output of .\scripts\run_stream.ps1 -Demo 4 ... after the stop (see docs\manual_demo_guide.md, Demo 4)` |
| `airflow_dag_success.png` | Airflow UI grid/graph of streampulse_batch with all 9 tasks green. | `Browser at http://localhost:8080 (user admin) after .\scripts\airflow_run_dag.ps1 -Dag streampulse_batch -Conf '{limit: 300}'` |
| `final_reconciliation.txt` | Reconciliation of the demo 4 topic. | `.\scripts\save_evidence.ps1 -TaskId DEMO4 -OutFile docs\evidence\11_manual_demos\demo_4_recovery\final_reconciliation.txt -Command '$py scripts\reconcile.py --topic demo4_orders --stream demo4 --parquet-dir data\test_runs\demo4\parquet --database streampulse_demo4 --runs demo4-A demo4-B demo4-dup'` |

Before committing run `python scripts/secret_scan.py` (must print `RESULT: clean`). Never include passwords or tokens.
