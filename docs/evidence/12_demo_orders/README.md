# Evidence 12: user-submitted demo orders traced through the pipeline (2026-10-16)

Requirement: a user places an order in Streamlit and visibly traces it through Kafka, Spark Structured Streaming, Parquet and PostgreSQL, with the processing status on the dashboard and Airflow demonstrated. Walkthrough: `docs/live_demo_order_guide.md`.

All files are real outputs. Order ids and offsets differ per run.

| File | What it shows |
|---|---|
| `ui_run1_01_form_filled.png`, `ui_run1_02_submitted.png`, `ui_run1_03_trace_complete.png` | The real Streamlit page driven by a headless browser (`scripts/demo_order_ui_check.py`): form, "Published to Kafka: partition 2, offset 33364", all four stages ticked, "Submit to PostgreSQL took 6.5 s" |
| `ui_final_01_form_filled.png`, `ui_final_02_submitted.png`, `ui_final_03_trace_complete.png` | The same flow re-run against the FINAL code (after the last dashboard fix: the stored-orders table is now live): order `demo-6be925a7...`, Kafka partition 0 offset 33595, Spark batch 18, stored in 16.8 s (latency varies 6.5 to 16.8 s between runs) |
| `reconcile_production_final_state.txt` | Production reconciliation after the last demo order (6 demo messages, 5 distinct orders): exit 0 |
| `trace_cli_order_placed_in_streamlit.txt` | The same order traced from the terminal (`scripts/trace_order.py --verbose`) |
| `independent_check_kafka_console_consumer.txt`, `independent_check_parquet.txt`, `independent_check_postgresql.txt` | The same order read by Kafka's own tool, by pyarrow, and by `psql` (not by the trace code). Parquet shows 2 rows (batch 15, and batch 16 after the resend) |
| `spark_batches_for_demo_orders.txt` | The real `BATCH_SUMMARY` lines: batch 15 (the order), batch 16 (resend: `inserted 0, retry_duplicates 1`), batch 17 (Airflow run: 3 messages, `inserted 2, retry_duplicates 1`) |
| `airflow_a_spark_down_*`, `airflow_b_spark_down_*` | Two orders placed while Spark was stopped: the page shows stages 3 and 4 as waiting (not faked) |
| `airflow_dag_run_processes_waiting_demo_orders.txt` | Real DAG run `manual__20261016T023952` (`limit: 1`): `FINAL STATE: success`, 9 of 9 tasks. **Also keeps the history**: the run sat in `queued` for about 10 minutes because the Airflow scheduler had stopped; restarting the container fixed it |
| `airflow_streampulse_batch_grid.png`, `_graph.png`, `_dags_list.png`, `airflow_c_after_dag_order1_trace.png` | The Airflow UI after the run (third bar green) and the trace of an order that Airflow processed |
| `reconcile_production_with_demo_orders.txt` | Production reconciliation with 1 demo order present: exit 0; demo messages are explained; **also explains the +199 from run id `demo1` executed twice** |
| `run_id_reuse_demo1_in_parquet.txt` | `Rows per ingest_run_id` shows `demo1: 398` while its summary published 199 |
| `quality_checks_with_demo_orders.txt` | SQL quality checks still all pass |
| `e2e_experiment_demo_orders_isolated.txt`, `e2e_experiment_reconcile_output.txt` | Isolated end-to-end experiment (own topic, checkpoint, Parquet and database): 19 checks PASS (honest "waiting" before Spark, invalid input publishes nothing, same order in all four layers, `user_demo` vs `kafka_replayed`, historical view excludes demo, resend is a retry duplicate, reconcile passes) |

Test runs of the extension: `docs/evidence/10_automated_tests/demo_ext_*.txt`.

Not claimed: no customer data, no cart/payment, no live orders. The demo orders are test orders marked `user_demo`.
