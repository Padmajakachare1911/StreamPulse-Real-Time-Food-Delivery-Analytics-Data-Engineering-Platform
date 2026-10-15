Phase: 10 - Manual demonstrations and viva preparation
Completed: four demos written from docs/05_DEMO_SPECS.md with every field of the template; every command dry-run tested; cheat sheet (terminal, app, directory, command, stays open, success, independent check, failure fix, safe stop); test cases; viva answers; PENDING_HUMAN evidence placeholders.
Files changed: docs/{manual_demo_guide,demo_cheatsheet,test_cases,viva_questions}.md, scripts/{demo_setup,demo_load,demo3_cleanup,reset_demo_state}.ps1, scripts/demo_db.py, scripts/{run_stream,stop_stream,run_dashboard}.ps1 (-Demo option), docs/evidence/11_manual_demos/{demo_*/README.md,dry_runs/*}
Design decision: Demos 3 and 4 run in ISOLATED environments (own topic demo<N>_orders, database streampulse_demo<N>, checkpoint/Parquet/dead-letter folder data/test_runs/demo<N>), so the live demonstration can be repeated and cleaned up without touching the production checkpoint or warehouse. scripts/demo_db.py refuses any database name that does not start with "streampulse_demo"; reset_demo_state.ps1 removes only that demo's topic, database, folder and demo<N>-* run files, and asks for YES.
Dry-run results (docs/evidence/11_manual_demos/dry_runs/):
  Demo 1+2: kafka_start, init_db, fingerprint, profile, 200-order replay, Spark, Parquet read-back, SQL, reconcile (exit 0), live API HTTP 200, simulated failure (3 attempts).
  Demo 3: 3 read = 2 valid + 1 rejected; dead letter RANGE_ERROR; 1 order after two ingestions; source_duplicate 1; Parquet 4 rows/2 distinct; dashboard shows dead letters (2 lines, 1 distinct) after demo_load; scoped cleanup; production intact (99,427 rows, checkpoint, topic).
  Demo 4: stop after 3 committed batches; +1000 new and +99 repeats while down; restart from batch 3; 3,995 unique orders; 99 retry duplicates ignored; reconcile exit 0; scoped reset; production intact. Airflow part verified in Phase 7.
Findings from the dry runs (fixed): the dashboard magic bug (see phase 9); Demo 3 needed a step to load dead letters into the demo database (demo_load.ps1); Demo 2 wording about what is persisted; the safe-stop log lines (StopRequested) are explained in the guide so they are not mistaken for failures.
Human-only (PENDING_HUMAN, not performed): the four live demonstrations, their observed-output fields, and their screenshots (docs/evidence/11_manual_demos/*/README.md list exactly what to capture and the command that produces it). The agent never claims the demos were performed.
Git: see commit "docs(demos): ..." (hash in docs/task_tracker.md)
Blocked items: none.
Next phase: 11 - final audit
