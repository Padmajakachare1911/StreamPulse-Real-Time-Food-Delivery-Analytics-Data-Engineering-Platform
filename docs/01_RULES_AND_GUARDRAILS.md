# 01 — Rules and Guardrails

## A. Status vocabulary (use exactly these in `docs/task_tracker.md`)
| Status | Meaning |
|---|---|
| `NOT_STARTED` | No work yet |
| `IN_PROGRESS` | Being worked on |
| `BLOCKED` | Cannot proceed; reason + unblock action recorded |
| `IMPLEMENTED_NOT_TESTED` | Code written, never executed/tested |
| `TESTED` | Automated/manual test executed and passed |
| `VERIFIED_WITH_EVIDENCE` | Real execution result saved in `docs/evidence/…` and logged in the evidence register |

Never skip a level. Never mark `VERIFIED_WITH_EVIDENCE` without an evidence file path.

## B. Non-negotiable rules
1. **Inspect before editing.** Run `git status` first; protect pre-existing user changes.
2. **Preserve working functionality.** Don't rewrite the whole project.
3. **Use the real Kaggle dataset.** Never fabricate a substitute and call it Kaggle data.
4. **Genuine API only.** A mock/hard-coded/local JSON is never "the API". Mocks are allowed only in unit tests, and never count as proof.
5. **Honest framing:** historical data replayed through Kafka = *"historical dataset replay through a streaming pipeline"*, never "live customer orders".
6. **Never invent** columns, API responses, counts, logs, screenshots, test results.
7. **Don't assume column names.** Don't assume `order_id`, `customer_id`, `order_status`, `delivery_time` exist. Inspect, then build a documented mapping layer.
8. **Never silently drop rows.** Rejected rows are counted, stored with reasons.
9. **Secrets:** only in `.env` (git-ignored); `.env.example` has placeholders only; never in logs, evidence, screenshots.
10. **Respect API quotas/terms.** Batch, dedupe, cache; don't call per-record for 100k rows.
11. **No hard-coded machine paths** in source; use config/env.
12. **Reruns must be safe** (idempotent); document duplicate behavior.
13. **Don't claim exactly-once** end-to-end. Explain Kafka offsets, Spark checkpoints, DB idempotency separately. Say "at-least-once delivery + idempotent sink".
14. **Don't claim even partition distribution** unless measured.
15. **No fake green indicators** in dashboard (Kafka connected / API healthy / 100% processed) unless derived from real checks.
16. **Don't hide warnings blindly** (e.g., `winutils`/`HADOOP_HOME`); determine whether they affect processing. Never download executables from untrusted sources.
17. **Don't add technologies** just to look advanced.
18. **Don't modify tests just to make them pass** — preserve intent.
19. **No destructive commands** (`git reset --hard`, force-push, deleting data/checkpoints) without explicit user authorization.
20. **Distinguish** implemented ≠ executed ≠ tested ≠ verified.

## C. Source-category labelling (must be kept distinct in pipeline, DB, dashboard)
`kaggle_historical` · `kafka_replayed` · `api_sourced` · `api_enriched` · `validated_processed` · `rejected_dead_letter`

## D. Things only the human can do (prepare, mark PENDING)
- Placing the Kaggle file; supplying API key
- Capturing live screenshots in front of the professor
- Running the 4 manual demos
Prepare exact steps; mark `PENDING_HUMAN` in the evidence register until done.

## E. Code quality expectations (beginner-friendly)
- Small modules, clear names, docstrings explaining *why*.
- `logging` (not `print`) with levels; counters at every stage.
- Type hints on public functions.
- Config via one loader (`src/common/config.py`).
- Each script has `--help`, a `--dry-run`/`--limit` option where sensible.
- PowerShell-compatible commands in all docs (no Bash-only syntax unless explicitly inside WSL/Docker).
