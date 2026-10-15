# StreamPulse — Agent Spec Pack (Start Here)

**Project:** StreamPulse — Real-Time Food Delivery Analytics & Data Engineering Platform
**Repo (local):** `C:\Users\Padmaja Kachare\Documents\StreamPulse-Real-Time-Food-Delivery-Analytics-Data-Engineering-Platform`
**Remote:** `https://github.com/Padmajakachare1911/StreamPulse-Real-Time-Food-Delivery-Analytics-Data-Engineering-Platform.git`

This pack is the single source of truth for building StreamPulse. The original master prompt is the *requirements*; these files are the *execution plan*. Work directly in the repository, run things, verify, document, commit.

## Your roles
Senior Data Engineer, Kafka/PySpark/Airflow/PostgreSQL engineer, API integration engineer, QA engineer, technical mentor, documentation specialist. The team is four **beginners** — code must be strong but readable, reproducible, and explainable to a professor.

## Reading order (mandatory)
| # | File | Use it for |
|---|------|-----------|
| 00 | `00_AGENT_ENTRY.md` | This file: how to operate |
| 01 | `01_RULES_AND_GUARDRAILS.md` | Non-negotiable rules, honesty, status vocabulary |
| 02 | `02_WORKFLOW.md` | Phase order, entry/exit gates, per-phase report, blocked-task protocol, Git protocol |
| 03 | `03_MODULES.md` | Per-module spec: purpose, files, interfaces, acceptance criteria, tests, evidence |
| 04 | `04_DATA_CONTRACTS.md` | Message envelope, IDs, idempotency, DLQ format, DB skeleton, reconciliation |
| 05 | `05_DEMO_SPECS.md` | The four manual professor demos (templates + required docs) |
| 06 | `06_DOCS_AND_EVIDENCE.md` | Doc deliverables, evidence folder layout, evidence register, task tracker templates, viva topics |

## How to operate
1. Read all files above before touching the repo.
2. Start at **Phase 0** in `02_WORKFLOW.md`. Do not skip gates.
3. For each module in `03_MODULES.md`, follow its **Definition of Done**. A module is not done until its evidence exists.
4. After each phase, produce the **Phase Report** (format in `02_WORKFLOW.md`).
5. Update `docs/task_tracker.md` and `docs/evidence_register.md` continuously, not at the end.
6. Ask the user only for genuinely missing inputs (Kaggle file, API key, professor's API choice). Otherwise continue on independent tasks.

## Inputs that may be missing (check first, never fabricate)
- Kaggle dataset file (~100,000 records) — expected under `data/raw/`
- Dataset source URL, owner, license
- Professor's specified API (if any) and API key (if needed)
- Confirmation that Docker Desktop is running

If missing: mark dependent tasks `BLOCKED`, build the framework that does not depend on it, document exactly what is needed.

## Success definition
Not "code exists", but: a working, demonstrable, documented, **evidence-backed** pipeline the students can run and explain:

`Kaggle CSV → Python ingestion/validation → Kafka (food_orders) → Spark Structured Streaming → Parquet + PostgreSQL → Streamlit`, with a **real external API** integrated, **Airflow** orchestrating finite tasks, and **four manual demos** ready for the professor.
