"""StreamPulse dashboard (Streamlit). Presentation only: data comes from queries.py, service status from status.py.

Run:  .\scripts\run_dashboard.ps1     then open http://localhost:8501

Every number is read from PostgreSQL / Parquet / Kafka when the page loads (cached for 30 s).
When a source is unavailable the page says so; nothing is shown as healthy unless a real check passed.
"""
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

import pandas as pd  # noqa: E402
import streamlit as st  # noqa: E402

from src.common.config import EVIDENCE_DIR, KAFKA_TOPIC  # noqa: E402
from src.dashboard import queries, status  # noqa: E402
from src.demo import demo_order, order_trace  # noqa: E402

st.set_page_config(page_title="StreamPulse", page_icon="📦", layout="wide")

FRAMING = ("Historical dataset replay through a streaming pipeline (Olist Brazilian e-commerce orders, 2016-2018). "
           "These are not live orders.")
ICON = {"ok": "🟢", "down": "🔴", "unknown": "⚪"}


@st.cache_data(ttl=30, show_spinner=False)
def cached(function_name: str, *args):
    """Run a function of queries.py; errors are returned (not raised) so the page can show a clear message."""
    try:
        return getattr(queries, function_name)(*args), None
    except queries.QueryError as exc:
        return None, str(exc)


def load(function_name: str, *args) -> pd.DataFrame | None:
    data, error = cached(function_name, *args)
    if error:
        st.error(f"Unavailable: {error}")
        return None
    return data


def show_table(frame: pd.DataFrame, empty_message: str) -> None:
    """A table, or a plain sentence when there are no rows. (Written as if/else on purpose: a conditional
    EXPRESSION on its own line would make Streamlit print the returned object.)"""
    if frame.empty:
        st.write(empty_message)
    else:
        st.dataframe(frame, hide_index=True, use_container_width=True)


def metric_row(items: list[tuple[str, object]]) -> None:
    for column, (label, value) in zip(st.columns(len(items)), items):
        column.metric(label, f"{value:,}" if isinstance(value, (int, float)) and not isinstance(value, bool) else str(value))


# ---------------------------------------------------------------------------------------------------
def page_overview() -> None:
    st.title("StreamPulse")
    st.caption("Real-Time Order Delivery Analytics & Data Engineering Platform (Olist e-commerce dataset)")
    st.info(FRAMING)
    st.subheader("Architecture (what is implemented)")
    st.code("Olist CSV files -> Python replay producer (join, validate, enrich) -> Kafka topic 'ecommerce_orders'\n"
            "   -> Spark Structured Streaming (Docker) -> Parquet landing layer  +  PostgreSQL warehouse -> this dashboard\n"
            "External API: Frankfurter / ECB exchange rates (BRL -> USD) cached in PostgreSQL\n"
            "Airflow orchestrates the finite batch steps; the continuous stream is not an Airflow task.", language="text")
    st.subheader("Service status (real checks)")
    for item in status.all_checks():
        st.write(f"{ICON[item.state]} **{item.name}**: {item.detail}")
    if st.button("Run a live check of the exchange-rate API (1 request)"):
        result = status.check_api_now()
        st.write(f"{ICON[result.state]} **{result.name}**: {result.detail}")


def page_demo_order() -> None:
    st.header("Place a demo order and trace it")
    st.warning("**Demo orders are new, user-submitted test orders.** They are marked `source_category = user_demo` and an "
               "`order_id` starting with `demo-`, and they are kept out of the historical Olist KPIs. "
               "Historical Olist records are replayed from the dataset and are never created here.")
    st.caption("Path: this form -> Kafka topic -> Spark Structured Streaming (Docker) -> Parquet + PostgreSQL. "
               "Start the stream first with `.\\scripts\\run_stream.ps1` (or run the Airflow DAG, which processes what is waiting).")
    with st.form("demo_order_form"):
        left, right = st.columns(2)
        state = left.selectbox("Customer state", demo_order.BRAZIL_STATES, index=demo_order.BRAZIL_STATES.index("SP"))
        city = left.text_input("Customer city", "sao paulo")
        zip_prefix = left.text_input("Zip code prefix (5 digits)", "01310")
        payment = left.selectbox("Payment type", demo_order.PAYMENT_TYPES)
        items = right.number_input("Number of items", min_value=1, max_value=20, value=2, step=1)
        price = right.number_input("Items price total (BRL)", min_value=0.01, max_value=100000.0, value=150.0, step=10.0)
        freight = right.number_input("Freight total (BRL)", min_value=0.0, max_value=10000.0, value=20.0, step=5.0)
        order_status = right.selectbox("Order status", demo_order.DEMO_STATUSES)
        submitted = st.form_submit_button("Place demo order")
    if submitted:
        try:
            record = demo_order.submit_demo_order(demo_order.DemoOrderInput(
                customer_state=state, customer_city=city, customer_zip_code_prefix=zip_prefix, items_count=int(items),
                items_price_total=float(price), items_freight_total=float(freight), payment_type=payment, order_status=order_status))
            st.session_state["demo_order_id"] = record["order_id"]
            st.success(f"Published to Kafka: partition {record['partition']}, offset {record['offset']}. Order id: {record['order_id']}")
        except demo_order.DemoOrderError as exc:
            st.error(f"The order was not published: {exc}")

    st.subheader("Trace an order")
    order_id = st.text_input("Order id to trace", st.session_state.get("demo_order_id", ""))
    auto = st.checkbox("Refresh automatically every 3 seconds until the order is stored", value=True)
    st.button("Refresh now")
    if order_id.strip():
        result = order_trace.trace_order(order_id.strip())
        marks = {"done": "✅", "waiting": "⏳", "unavailable": "⚠️"}
        for stage in result["stages"]:
            st.write(f"{marks[stage.state]} **{stage.name}**: {stage.detail}")
        if result["state"] == "complete":
            seconds = result["latency_seconds"]
            st.success("Complete: the same order was found in Kafka, Spark/Parquet and PostgreSQL." +
                       (f" Submit to PostgreSQL took {seconds} s." if seconds is not None else ""))
        with st.expander("Technical details of each stage"):
            st.json({stage.name: stage.facts for stage in result["stages"]}, expanded=False)
        if st.button("Send the identical message again (duplicate test)"):
            try:
                again = demo_order.resend_demo_order(order_id.strip())
                st.info(f"Sent again to partition {again['partition']}, offset {again['offset']}. Spark counts it as a retry duplicate; "
                        "PostgreSQL keeps one row for this order.")
            except demo_order.DemoOrderError as exc:
                st.error(str(exc))
        if auto and result["state"] == "in_progress":
            time.sleep(3)
            st.rerun()

    st.subheader("Demo orders stored in PostgreSQL")
    try:                                    # not cached: this table must follow the live trace above
        show_table(queries.recent_demo_orders(10), "No demo order has reached PostgreSQL yet.")
    except queries.QueryError as exc:
        st.error(f"Unavailable: {exc}")


def page_dataset() -> None:
    st.header("Dataset and provenance")
    st.write("**Brazilian E-Commerce Public Dataset by Olist** (Kaggle, license CC BY-NC-SA 4.0). Historical, anonymized, not live.")
    profile_path = EVIDENCE_DIR / "01_dataset" / "dataset_profile.json"
    if profile_path.exists():
        profile = json.loads(profile_path.read_text(encoding="utf-8"))
        table = pd.DataFrame([{"file": t["file"], "rows": t["rows"], "columns": t["column_count"], "sha256": t["sha256"][:16] + "..."}
                              for t in profile["tables"].values()])
        st.caption(f"Row counts from the profiling run of {profile['generated_at_utc'][:10]} (docs/evidence/01_dataset)")
        st.dataframe(table, hide_index=True, use_container_width=True)
    else:
        st.warning("Profiling report not found.")
    st.subheader("Warehouse")
    overview = load("overview")
    if overview is not None and not overview.empty:
        row = overview.iloc[0]
        metric_row([("Orders in PostgreSQL", int(row["orders"])), ("Distinct record_uid", int(row["distinct_record_uid"])),
                    ("First purchase", str(row["first_purchase"])[:10]), ("Last purchase", str(row["last_purchase"])[:10])])
    files = load("source_files")
    if files is not None:
        st.caption("Source file fingerprint (SHA-256) that each warehouse row came from")
        st.dataframe(files, hide_index=True, use_container_width=True)
    refs = load("reference_table_counts")
    if refs is not None:
        st.caption("Reference tables loaded in batch")
        st.dataframe(refs, hide_index=True, use_container_width=True)


def page_pipeline() -> None:
    st.header("Ingestion and processing counts")
    runs = load("ingestion_runs")
    if runs is not None:
        st.caption("Each row is one replay run. Runs can overlap (the same orders replayed again), so do not add them up as unique orders.")
        st.dataframe(runs, hide_index=True, use_container_width=True)
    st.subheader("Layer reconciliation (live)")
    kafka = status.kafka_topic_messages()
    try:
        parquet = queries.parquet_counts()
    except queries.QueryError as exc:
        parquet = None
        st.error(f"Parquet: {exc}")
    overview = load("overview")
    dups = load("duplicate_summary")
    if kafka is None:
        st.error(f"Kafka message count unavailable (topic {KAFKA_TOPIC}).")
    if kafka is not None and parquet and overview is not None and not overview.empty:
        pg_rows, pg_distinct = int(overview.iloc[0]["orders"]), int(overview.iloc[0]["distinct_record_uid"])
        discarded = int(dups["records"].sum()) if dups is not None and not dups.empty else 0
        table = pd.DataFrame([
            {"layer": "Kafka messages", "count": kafka, "meaning": "everything published, repeats included"},
            {"layer": "Parquet rows (landing layer)", "count": parquet["rows"], "meaning": "valid messages Spark consumed, append-only"},
            {"layer": "Parquet distinct record_uid", "count": parquet["distinct_record_uid"], "meaning": "unique valid records"},
            {"layer": "PostgreSQL orders", "count": pg_rows, "meaning": "deduplicated warehouse"},
        ])
        st.dataframe(table, hide_index=True, use_container_width=True)
        repeats = parquet["rows"] - parquet["distinct_record_uid"]
        st.write(f"- Kafka minus Parquet rows = **{kafka - parquet['rows']:,}** (malformed messages and duplicates removed inside a batch)")
        st.write(f"- Parquet rows minus distinct = **{repeats:,}** repeats of an already stored record_uid (retry duplicates)")
        gap = parquet["distinct_record_uid"] - pg_rows
        verdict = "matches" if gap == discarded else "DOES NOT MATCH"
        st.write(f"- Parquet distinct minus PostgreSQL = **{gap:,}**; discarded source duplicates logged = **{discarded:,}** ({verdict})")
        if pg_rows != pg_distinct:
            st.error("PostgreSQL has repeated record_uid values: investigate.")
    demo_count = load("demo_order_count")
    if demo_count is not None and not demo_count.empty and int(demo_count.iloc[0]["demo_orders"]) > 0:
        st.write(f"- Of the PostgreSQL orders, **{int(demo_count.iloc[0]['demo_orders']):,}** are user-submitted demo orders (the rest are historical Olist orders).")
    st.caption("Full stage-by-stage reconciliation with expected values computed from the source: `python scripts/reconcile.py`")


def page_quality() -> None:
    st.header("Data quality")
    comp = load("completeness")
    if comp is not None and not comp.empty:
        row = comp.iloc[0]
        metric_row([("Orders without items (kept)", int(row["orders_without_items"])), ("Orders without payments (kept)", int(row["orders_without_payments"])),
                    ("Delivered without date", int(row["delivered_without_date"])), ("Delivered before purchase", int(row["delivered_before_purchase"]))])
    st.subheader("Rejected records (dead-letter queue)")
    dead = load("dead_letter_summary")
    if dead is not None:
        show_table(dead, "No rejected records.")
        st.caption("records = dead-letter lines (a record rejected in two overlapping runs appears twice); distinct_records counts each record once.")
    st.subheader("Duplicates")
    dups = load("duplicate_summary")
    if dups is not None:
        show_table(dups, "No source duplicates were found in the data processed so far.")


def page_kpis() -> None:
    st.header("Business KPIs")
    st.caption("Revenue = payments_value_total (what customers paid, incl. freight). USD values are an estimate: BRL x ECB reference rate of the purchase day.")
    summary = load("revenue_summary")
    if summary is not None and not summary.empty:
        row = summary.iloc[0]
        metric_row([("Orders", int(row["orders"])), ("Revenue (BRL)", f"{float(row['revenue_brl']):,.0f}"),
                    ("Revenue (USD, estimate)", f"{float(row['revenue_usd_estimate']):,.0f}" if pd.notna(row["revenue_usd_estimate"]) else "n/a"),
                    ("Average order value (BRL)", f"{float(row['avg_order_value_brl']):,.2f}")])
    late = load("late_delivery")
    if late is not None and not late.empty:
        row = late.iloc[0]
        metric_row([("Delivered orders", int(row["delivered_orders"])), ("Late deliveries", int(row["late_orders"])),
                    ("Late %", f"{float(row['late_percent']):.2f}"), ("Average delivery days", f"{float(row['avg_delivery_days']):.2f}")])
    left, right = st.columns(2)
    with left:
        monthly = load("orders_per_month")
        if monthly is not None and not monthly.empty:
            st.subheader("Orders per month")
            st.bar_chart(monthly.set_index("order_month")["orders"])
        states = load("orders_by_state", 10)
        if states is not None and not states.empty:
            st.subheader("Top 10 customer states")
            st.bar_chart(states.set_index("customer_state")["orders"])
        pay = load("payment_mix")
        if pay is not None and not pay.empty:
            st.subheader("Payment types (orders using the type)")
            st.bar_chart(pay.set_index("payment_type")["orders_using_type"])
    with right:
        statuses = load("status_distribution")
        if statuses is not None and not statuses.empty:
            st.subheader("Order status")
            st.dataframe(statuses, hide_index=True, use_container_width=True)
        delivery = load("delivery_time_by_month")
        if delivery is not None and not delivery.empty:
            st.subheader("Average delivery days by purchase month")
            st.line_chart(delivery.set_index("order_month")["avg_delivery_days"])
        cats = load("top_categories", 10)
        if cats is not None and not cats.empty:
            st.subheader("Top 10 product categories (items sold)")
            st.bar_chart(cats.set_index("category")["items_sold"])
        reviews = load("review_scores")
        if reviews is not None and not reviews.empty:
            st.subheader("Review scores")
            st.bar_chart(reviews.set_index("review_score")["reviews"])


def page_api() -> None:
    st.header("External API: exchange rates (Frankfurter / ECB)")
    st.write("Source label: `api_enriched`. The API provides reference data (BRL -> USD rates), not orders.")
    summary = load("api_enrichment")
    if summary is not None and not summary.empty:
        row = summary.iloc[0]
        metric_row([("Orders", int(row["orders"])), ("Enriched with a rate", int(row["orders_enriched"])),
                    ("Match rate %", f"{float(row['match_rate_percent']):.2f}"),
                    ("Matched on previous business day", int(row["matched_previous_business_day"]))])
        st.write(f"Last API retrieval stored on records: **{str(row['last_api_retrieval'])[:19]}**, HTTP status **{row['last_http_status']}**")
    st.write(f"{ICON[status.check_api_last_success().state]} {status.check_api_last_success().detail}")
    rates = load("fx_rates")
    if rates is not None and not rates.empty:
        st.subheader("BRL -> USD rate stored from the API")
        st.line_chart(rates.set_index("rate_date")["rate"])


def page_records() -> None:
    st.header("Most recently processed orders")
    st.caption("Business columns only. Customer identifiers are not displayed. source = kafka_replayed (historical Olist) or user_demo (placed from the dashboard).")
    limit = st.slider("Rows", 5, 100, 20)
    recent = load("recent_orders", limit)
    if recent is not None:
        show_table(recent, "The warehouse is empty.")
    overview = load("overview")
    if overview is not None and not overview.empty:
        st.write(f"Last processed into PostgreSQL: **{str(overview.iloc[0]['last_processed_at'])[:19]}**")
    runs = load("ingestion_runs")
    if runs is not None and not runs.empty:
        st.write(f"Last ingestion run started: **{str(runs['started_at'].max())[:19]}**")


PAGES = {"Overview and status": page_overview, "Place a demo order": page_demo_order, "Dataset and provenance": page_dataset, "Ingestion and reconciliation": page_pipeline,
         "Data quality": page_quality, "Business KPIs": page_kpis, "External API": page_api, "Recent records": page_records}

choice = st.sidebar.radio("Page", list(PAGES))
st.sidebar.caption(FRAMING)
if st.sidebar.button("Refresh data"):
    st.cache_data.clear()
PAGES[choice]()
