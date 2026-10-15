"""Real service checks for the dashboard. A status is "ok" only when a check really succeeded.

Each check actually talks to the service (database query, Kafka metadata request, file system, HTTP).
When the service is down the status is "down" with a short, safe reason; when a check cannot be made it
is "unknown". Nothing here is hard-coded green.
"""
import logging
from dataclasses import dataclass
from pathlib import Path

import requests

from src.common.config import DATASET_DIR, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, PARQUET_OUTPUT_DIR
from src.dashboard import queries

log = logging.getLogger("dashboard.status")
EXPECTED_DATASET_FILES = 9


@dataclass
class Status:
    name: str
    state: str          # "ok" | "down" | "unknown"
    detail: str


def check_postgres() -> Status:
    try:
        queries.run_query("SELECT 1")
        return Status("PostgreSQL", "ok", "query succeeded")
    except queries.QueryError:
        return Status("PostgreSQL", "down", "not reachable or query failed")


def check_kafka(bootstrap: str = KAFKA_BOOTSTRAP_SERVERS, topic: str = KAFKA_TOPIC, timeout_ms: int = 3000) -> Status:
    """Ask the broker for topic metadata (a real request)."""
    try:
        from kafka import KafkaConsumer
        consumer = KafkaConsumer(bootstrap_servers=bootstrap, request_timeout_ms=timeout_ms)
        try:
            partitions = consumer.partitions_for_topic(topic)
        finally:
            consumer.close()
    except Exception as exc:
        log.error("kafka check failed: %s", type(exc).__name__)
        return Status("Kafka", "down", "broker not reachable")
    if not partitions:
        return Status("Kafka", "down", f"broker reachable, but topic '{topic}' not found")
    return Status("Kafka", "ok", f"topic '{topic}' has {len(partitions)} partition(s)")


def check_api_last_success() -> Status:
    """Last successful exchange-rate API response stored in PostgreSQL (no call is made)."""
    try:
        row = queries.api_cache_last_success().iloc[0]
    except queries.QueryError:
        return Status("Exchange-rate API", "unknown", "cannot read API history (database unavailable)")
    if row["last_success"] is None or str(row["last_success"]) == "NaT":
        return Status("Exchange-rate API", "unknown", "no successful call recorded yet")
    return Status("Exchange-rate API", "ok", f"last successful response stored {str(row['last_success'])[:19]} (UTC)")


def check_api_now() -> Status:
    """One real request to the API (used by a button; never automatic)."""
    from src.api.client import ApiError, FrankfurterClient
    from src.common.config import API_BASE_URL, API_USER_AGENT
    try:
        result = FrankfurterClient(API_BASE_URL, API_USER_AGENT, cache_dir=None, max_retries=1, backoff_seconds=1).get_rates(
            "BRL", "USD", "2017-10-02", "2017-10-06")
    except ApiError:
        return Status("Exchange-rate API (live check)", "down", "request failed")
    except requests.RequestException:
        return Status("Exchange-rate API (live check)", "down", "not reachable")
    return Status("Exchange-rate API (live check)", "ok", f"HTTP {result.provenance['api_http_status']}, {len(result.rows)} rates")


def check_parquet(parquet_dir: Path = PARQUET_OUTPUT_DIR) -> Status:
    files = list(Path(parquet_dir).rglob("*.parquet"))
    return Status("Parquet landing layer", "ok", f"{len(files)} file(s)") if files else Status("Parquet landing layer", "down", "no Parquet files found")


def check_dataset(dataset_dir: Path = DATASET_DIR) -> Status:
    found = len(list(Path(dataset_dir).glob("*.csv")))
    if found == EXPECTED_DATASET_FILES:
        return Status("Dataset files", "ok", f"{found} CSV files present")
    return Status("Dataset files", "down", f"{found} of {EXPECTED_DATASET_FILES} CSV files found")


def check_airflow(url: str = "http://localhost:8080/health") -> Status:
    """Optional service: it is only needed for orchestration, not for viewing results."""
    try:
        health = requests.get(url, timeout=2).json()
        scheduler = health.get("scheduler", {}).get("status")
        return Status("Airflow (optional)", "ok" if scheduler == "healthy" else "down", f"scheduler {scheduler}")
    except (requests.RequestException, ValueError):
        return Status("Airflow (optional)", "down", "not running")


def all_checks() -> list[Status]:
    return [check_postgres(), check_kafka(), check_api_last_success(), check_parquet(), check_dataset(), check_airflow()]


def kafka_topic_messages(bootstrap: str = KAFKA_BOOTSTRAP_SERVERS, topic: str = KAFKA_TOPIC, timeout_ms: int = 3000) -> int | None:
    """Messages currently in the topic (end offsets minus start offsets), or None when Kafka cannot be reached."""
    try:
        from kafka import KafkaConsumer, TopicPartition
        consumer = KafkaConsumer(bootstrap_servers=bootstrap, request_timeout_ms=timeout_ms)
        try:
            partitions = [TopicPartition(topic, p) for p in (consumer.partitions_for_topic(topic) or [])]
            if not partitions:
                return None
            return int(sum(consumer.end_offsets(partitions).values()) - sum(consumer.beginning_offsets(partitions).values()))
        finally:
            consumer.close()
    except Exception as exc:
        log.error("kafka count failed: %s", type(exc).__name__)
        return None
