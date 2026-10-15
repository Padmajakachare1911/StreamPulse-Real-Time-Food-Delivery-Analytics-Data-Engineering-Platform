"""Dashboard status checks and error handling (M8). No running service is needed: the checks are pointed
at things that really do not exist, so "down" is produced by a real failed check, not by a mock answer."""
import psycopg2
import pytest

from src.dashboard import queries, status

pytestmark = pytest.mark.unit


def test_kafka_check_reports_down_for_an_unreachable_broker():
    result = status.check_kafka(bootstrap="localhost:1", timeout_ms=1500)
    assert result.state == "down" and "not reachable" in result.detail


def test_kafka_message_count_is_none_when_broker_is_unreachable():
    assert status.kafka_topic_messages(bootstrap="localhost:1", timeout_ms=1500) is None


def test_postgres_check_reports_down_when_the_query_fails(monkeypatch):
    def broken(sql, params=None):
        raise queries.QueryError("The database is unavailable or the query failed.")
    monkeypatch.setattr(status.queries, "run_query", broken)
    assert status.check_postgres().state == "down"


def test_api_history_is_unknown_when_the_database_is_down(monkeypatch):
    def broken():
        raise queries.QueryError("x")
    monkeypatch.setattr(status.queries, "api_cache_last_success", broken)
    assert status.check_api_last_success().state == "unknown"


def test_dataset_and_parquet_checks_fail_for_empty_folders(tmp_path):
    assert status.check_dataset(tmp_path).state == "down"
    assert status.check_parquet(tmp_path).state == "down"


def test_dataset_check_is_ok_only_with_all_nine_files(tmp_path):
    for i in range(9):
        (tmp_path / f"f{i}.csv").write_text("a\n")
    assert status.check_dataset(tmp_path).state == "ok"


def test_airflow_check_reports_down_when_nothing_listens():
    assert status.check_airflow("http://localhost:1/health").state == "down"


def test_database_errors_are_sanitised_for_users(monkeypatch):
    def leaking(connect_timeout=3):
        raise psycopg2.OperationalError('connection to server at "10.9.8.7", port 5432 failed: password authentication failed for user "secretuser"')
    monkeypatch.setattr(queries, "get_connection", leaking)
    with pytest.raises(queries.QueryError) as caught:
        queries.run_query("SELECT 1")
    message = str(caught.value)
    assert "10.9.8.7" not in message and "secretuser" not in message and "password" not in message.lower()


def test_parquet_counts_raises_a_clear_error_when_there_is_no_data(tmp_path):
    with pytest.raises(queries.QueryError, match="No Parquet files"):
        queries.parquet_counts(tmp_path)


def test_decimal_columns_become_floats_so_charts_plot_numbers():
    from decimal import Decimal
    import pandas as pd
    frame = pd.DataFrame({"month": ["2017-01", "2017-02"], "avg_days": [Decimal("12.50"), Decimal("9.25")], "orders": [1, 2]})
    out = queries.numeric_columns_to_float(frame)
    assert out["avg_days"].dtype == float and out["avg_days"].tolist() == [12.5, 9.25]
    assert out["month"].tolist() == ["2017-01", "2017-02"] and out["orders"].tolist() == [1, 2]   # other columns untouched
