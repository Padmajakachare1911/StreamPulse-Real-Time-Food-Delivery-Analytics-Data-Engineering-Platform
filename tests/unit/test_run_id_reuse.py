"""A run id names ONE execution. Reusing it once overwrote a run summary and hid 199 published messages
(found by scripts/reconcile.py). These tests cover the producer guard and the reconciliation's detection."""
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts.reconcile import run_id_reuse_excess
from src.producer.dataset_producer import unique_run_id

pytestmark = pytest.mark.unit


def test_unused_run_id_is_kept(tmp_path):
    assert unique_run_id("demo1", tmp_path) == "demo1"


def test_reused_run_id_gets_a_suffix_and_the_earlier_summary_is_untouched(tmp_path):
    (tmp_path / "demo1.json").write_text("first", encoding="utf-8")
    second = unique_run_id("demo1", tmp_path)
    assert second == "demo1-2"
    (tmp_path / f"{second}.json").write_text("second", encoding="utf-8")
    assert unique_run_id("demo1", tmp_path) == "demo1-3"
    assert (tmp_path / "demo1.json").read_text(encoding="utf-8") == "first"


def _parquet_with_run_ids(folder, run_ids):
    batch = folder / "batch_id=00000001"
    batch.mkdir(parents=True)
    pq.write_table(pa.table({"ingest_run_id": run_ids, "record_uid": [str(i) for i in range(len(run_ids))]}), batch / "part.parquet")


def test_excess_is_reported_only_when_parquet_holds_more_rows_than_the_summary_published(tmp_path):
    _parquet_with_run_ids(tmp_path, ["demo1"] * 6 + ["ok-run"] * 3 + ["short-run"] * 1)
    runs = [{"ingest_run_id": "demo1", "rows_published": 4}, {"ingest_run_id": "ok-run", "rows_published": 3},
            {"ingest_run_id": "short-run", "rows_published": 5}]            # fewer rows than published is normal (in-batch duplicates)
    assert run_id_reuse_excess(runs, tmp_path) == {"demo1": 2}


def test_no_parquet_means_nothing_to_explain(tmp_path):
    assert run_id_reuse_excess([{"ingest_run_id": "x", "rows_published": 1}], tmp_path) == {}
