"""Parquet persistence (M4/M6): the layout Spark writes (one batch_id=NNNNNNNN folder per batch) is read back
correctly by the dashboard/reconciliation helpers. Uses real Parquet files written with pyarrow."""
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.dashboard import queries

pytestmark = pytest.mark.unit


def write_batch(root, batch_id: int, uids: list[str]) -> None:
    folder = root / f"batch_id={batch_id:08d}"
    folder.mkdir(parents=True)
    pq.write_table(pa.table({"record_uid": uids, "order_id": [f"o-{u}" for u in uids]}), folder / "part-0.snappy.parquet")


def test_counts_rows_files_and_distinct_ids_across_batch_folders(tmp_path):
    write_batch(tmp_path, 0, ["a", "b", "c"])
    write_batch(tmp_path, 1, ["c", "d"])           # "c" arrives again in a later batch: kept in the landing layer
    counts = queries.parquet_counts(tmp_path)
    assert counts == {"files": 2, "rows": 5, "distinct_record_uid": 4}


def test_overwriting_one_batch_folder_does_not_change_other_batches(tmp_path):
    write_batch(tmp_path, 0, ["a", "b"])
    write_batch(tmp_path, 1, ["c"])
    # a re-run of batch 1 overwrites ITS folder only (what the Spark job does)
    (tmp_path / "batch_id=00000001" / "part-0.snappy.parquet").unlink()
    pq.write_table(pa.table({"record_uid": ["c"], "order_id": ["o-c"]}), tmp_path / "batch_id=00000001" / "part-0.snappy.parquet")
    assert queries.parquet_counts(tmp_path)["rows"] == 3
