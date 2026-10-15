"""Unit tests for the reconciliation accounting rules (scripts/reconcile.py): no services needed."""
import importlib.util
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

_spec = importlib.util.spec_from_file_location("reconcile", Path(__file__).resolve().parents[2] / "scripts" / "reconcile.py")
reconcile = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(reconcile)


def test_run_slice_defaults_to_the_start_of_the_file():
    assert reconcile.run_slice({"rows_read": 100}) == (0, 100)
    assert reconcile.run_slice({"rows_read": 50, "skip_rows": 200}) == (200, 250)


def test_overlapping_runs_are_not_counted_twice_at_source_level():
    small = {"rows_read": 100}                       # rows 0..99
    full = {"rows_read": 1000}                       # rows 0..999 (overlaps the small run)
    covered = reconcile.union_positions([small, full], source_rows=1000)
    assert len(covered) == 1000                      # not 1,100
    assert sum(r["rows_read"] for r in (small, full)) - len(covered) == 100   # rows replayed more than once


def test_union_respects_the_end_of_the_source_file():
    assert len(reconcile.union_positions([{"rows_read": 5000, "skip_rows": 90}], source_rows=100)) == 10


def test_disjoint_slices_add_up():
    runs = [{"rows_read": 60}, {"rows_read": 40, "skip_rows": 60}]
    assert len(reconcile.union_positions(runs, source_rows=100)) == 100


def test_status_ok_explained_and_unexplained():
    assert reconcile.status_of({"expected": 10, "actual": 10}) == "OK"
    assert reconcile.status_of({"expected": 10, "actual": 7, "explained": {"discarded": -3}}) == "EXPLAINED"
    assert reconcile.status_of({"expected": 10, "actual": 7, "explained": {"discarded": -2}}) == "UNEXPLAINED"
    assert reconcile.status_of({"expected": 10, "actual": 12}) == "UNEXPLAINED"


def test_render_reports_failure_only_for_unexplained_differences():
    ok_rows = [{"stage": "a", "expected": 5, "actual": 5, "note": ""},
               {"stage": "b", "expected": 5, "actual": 4, "explained": {"x": -1}, "note": ""}]
    text, ok = reconcile.render([("section", ok_rows)])
    assert ok and "all differences are zero or explained" in text

    bad_rows = ok_rows + [{"stage": "c", "expected": 5, "actual": 9, "note": ""}]
    text, ok = reconcile.render([("section", bad_rows)])
    assert not ok and "UNEXPLAINED MISMATCH FOUND" in text and "[UNEXPLAINED]" in text
