"""The experiment checker must accept ONLY the deliberate stop signature, and still catch real errors (M4)."""
import importlib.util
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

_spec = importlib.util.spec_from_file_location("spark_experiments", Path(__file__).resolve().parents[2] / "scripts" / "spark_experiments.py")
experiments = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(experiments)
classify = experiments.classify_problem_lines


def test_clean_log_has_no_problems():
    assert classify(["INFO a", "INFO Stop requested", "INFO Stream stopped."]) == ([], [])


def test_the_deliberate_stop_signature_after_the_marker_is_accepted():
    lines = ["INFO BATCH_SUMMARY", "INFO Stop requested: no new batch will start",
             "ERROR MicroBatchExecution: Query terminated with error",
             "Traceback (most recent call last):", "StopRequested: stop requested: batch 3 was not started"]
    unexpected, stop_related = classify(lines)
    assert unexpected == [] and len(stop_related) == 2     # the ERROR line and the Traceback line (the exception line itself holds no error keyword)


def test_the_old_interruption_without_the_signature_is_still_unexpected():
    lines = ["INFO Stop requested", "ERROR py4j.clientserver There was an exception while executing the Python Proxy",
             "java.lang.InterruptedException"]
    unexpected, stop_related = classify(lines)
    assert len(unexpected) == 2 and stop_related == []


def test_errors_before_the_stop_request_are_always_unexpected_even_if_the_signature_appears_later():
    lines = ["ERROR something broke early", "INFO Stop requested", "StopRequested: stop requested: batch 4 was not started"]
    unexpected, _ = classify(lines)
    assert unexpected == ["ERROR something broke early"]


def test_errors_without_any_stop_request_are_unexpected():
    unexpected, stop_related = classify(["INFO ok", "Exception: boom"])
    assert unexpected == ["Exception: boom"] and stop_related == []
