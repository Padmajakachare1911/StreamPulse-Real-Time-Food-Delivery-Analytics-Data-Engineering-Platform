"""One place that decides whether a source row is accepted or rejected.

Used by the producer (to publish or dead-letter a row) and by scripts/reconcile.py
(to recompute the expected counts from the source file with the very same rules).
"""
from src.common.mapping import apply_mapping
from src.processing.validators import split_by_severity, validate_payload


def classify_record(row: dict, mapping: dict, rules: dict) -> tuple[dict | None, list[dict], list[dict]]:
    """Return (payload, problems, warnings).

    payload  : typed canonical record, or None when mapping failed
    problems : reasons to reject, each {"reason_code", "detail"}; empty means the row is accepted
    warnings : rules with severity 'warn' that fired (the row is still accepted)
    """
    payload, errors = apply_mapping(row, mapping)
    problems = [{"reason_code": e["reason_code"], "detail": f"{e['field']}: {e['detail']}"} for e in errors]
    warnings: list[dict] = []
    if payload is not None:
        rejections, warnings = split_by_severity(validate_payload(payload, rules))
        problems += [{"reason_code": r["reason_code"], "detail": f"{r['rule']}: {r['detail']}"} for r in rejections]
    return payload, problems, warnings
