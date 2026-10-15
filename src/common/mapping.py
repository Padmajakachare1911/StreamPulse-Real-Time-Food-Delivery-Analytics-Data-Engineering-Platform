"""Apply config/field_mapping.yaml: rename source columns and convert types.

`apply_mapping` never raises for bad data. It returns (payload or None, errors),
so the caller can send the record to the dead-letter queue with a reason.
"""
from datetime import datetime
from pathlib import Path

import yaml

MISSING_REQUIRED = "MISSING_REQUIRED"
TYPE_ERROR = "TYPE_ERROR"


def load_mapping(path: Path) -> dict:
    """Read the YAML mapping file."""
    with Path(path).open(encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def _is_empty(value) -> bool:
    """None, NaN and blank text all count as 'no value'."""
    if value is None:
        return True
    if isinstance(value, float) and value != value:  # NaN
        return True
    return isinstance(value, str) and not value.strip()


def _convert(value, type_name: str, timestamp_format: str):
    """Convert one value to its declared type. Raises ValueError on bad input."""
    if type_name == "string":
        return str(value).strip()
    if type_name == "int":
        return int(float(value))  # accepts "3" and "3.0"
    if type_name == "float":
        return float(value)
    if type_name == "timestamp":
        # ISO-8601 text without time zone, e.g. 2017-10-02T10:56:33
        return datetime.strptime(str(value).strip(), timestamp_format).isoformat()
    raise ValueError(f"unknown type in mapping: {type_name}")


def apply_mapping(record: dict, mapping: dict) -> tuple[dict | None, list[dict]]:
    """Map one source record to canonical fields.

    Returns (payload, errors). payload is None when any error was found.
    Extra source columns that are not in the mapping are left untouched in the
    caller's `raw` copy of the record (nothing is thrown away).
    """
    timestamp_format = mapping["timestamp_format"]
    payload: dict = {}
    errors: list[dict] = []

    for field in mapping["fields"]:
        source = field["source_column"]
        name = field["canonical_name"]
        value = record.get(source)

        if _is_empty(value):
            if field["required"]:
                errors.append({"field": name, "reason_code": MISSING_REQUIRED,
                               "detail": f"required column '{source}' is empty or missing"})
            payload[name] = None
            continue

        try:
            payload[name] = _convert(value, field["type"], timestamp_format)
        except (ValueError, TypeError):
            errors.append({"field": name, "reason_code": TYPE_ERROR,
                           "detail": f"column '{source}' value {value!r} is not a valid {field['type']}"})
            payload[name] = None

    return (None if errors else payload), errors
