"""Dead-letter records: rejected data is stored with a reason, never dropped silently.

Format follows docs/04 section 4. Files: data/dead_letter/<stage>/<run_id>.jsonl
"""
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path


def make_dead_letter(stage: str, reason_code: str, reason_detail: str, raw_payload,
                     ingest_run_id: str, record_uid: str | None = None) -> dict:
    """Build one dead-letter record."""
    return {
        "dlq_id": str(uuid.uuid4()),
        "stage": stage,                      # ingest | stream | db
        "reason_code": reason_code,          # e.g. MISSING_REQUIRED, TYPE_ERROR, RANGE_ERROR
        "reason_detail": reason_detail,
        "record_uid": record_uid,
        "ingest_run_id": ingest_run_id,
        "raw_payload": raw_payload,
        "rejected_at": datetime.now(timezone.utc).isoformat(),
    }


class DeadLetterWriter:
    """Write dead-letter records to one JSONL file per (stage, run).

    A run id names ONE execution, so an old file with the same run id is replaced
    at the start (otherwise a re-run would list every rejection twice).
    """

    def __init__(self, base_dir: Path, stage: str, run_id: str):
        self.path = Path(base_dir) / stage / f"{run_id}.jsonl"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.unlink(missing_ok=True)
        self.count = 0

    def write(self, record: dict) -> None:
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
        self.count += 1
