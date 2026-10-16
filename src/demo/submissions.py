"""The log of user-submitted demo orders (one JSON line per Kafka message published).

It is the single record of what the dashboard form sent, so a demo order can be traced and so
scripts/reconcile.py can account for these messages separately from the historical Olist replay.
The file lives under data/processed/ (git-ignored, like every other run output).
"""
import json
from pathlib import Path

from src.common.config import PROJECT_ROOT

DEMO_DIR = PROJECT_ROOT / "data" / "processed" / "demo_orders"
SUBMISSIONS_FILE = DEMO_DIR / "submissions.jsonl"


def append_submission(record: dict, path: Path = SUBMISSIONS_FILE) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def read_submissions(topic: str | None = None, path: Path = SUBMISSIONS_FILE) -> list[dict]:
    """All logged messages, oldest first; optionally only those published to one topic."""
    if not Path(path).exists():
        return []
    records = [json.loads(line) for line in Path(path).read_text(encoding="utf-8").splitlines() if line.strip()]
    return [r for r in records if topic is None or r["topic"] == topic]


def demo_message_counts(topic: str, path: Path = SUBMISSIONS_FILE) -> dict:
    """{'messages': published messages (resends included), 'orders': distinct order_id, 'record_uids': distinct record_uid}."""
    records = read_submissions(topic, path)
    return {"messages": len(records), "orders": len({r["order_id"] for r in records}),
            "record_uids": len({r["record_uid"] for r in records})}
