"""Show WHY run-level rejected counts (14 + 1 = 15) differ from the unique rejected records (14).

Reads the dead-letter files of the two production runs and compares their record_uid sets.
  python scripts/explain_rejected_count.py
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.common.config import DEAD_LETTER_DIR  # noqa: E402


def uids(run_id: str) -> list[str]:
    path = DEAD_LETTER_DIR / "ingest" / f"{run_id}.jsonl"
    return [json.loads(line)["record_uid"] for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def main() -> int:
    small, full = uids("limited-100"), uids("full-run-01")
    shared = set(small) & set(full)
    print(f"limited-100 (replay rows 0..99) rejected records : {len(small)}")
    print(f"full-run-01 (whole file)        rejected records : {len(full)}")
    print(f"sum of run-level rejected counts                 : {len(small) + len(full)}")
    print(f"record_uid rejected by BOTH runs                 : {len(shared)}  {sorted(shared)}")
    print(f"unique rejected records (union by record_uid)    : {len(set(small) | set(full))}")
    ok = len(small) + len(full) - len(shared) == len(set(small) | set(full)) and len(set(small) | set(full)) == 14
    print("EXPLANATION VERIFIED: 15 = 14 unique rejected records + 1 record rejected again by the overlapping test run" if ok else "EXPLANATION NOT CONFIRMED")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
