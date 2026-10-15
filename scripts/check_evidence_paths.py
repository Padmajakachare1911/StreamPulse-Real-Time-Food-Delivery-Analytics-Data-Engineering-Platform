"""Check that every evidence file cited in docs/task_tracker.md and docs/evidence_register.md really exists,
and that every VERIFIED_WITH_EVIDENCE row cites at least one path (rule: never VERIFIED without an evidence file).

  python scripts/check_evidence_paths.py     exit 0 = all cited paths exist
"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = [ROOT / "docs" / "task_tracker.md", ROOT / "docs" / "evidence_register.md"]
BACKTICK = re.compile(r"`([^`]+)`")
PATHLIKE = re.compile(r"[\w./\\-]+\.(txt|json|png|md|py|ps1|sql|yaml|yml|csv|log|jsonl)$|/$")


def candidates(cell: str) -> list[str]:
    return [m for m in BACKTICK.findall(cell) if "/" in m or "\\" in m]


def resolve(path_text: str, base_hint: str | None) -> list[Path]:
    """A cited path may be a file, a folder, a glob (*), or a sibling name written relative to the previous path."""
    text = path_text.replace("\\", "/").strip()
    if text.startswith("docs/") or text.startswith("scripts/") or text.startswith("tests/") or text.startswith("src/"):
        full = ROOT / text
    elif base_hint:
        full = ROOT / base_hint / text
    else:
        full = ROOT / text
    if "*" in text:
        return list(full.parent.glob(full.name))
    return [full] if full.exists() else []


def main() -> int:
    missing, unverified, checked = [], [], 0
    for doc in DOCS:
        for line in doc.read_text(encoding="utf-8").splitlines():
            if not line.startswith("|") or "VERIFIED_WITH_EVIDENCE" not in line:
                continue
            cells = [c.strip() for c in line.strip("|").split("|")]
            paths = [p for cell in cells for p in candidates(cell)]
            if not paths:
                unverified.append(f"{doc.name}: {cells[0]} is VERIFIED_WITH_EVIDENCE but cites no path")
            hint = None
            for p in paths:
                checked += 1
                if resolve(p, hint) == []:
                    missing.append(f"{doc.name}: {cells[0]} cites '{p}' which does not exist")
                elif p.replace("\\", "/").startswith("docs/"):
                    hint = str(Path(p.replace("\\", "/")).parent).replace("\\", "/")
    print(f"checked {checked} cited paths in {len(DOCS)} documents")
    for m in missing + unverified:
        print("PROBLEM:", m)
    print("RESULT: " + ("every cited evidence path exists" if not (missing or unverified) else f"{len(missing) + len(unverified)} problem(s)"))
    return 0 if not (missing or unverified) else 1


if __name__ == "__main__":
    sys.exit(main())
