"""Scan every file that Git tracks or could commit (not ignored) for secrets.

  python scripts/secret_scan.py          exit 0 = clean, exit 1 = something found (file and rule are printed, never the secret)

Two checks:
  1. the real secret VALUES from the local .env (any key containing PASSWORD, SECRET, TOKEN or KEY) must not appear anywhere
  2. generic patterns: private keys, "password = <literal>", AWS-style and API-key-looking strings
It also fails if .env itself is tracked or not ignored.
"""
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SECRET_KEY_WORDS = ("PASSWORD", "SECRET", "TOKEN", "KEY")
PATTERNS = {
    "private key block": re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
    "AWS access key": re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    "GitHub token": re.compile(r"\bgh[pousr]_[A-Za-z0-9]{30,}\b"),
    "literal password assignment": re.compile(r"(?i)\b(password|passwd|pwd)\b\s*[:=]\s*['\"][^'\"\s]{6,}['\"]"),
}
SKIP_SUFFIXES = {".png", ".jpg", ".parquet", ".pyc", ".zip", ".gz"}


def env_secret_values() -> dict[str, str]:
    env = ROOT / ".env"
    values = {}
    if env.exists():
        for line in env.read_text(encoding="utf-8-sig").splitlines():
            if "=" in line and not line.lstrip().startswith("#"):
                key, value = line.split("=", 1)
                value = value.strip()
                if any(word in key.upper() for word in SECRET_KEY_WORDS) and len(value) >= 6 and value.lower() not in ("change_me", "change-me-in-env"):
                    values[key.strip()] = value
    return values


def candidate_files() -> list[Path]:
    out = subprocess.run(["git", "ls-files", "--cached", "--others", "--exclude-standard"], cwd=ROOT, capture_output=True, text=True, check=True)
    return [ROOT / name for name in out.stdout.splitlines() if (ROOT / name).is_file()]


def main() -> int:
    problems = []
    tracked = subprocess.run(["git", "ls-files", ".env"], cwd=ROOT, capture_output=True, text=True).stdout.strip()
    if tracked:
        problems.append(".env is TRACKED by git")
    ignored = subprocess.run(["git", "check-ignore", "-q", ".env"], cwd=ROOT).returncode == 0
    if (ROOT / ".env").exists() and not ignored:
        problems.append(".env exists but is NOT ignored")

    secrets = env_secret_values()
    files = candidate_files()
    for path in files:
        if path.suffix.lower() in SKIP_SUFFIXES or path.stat().st_size > 20_000_000:
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        rel = path.relative_to(ROOT)
        for key, value in secrets.items():
            if value in text:
                problems.append(f"{rel}: contains the value of {key} from .env")
        for name, pattern in PATTERNS.items():
            if pattern.search(text) and rel.name != "secret_scan.py":
                problems.append(f"{rel}: matches pattern '{name}'")
    print(f"scanned {len(files)} files that git tracks or could commit; checked {len(secrets)} secret values from .env")
    for problem in problems:
        print("FOUND:", problem)
    print("RESULT: " + ("clean" if not problems else f"{len(problems)} problem(s)"))
    return 0 if not problems else 1


if __name__ == "__main__":
    sys.exit(main())
