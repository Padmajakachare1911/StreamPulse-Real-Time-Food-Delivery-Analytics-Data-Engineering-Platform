"""Create the PostgreSQL schemas and tables from sql/schema.sql (safe to run repeatedly)."""
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.warehouse.db import init_schema  # noqa: E402


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    init_schema()
    print("schema OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
