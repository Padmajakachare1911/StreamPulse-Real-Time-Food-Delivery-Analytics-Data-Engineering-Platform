"""PostgreSQL connection helper and schema initialisation."""
import logging
from pathlib import Path

import psycopg2

from src.common.config import (POSTGRES_DB, POSTGRES_HOST, POSTGRES_PASSWORD, POSTGRES_PORT, POSTGRES_USER,
                               PROJECT_ROOT)

log = logging.getLogger(__name__)
SCHEMA_FILE = PROJECT_ROOT / "sql" / "schema.sql"
ANALYTICS_FILE = PROJECT_ROOT / "sql" / "analytics.sql"


def get_connection(connect_timeout: int = 5, dbname: str | None = None):
    """Open a new connection (to POSTGRES_DB unless `dbname` is given, e.g. an isolated test database).
    Raises a clear error when the password is not configured."""
    if not POSTGRES_PASSWORD:
        raise RuntimeError("POSTGRES_PASSWORD is not set. Copy .env.example to .env and set it.")
    return psycopg2.connect(host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=dbname or POSTGRES_DB, user=POSTGRES_USER,
                            password=POSTGRES_PASSWORD, connect_timeout=connect_timeout)


def init_schema(schema_file: Path = SCHEMA_FILE, analytics_file: Path | None = ANALYTICS_FILE) -> None:
    """Create schemas, tables and analytics views. Repeatable: IF NOT EXISTS / CREATE OR REPLACE."""
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(schema_file.read_text(encoding="utf-8"))
        log.info("Schema applied from %s", schema_file.name)
        if analytics_file:
            cur.execute(analytics_file.read_text(encoding="utf-8"))
            log.info("Views applied from %s", analytics_file.name)


def ping() -> tuple[bool, str]:
    """Real connectivity check used by the dashboard: (ok, message)."""
    try:
        with get_connection(connect_timeout=3) as conn, conn.cursor() as cur:
            cur.execute("SELECT 1")
            return True, "connected"
    except Exception as exc:  # report the real reason instead of hiding it
        return False, f"{type(exc).__name__}: {str(exc).splitlines()[0] if str(exc) else ''}"
