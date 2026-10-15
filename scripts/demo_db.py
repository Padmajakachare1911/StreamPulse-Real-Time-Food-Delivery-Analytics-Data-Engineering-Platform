"""Create or drop the isolated PostgreSQL database of a demo (Demo 3 or 4).

  python scripts/demo_db.py create streampulse_demo3
  python scripts/demo_db.py drop   streampulse_demo3

SAFETY: only databases whose name starts with "streampulse_demo" can be touched. The real warehouse
("streampulse") and Airflow's database can never be created over or dropped by this script.
"""
import argparse
import os
import subprocess
import sys
from pathlib import Path

import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.common.config import POSTGRES_HOST, POSTGRES_PASSWORD, POSTGRES_PORT, POSTGRES_USER  # noqa: E402

PREFIX = "streampulse_demo"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("action", choices=["create", "drop"])
    parser.add_argument("name")
    args = parser.parse_args()
    if not args.name.startswith(PREFIX) or not args.name.replace("_", "").isalnum():
        print(f"REFUSED: database name must start with '{PREFIX}' and contain only letters, digits and underscores")
        return 2
    admin = psycopg2.connect(host=POSTGRES_HOST, port=POSTGRES_PORT, dbname="postgres", user=POSTGRES_USER,
                             password=POSTGRES_PASSWORD, connect_timeout=5)
    admin.autocommit = True
    with admin.cursor() as cur:
        if args.action == "drop":
            cur.execute(f"DROP DATABASE IF EXISTS {args.name} WITH (FORCE)")
            print(f"dropped database {args.name} (if it existed)")
        else:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (args.name,))
            if cur.fetchone():
                print(f"database {args.name} already exists (kept)")
            else:
                cur.execute(f"CREATE DATABASE {args.name}")
                print(f"created database {args.name}")
    admin.close()
    if args.action == "create":
        out = subprocess.run([sys.executable, str(Path(__file__).with_name("init_db.py"))], capture_output=True, text=True,
                             env={**os.environ, "POSTGRES_DB": args.name})
        print(out.stdout.strip().splitlines()[-1] if out.stdout.strip() else out.stderr.strip()[-300:])
        return 0 if "schema OK" in out.stdout else 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
