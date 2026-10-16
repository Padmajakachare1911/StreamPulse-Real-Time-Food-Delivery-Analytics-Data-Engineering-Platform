"""Follow one order through Kafka -> Spark -> Parquet -> PostgreSQL from the terminal (same checks as the dashboard page).

  python scripts/trace_order.py demo-0123abc...                 # one look
  python scripts/trace_order.py demo-0123abc... --wait 60       # poll every 3 s until the order is stored (bounded)
  python scripts/trace_order.py --list                          # the demo orders submitted so far

Exit code 0 only when the order was found in all four stages. Every stage is a real read of the real layer.
"""
import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.demo import order_trace  # noqa: E402
from src.demo.submissions import read_submissions  # noqa: E402

MARK = {"done": "[DONE]   ", "waiting": "[WAITING]", "unavailable": "[UNAVAIL]"}


def show(result: dict, verbose: bool) -> None:
    for stage in result["stages"]:
        print(f"{MARK[stage.state]} {stage.name}: {stage.detail}")
        if verbose and stage.facts:
            print("          " + json.dumps(stage.facts, default=str)[:400])
    seconds = result["latency_seconds"]
    print(f"STATE: {result['state']}" + (f"   submit -> PostgreSQL: {seconds} s" if seconds is not None else ""))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("order_id", nargs="?")
    parser.add_argument("--wait", type=int, default=0, help="seconds to keep polling until the order is stored")
    parser.add_argument("--list", action="store_true", help="list submitted demo orders")
    parser.add_argument("--verbose", action="store_true", help="print the facts found in each stage")
    args = parser.parse_args()
    if args.list:
        for record in read_submissions():
            print(f"{record['submitted_at'][:19]}  {record['kind']:6}  {record['order_id']}  kafka {record['partition']}/{record['offset']}")
        return 0
    if not args.order_id:
        parser.error("give an order id (or --list)")
    deadline = time.time() + args.wait
    while True:
        result = order_trace.trace_order(args.order_id)
        if result["state"] == "complete" or time.time() >= deadline:
            break
        time.sleep(3)
    show(result, args.verbose)
    return 0 if result["state"] == "complete" else 1


if __name__ == "__main__":
    sys.exit(main())
