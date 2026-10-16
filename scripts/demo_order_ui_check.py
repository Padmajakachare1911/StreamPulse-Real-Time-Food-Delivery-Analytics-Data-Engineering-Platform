"""Drive the REAL Streamlit page "Place a demo order" in a headless browser, like a user would, and save screenshots.

  python scripts/demo_order_ui_check.py --url http://localhost:8501 --out docs/evidence/12_demo_orders --prefix ui

Steps: open the page, fill the form, press "Place demo order", wait (bounded) until the page reports that the same order was
found in Kafka, Spark/Parquet and PostgreSQL. It prints the order id and the page's own stage lines, and exits 1 on a timeout.
The dashboard and the Spark stream must already be running. Nothing is drawn or edited: the PNGs are what the browser rendered.
"""
import argparse
import re
import sys
from pathlib import Path

from playwright.sync_api import sync_playwright


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--url", default="http://localhost:8501")
    parser.add_argument("--out", default="docs/evidence/12_demo_orders")
    parser.add_argument("--prefix", default="ui")
    parser.add_argument("--timeout", type=int, default=120, help="seconds to wait for the order to reach PostgreSQL")
    parser.add_argument("--city", default="curitiba")
    parser.add_argument("--zip", default="80010")
    parser.add_argument("--state", default="PR")
    parser.add_argument("--price", default="249.90")
    parser.add_argument("--trace-only", default=None, metavar="ORDER_ID", help="do not place an order: only trace this existing order and take a screenshot")
    args = parser.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1500, "height": 1500})
        page.goto(args.url, wait_until="networkidle")
        page.wait_for_selector("text=StreamPulse", timeout=30000)
        page.get_by_text("Place a demo order", exact=True).first.click()
        page.wait_for_selector("text=Place a demo order and trace it", timeout=30000)

        if args.trace_only:
            box = page.get_by_label("Order id to trace")
            box.fill(args.trace_only)
            box.press("Enter")
            try:
                page.wait_for_selector("text=Complete: the same order was found", timeout=args.timeout * 1000)
                done = True
            except Exception:
                done = False
            page.wait_for_timeout(1500)
            page.screenshot(path=str(out / f"{args.prefix}_trace.png"))
            for line in page.inner_text("body").splitlines():
                if re.search(r"[1-4] (Submitted|Kafka|Spark and Parquet|PostgreSQL):", line) or line.startswith("Complete:"):
                    print("PAGE STAGE:", line.strip())
            print("RESULT:", "complete" if done else "NOT COMPLETE")
            browser.close()
            return 0 if done else 1

        page.get_by_label("Customer city").fill(args.city)
        page.get_by_label("Zip code prefix (5 digits)").fill(args.zip)
        page.get_by_label("Items price total (BRL)").fill(args.price)
        page.get_by_label("Customer state").click()
        page.keyboard.type(args.state)
        page.keyboard.press("Enter")
        page.get_by_label("Order status").click()
        page.keyboard.type("approved")
        page.keyboard.press("Enter")
        page.wait_for_timeout(500)
        page.screenshot(path=str(out / f"{args.prefix}_01_form_filled.png"))

        page.get_by_role("button", name="Place demo order").click()
        page.wait_for_selector("text=Published to Kafka", timeout=60000)
        message = page.get_by_text(re.compile(r"Published to Kafka")).first.inner_text()
        order_id = re.search(r"Order id: (demo-[0-9a-f]+)", message).group(1)
        print("PAGE SAYS:", message.replace("\n", " "))
        page.screenshot(path=str(out / f"{args.prefix}_02_submitted.png"))

        try:
            page.wait_for_selector("text=Complete: the same order was found", timeout=args.timeout * 1000)
            done = True
        except Exception:
            done = False
        page.wait_for_timeout(1500)
        page.screenshot(path=str(out / f"{args.prefix}_03_trace_complete.png" if done else out / f"{args.prefix}_03_trace_TIMEOUT.png"))
        body = page.inner_text("body")
        for line in body.splitlines():
            if re.search(r"[1-4] (Submitted|Kafka|Spark and Parquet|PostgreSQL):", line) or line.startswith("Complete:"):
                print("PAGE STAGE:", line.strip())
        print("ORDER_ID", order_id)
        print("RESULT:", "complete" if done else "TIMEOUT: the order did not reach PostgreSQL in time")
        browser.close()
    return 0 if done else 1


if __name__ == "__main__":
    sys.exit(main())
