"""Take REAL screenshots of a running web app (dashboard or Airflow UI) with a headless browser.

  python scripts/capture_screenshots.py dashboard --out docs/evidence/09_dashboard --prefix dashboard
  python scripts/capture_screenshots.py dashboard --pages "Overview and status" --prefix dashboard_services_down
  python scripts/capture_screenshots.py airflow --out docs/evidence/08_airflow --dag streampulse_batch

The app must already be running. Nothing is drawn or edited: the PNG is what the browser rendered.
"""
import argparse
import os
import re
import sys
from pathlib import Path

from playwright.sync_api import sync_playwright

PAGES = ["Overview and status", "Dataset and provenance", "Ingestion and reconciliation", "Data quality",
         "Business KPIs", "External API", "Recent records"]


def slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


def dashboard(args) -> list[Path]:
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    saved = []
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1500, "height": 2100})
        page.goto(args.url, wait_until="networkidle")
        page.wait_for_selector("text=StreamPulse", timeout=30000)
        for i, name in enumerate(args.pages or PAGES, start=1):
            page.get_by_text(name, exact=True).first.click()
            page.wait_for_timeout(2000)               # let Streamlit start the rerun after the click
            try:                                      # then wait until it has FINISHED running (no spinner), up to 2 minutes
                page.wait_for_function("() => !document.querySelector('[data-testid=\"stStatusWidget\"]')", timeout=120000)
            except Exception:
                print("warning: the page was still running after 2 minutes; capturing anyway")
            page.wait_for_timeout(args.wait * 1000)
            target = out / f"{args.prefix}_{i:02d}_{slug(name)}.png"
            page.screenshot(path=str(target))
            saved.append(target)
        browser.close()
    return saved


def airflow(args) -> list[Path]:
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    password = os.getenv("AIRFLOW_ADMIN_PASSWORD")
    if not password:
        sys.exit("Set AIRFLOW_ADMIN_PASSWORD in the environment (value is in .env; it is never printed).")
    saved = []
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1500, "height": 1000})
        page.goto(f"{args.url}/login/", wait_until="networkidle")
        page.fill("input[name=username]", "admin")
        page.fill("input[name=password]", password)
        page.click("input[type=submit], button[type=submit]")
        page.wait_for_load_state("networkidle")
        for label, path in (("dags_list", "/home"), ("grid", f"/dags/{args.dag}/grid"), ("graph", f"/dags/{args.dag}/graph")):
            page.goto(f"{args.url}{path}", wait_until="networkidle")
            page.wait_for_timeout(3000)
            target = out / f"airflow_{args.dag}_{label}.png"
            page.screenshot(path=str(target))
            saved.append(target)
        browser.close()
    return saved


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("app", choices=["dashboard", "airflow"])
    parser.add_argument("--url", default=None)
    parser.add_argument("--out", default="docs/evidence/09_dashboard")
    parser.add_argument("--prefix", default="dashboard")
    parser.add_argument("--pages", nargs="*", help="dashboard pages to capture (default: all)")
    parser.add_argument("--dag", default="streampulse_batch")
    parser.add_argument("--wait", type=int, default=6, help="seconds to wait after opening a dashboard page")
    args = parser.parse_args()
    args.url = args.url or ("http://localhost:8501" if args.app == "dashboard" else "http://localhost:8080")
    for path in (dashboard if args.app == "dashboard" else airflow)(args):
        print("saved", path, f"({path.stat().st_size:,} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
