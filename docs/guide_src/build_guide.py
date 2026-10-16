"""Build docs/StreamPulse_Complete_Project_Guide.pdf from the chapter files in docs/guide_src/.

  python docs/guide_src/build_guide.py

Steps: join the chapter HTML files, build a clickable table of contents from the headings, render with the headless
browser (Playwright/Chromium) to a PDF with page numbers. Screenshots come from docs/evidence/09_dashboard.
"""
import re
import sys
from pathlib import Path

from playwright.sync_api import sync_playwright

HERE = Path(__file__).resolve().parent
DOCS = HERE.parent
OUT_PDF = DOCS / "StreamPulse_Complete_Project_Guide.pdf"
OUT_HTML = HERE / "guide.html"


def slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")[:60]


def add_ids_and_toc(body: str) -> tuple[str, str]:
    """Give every h1/h2 an id and return (body, toc_html)."""
    entries = []
    counter = {"n": 0}

    def repl(match):
        level, attrs, title = match.group(1), match.group(2), match.group(3)
        counter["n"] += 1
        anchor = f"s{counter['n']}-{slug(re.sub('<.*?>', '', title))}"
        plain = re.sub("<.*?>", "", title)
        if plain not in ("StreamPulse", "Contents"):
            entries.append((int(level), anchor, plain))
        return f'<h{level}{attrs} id="{anchor}">{title}</h{level}>'

    body = re.sub(r"<h([12])([^>]*)>(.*?)</h\1>", repl, body, flags=re.S)
    toc, open_sub = ["<div class='toc'><ol>"], False
    for level, anchor, title in entries:
        if level == 1:
            if open_sub:
                toc.append("</ol></li>")
                open_sub = False
            toc.append(f"<li><b><a href='#{anchor}'>{title}</a></b>")
            toc.append("<ol>")
            open_sub = True
        else:
            toc.append(f"<li><a href='#{anchor}'>{title}</a></li>")
    if open_sub:
        toc.append("</ol></li>")
    toc.append("</ol></div>")
    # close dangling <li><b>.. structure properly
    html = "".join(toc).replace("<ol></ol>", "")
    return body, html


def main() -> int:
    parts = sorted(HERE.glob("part*.html"))
    if not parts:
        print("no chapter files found")
        return 1
    body = "\n".join(p.read_text(encoding="utf-8") for p in parts)
    body, toc = add_ids_and_toc(body)
    body = body.replace("<!--TOC-->", toc)
    css = (HERE / "style.css").read_text(encoding="utf-8")
    html = f"<!doctype html><html lang='en'><head><meta charset='utf-8'><title>StreamPulse: Complete Project Guide</title><style>{css}</style></head><body>{body}</body></html>"
    OUT_HTML.write_text(html, encoding="utf-8")

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.goto(OUT_HTML.as_uri(), wait_until="networkidle")
        page.pdf(
            path=str(OUT_PDF), format="A4", print_background=True, display_header_footer=True,
            margin={"top": "18mm", "bottom": "18mm", "left": "16mm", "right": "16mm"},
            header_template="<div style='font-size:8px;color:#7a8590;width:100%;padding:0 16mm;text-align:right'>StreamPulse: Complete Project Guide</div>",
            footer_template="<div style='font-size:8px;color:#7a8590;width:100%;padding:0 16mm;display:flex;justify-content:space-between'>"
                            "<span>Historical dataset replay through a streaming pipeline (not live orders)</span>"
                            "<span>Page <span class='pageNumber'></span> of <span class='totalPages'></span></span></div>",
        )
        browser.close()
    print(f"wrote {OUT_PDF} ({OUT_PDF.stat().st_size:,} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
