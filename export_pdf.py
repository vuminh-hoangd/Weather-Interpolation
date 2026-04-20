"""
export_pdf.py
Renders report.html (including Chart.js charts) to report.pdf using a
headless Chromium browser via Playwright.

Requirements: pip install playwright && playwright install chromium
Usage:        python export_pdf.py
"""

import os
from playwright.sync_api import sync_playwright

INPUT  = "report.html"
OUTPUT = "report.pdf"

with sync_playwright() as p:
    browser = p.chromium.launch()
    page    = browser.new_page()

    html_path = os.path.abspath(INPUT).replace("\\", "/")
    page.goto(f"file:///{html_path}")

    # Wait for Chart.js to finish rendering
    page.wait_for_timeout(2000)

    page.pdf(
        path             = OUTPUT,
        format           = "A4",
        print_background = True,
        margin           = {"top": "1.5cm", "bottom": "1.5cm",
                            "left": "1.5cm", "right": "1.5cm"},
    )

    browser.close()

print(f"PDF saved -> {OUTPUT}")
os.startfile(os.path.abspath(OUTPUT))
