"""
Capture dashboard screenshots using Playwright headless Chromium.

Strategy: scroll_into_view_if_needed() puts the heading at the top,
then we scrollBy() extra pixels so the actual chart content is centered.

Usage: python screenshots/capture.py
"""

import os
from playwright.sync_api import sync_playwright, Page

BASE_URL = "http://localhost:8501"
OUT_DIR = os.path.dirname(os.path.abspath(__file__))
VIEWPORT = {"width": 1400, "height": 900}

# Streamlit's inner scrollable container selector
def wait_for_streamlit(page: Page):
    page.wait_for_selector('[data-testid="stApp"]', timeout=20000)
    page.wait_for_timeout(7000)


def set_scroll(page: Page, px: int, wait: int = 1000):
    """Directly set stMain.scrollTop — predictable, no zoom side-effects."""
    page.evaluate(
        "(function(y) {"
        "  var m = document.querySelector('[data-testid=\"stMain\"]')"
        "       || document.querySelector('.main')"
        "       || document.scrollingElement;"
        "  if (m) m.scrollTop = y;"
        "})(" + str(px) + ")"
    )
    page.wait_for_timeout(wait)


def click_tab(page: Page, label: str, wait: int = 1800):
    page.locator(f'button[role="tab"]:has-text("{label}")').first.click()
    page.wait_for_timeout(wait)


def click_tab(page: Page, label: str, wait: int = 1500):
    page.locator(f'button[role="tab"]:has-text("{label}")').first.click()
    page.wait_for_timeout(wait)


def save(page: Page, name: str):
    path = os.path.join(OUT_DIR, f"{name}.png")
    page.screenshot(path=path, full_page=False)
    print(f"  ✓  {name}.png")


def run():
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_context(viewport=VIEWPORT).new_page()

        print(f"Opening {BASE_URL} ...")
        page.goto(BASE_URL)
        wait_for_streamlit(page)
        print("Streamlit loaded — capturing...\n")

        # Approximate stMain.scrollTop values for each section (calibrated):
        #   0    → KPI cards
        #   380  → Time series chart centered
        #   820  → Risk Distribution (donut + bar)
        #   1280 → Top 20 Anomalies table
        #   1780 → Pattern Insights tabs

        # ── 01: KPI Cards ─────────────────────────────────────────────────────
        set_scroll(page, 0, wait=800)
        save(page, "01_overview_kpis")

        # ── 02: Time Series (stacked bars by risk level) ───────────────────────
        set_scroll(page, 380, wait=1000)
        save(page, "02_time_series_risk_colorcoded")

        # ── 03: Risk Distribution (donut + stacked bar by country) ─────────────
        set_scroll(page, 820, wait=1000)
        save(page, "03_risk_distribution")

        # ── 04: Top 20 Anomalies (CRITICAL rows highlighted) ───────────────────
        set_scroll(page, 960, wait=1000)
        save(page, "04_top_anomalies_critical_highlighted")

        # ── 05: Pattern Insights → BINs tab ───────────────────────────────────
        set_scroll(page, 1780, wait=800)
        click_tab(page, "💳 BINs")
        set_scroll(page, 1980, wait=1000)
        save(page, "05_pattern_bins")

        # ── 06: IPs tab (card testing map) ────────────────────────────────────
        set_scroll(page, 1780, wait=600)
        click_tab(page, "🌐 IPs")
        set_scroll(page, 1980, wait=1000)
        save(page, "06_pattern_ips_card_testing")

        # ── 07: Horario heatmap ────────────────────────────────────────────────
        set_scroll(page, 1780, wait=600)
        click_tab(page, "🕐 Horario")
        set_scroll(page, 1980, wait=1200)
        save(page, "07_pattern_hourly_heatmap")

        # ── 08: Geo mismatches ─────────────────────────────────────────────────
        set_scroll(page, 1780, wait=600)
        click_tab(page, "🗺️ Geo")
        set_scroll(page, 1980, wait=1000)
        save(page, "08_pattern_geo_mismatches")

        # ── 00: Full page ──────────────────────────────────────────────────────
        set_scroll(page, 0, wait=800)
        page.screenshot(path=os.path.join(OUT_DIR, "00_full_page.png"), full_page=True)
        print(f"  ✓  00_full_page.png")

        browser.close()
        print(f"\nAll screenshots saved → {OUT_DIR}/")


if __name__ == "__main__":
    run()
