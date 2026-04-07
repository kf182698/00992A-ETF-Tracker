from __future__ import annotations

import os

from playwright.sync_api import sync_playwright

from capital_client import fetch_snapshot, normalize_date, open_holdings_page, today_str, write_snapshot_artifacts


def requested_report_date() -> str:
    value = normalize_date(os.getenv("REPORT_DATE"))
    return value or today_str()


def main() -> None:
    target_date = requested_report_date()
    print(f"[fetch] requested REPORT_DATE={target_date}")

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page()
        open_holdings_page(page)
        snapshot = fetch_snapshot(page, target_date, fallback_latest=True)
        browser.close()

    outputs = write_snapshot_artifacts(
        snapshot,
        write_manifest=True,
        write_daily_csv=True,
        write_snapshot_csv=False,
    )
    print(f"[fetch] EFFECTIVE_DATE={snapshot.effective_date}")
    print(f"[fetch] saved {outputs['xlsx']} rows={len(snapshot.frame)}")
    if "data_csv" in outputs:
        print(f"[fetch] saved {outputs['data_csv']}")


if __name__ == "__main__":
    main()
