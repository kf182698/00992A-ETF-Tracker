from __future__ import annotations

import argparse
from datetime import date, timedelta
from pathlib import Path
import sys
import time

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from capital_client import fetch_snapshot, iter_business_dates, open_holdings_page, snapshot_exists, today_str, write_snapshot_artifacts


def month_start(dt: date) -> date:
    return dt.replace(day=1)


def previous_month(dt: date) -> date:
    return (dt.replace(day=1) - timedelta(days=1)).replace(day=1)


def month_end(dt: date) -> date:
    next_month = (dt.replace(day=28) + timedelta(days=4)).replace(day=1)
    return next_month - timedelta(days=1)


def last_business_day_of_month(dt: date) -> date:
    cursor = month_end(dt)
    while cursor.weekday() >= 5:
        cursor -= timedelta(days=1)
    return cursor


def discover_first_available_date(page, hard_floor: str, end_date: str) -> str:
    floor = date.fromisoformat(hard_floor)
    probe_month = month_start(date.fromisoformat(end_date))
    earliest_month_with_data: date | None = None
    data_found = False
    missing_months_after_data = 0

    while probe_month >= month_start(floor):
        probe_date = last_business_day_of_month(probe_month).strftime("%Y-%m-%d")
        try:
            fetch_snapshot(page, probe_date, fallback_latest=False)
            earliest_month_with_data = probe_month
            data_found = True
            missing_months_after_data = 0
        except Exception:
            if data_found:
                missing_months_after_data += 1
                if missing_months_after_data >= 3:
                    break
        probe_month = previous_month(probe_month)

    if earliest_month_with_data is None:
        raise SystemExit("找不到可回補的群益投信歷史投組資料")

    search_start = max(floor, earliest_month_with_data)
    search_end = month_end(earliest_month_with_data)
    for candidate in iter_business_dates(search_start.strftime("%Y-%m-%d"), search_end.strftime("%Y-%m-%d")):
        try:
            fetch_snapshot(page, candidate, fallback_latest=False)
            return candidate
        except Exception:
            continue

    raise SystemExit(f"無法在 {earliest_month_with_data.strftime('%Y-%m')} 內找到第一個有效交易日")


def main() -> None:
    parser = argparse.ArgumentParser(description="補齊群益投信 00992A 每日持股投資組合")
    parser.add_argument("--start-date", default="", help="開始日期，格式 YYYY-MM-DD；留空會自動探測可用起始日")
    parser.add_argument("--end-date", default=today_str(), help="結束日期，格式 YYYY-MM-DD")
    parser.add_argument("--hard-floor", default="2025-01-01", help="自動探測時的最早探測日期")
    parser.add_argument("--force", action="store_true", help="即使檔案已存在也重新下載覆蓋")
    parser.add_argument("--sleep-seconds", type=float, default=0.35, help="每次請求後等待秒數")
    args = parser.parse_args()

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page()
        open_holdings_page(page)

        start_date = args.start_date.strip()
        if not start_date:
            start_date = discover_first_available_date(page, args.hard_floor, args.end_date)
            print(f"[backfill] auto discovered start date: {start_date}")

        fetched = 0
        skipped_existing = 0
        unavailable = 0
        seen_effective_dates: set[str] = set()
        latest_effective_date = ""

        for candidate in iter_business_dates(start_date, args.end_date):
            try:
                snapshot = fetch_snapshot(page, candidate, fallback_latest=False)
            except Exception:
                unavailable += 1
                continue

            effective_date = snapshot.effective_date
            latest_effective_date = max(latest_effective_date, effective_date)

            if effective_date in seen_effective_dates:
                continue
            seen_effective_dates.add(effective_date)

            if not args.force and snapshot_exists(effective_date):
                skipped_existing += 1
                continue

            outputs = write_snapshot_artifacts(
                snapshot,
                write_manifest=False,
                write_daily_csv=True,
                write_snapshot_csv=True,
            )
            fetched += 1
            print(f"[backfill] saved {effective_date} -> {outputs['xlsx'].name}")
            time.sleep(args.sleep_seconds)

        browser.close()

    if latest_effective_date:
        manifest_path = ROOT / "manifest" / "effective_date.txt"
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(latest_effective_date, encoding="utf-8")

    print(
        f"[backfill] completed: fetched={fetched} skipped_existing={skipped_existing} unavailable={unavailable} latest={latest_effective_date or 'N/A'}"
    )


if __name__ == "__main__":
    main()
