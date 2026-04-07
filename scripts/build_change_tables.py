from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


def normalize_date(raw: str) -> str:
    value = str(raw).strip()
    if len(value) == 8 and value.isdigit():
        return f"{value[:4]}-{value[4:6]}-{value[6:]}"
    return value


def price_file_has_values(path: Path) -> bool:
    if not path.exists():
        return False
    try:
        df = pd.read_csv(path, encoding="utf-8-sig")
    except Exception:
        return False
    if df.empty or "收盤價" not in df.columns:
        return False
    return pd.to_numeric(df["收盤價"], errors="coerce").notna().any()


def main() -> None:
    parser = argparse.ArgumentParser(description="依指定日期重建 change_table 報表")
    parser.add_argument("--date", action="append", default=[], help="指定單一日期，可重複帶入")
    parser.add_argument("--through-date", default="", help="只處理 <= 此日期的 data/*.csv")
    parser.add_argument("--require-price", action="store_true", help="只重建已有價格檔且至少一筆有效價格的日期")
    args = parser.parse_args()

    data_dir = ROOT / "data"
    if args.date:
        target_dates = sorted({normalize_date(value) for value in args.date})
    else:
        target_dates = sorted(
            path.stem
            for path in data_dir.glob("*.csv")
            if not path.stem.endswith("_with_price")
        )

    through_date = normalize_date(args.through_date) if args.through_date else ""
    if through_date:
        target_dates = [date_str for date_str in target_dates if date_str <= through_date]

    if args.require_price:
        target_dates = [
            date_str
            for date_str in target_dates
            if price_file_has_values(ROOT / "prices" / f"{date_str}.csv")
        ]

    if not target_dates:
        print("[reports] no target dates to build")
        return

    for date_str in target_dates:
        env = os.environ.copy()
        env["REPORT_DATE"] = date_str
        subprocess.run(
            [sys.executable, str(ROOT / "build_change_table.py")],
            cwd=str(ROOT),
            env=env,
            check=True,
        )
        print(f"[reports] built change_table_{date_str}.csv")


if __name__ == "__main__":
    main()
