from __future__ import annotations

import argparse
from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(Path(__file__).resolve().parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent))

from update_cost_basis import load_cost_basis, update_cost_basis


def has_required_prices(change_df: pd.DataFrame) -> bool:
    prices = pd.to_numeric(change_df.get("今日收盤價"), errors="coerce")
    if prices.notna().sum() == 0:
        return False

    change_sizes = pd.to_numeric(change_df.get("買賣超股數"), errors="coerce").fillna(0)
    first_buys = change_df.get("首次買進")
    if first_buys is None:
        first_buy_flags = pd.Series([False] * len(change_df))
    else:
        first_buy_flags = (
            first_buys.astype(str).str.lower().map({"true": True, "false": False}).fillna(False)
        )

    required_mask = change_sizes.ne(0) | first_buy_flags
    if not required_mask.any():
        return True
    return prices[required_mask].notna().all()


def main() -> None:
    parser = argparse.ArgumentParser(description="依全部 change_table 重新建置成本紀錄")
    parser.add_argument("--reports-dir", type=Path, default=ROOT / "reports")
    parser.add_argument("--cost-basis-path", type=Path, default=ROOT / "data" / "cost_basis.csv")
    parser.add_argument("--gains-log-path", type=Path, default=ROOT / "data" / "realized_gains_log.csv")
    parser.add_argument("--reset", action="store_true", help="重建前先刪除既有成本與已實現損益紀錄")
    args = parser.parse_args()

    report_paths = sorted(args.reports_dir.glob("change_table_*.csv"))
    if not report_paths:
        raise SystemExit("找不到 reports/change_table_*.csv，無法重建成本紀錄")

    if args.reset:
        if args.cost_basis_path.exists():
            args.cost_basis_path.unlink()
        if args.gains_log_path.exists():
            args.gains_log_path.unlink()

    args.cost_basis_path.parent.mkdir(parents=True, exist_ok=True)
    cost_df = load_cost_basis(args.cost_basis_path)
    encountered_incomplete_report = False

    for report_path in report_paths:
        report_date = report_path.stem.replace("change_table_", "")
        change_df = pd.read_csv(report_path, encoding="utf-8-sig", dtype=str)
        change_df.columns = [str(column).replace("\ufeff", "").strip() for column in change_df.columns]
        required_cols = ["股票代號", "今日股數", "買賣超股數", "首次買進", "股票名稱", "今日收盤價"]
        missing = [column for column in required_cols if column not in change_df.columns]
        if missing:
            raise SystemExit(f"{report_path.name} 缺少必要欄位: {', '.join(missing)}")
        if not has_required_prices(change_df):
            encountered_incomplete_report = True
            print(f"[cost] stopped at {report_path.name} because required close prices are incomplete")
            break
        cost_df = update_cost_basis(cost_df, change_df, report_date, args.gains_log_path)
        print(f"[cost] applied {report_path.name}")

    cost_df.to_csv(args.cost_basis_path, index=False, encoding="utf-8-sig")
    print(f"[cost] rebuilt {args.cost_basis_path}")
    if encountered_incomplete_report:
        print("[cost] rebuild stopped before the newest date; complete missing close prices and rerun to continue")


if __name__ == "__main__":
    main()
