from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys
import time

import pandas as pd
import shioaji as sj
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def normalize_date(raw: str) -> str:
    value = str(raw).strip()
    if len(value) == 8 and value.isdigit():
        return f"{value[:4]}-{value[4:6]}-{value[6:]}"
    return value


def today_str() -> str:
    return pd.Timestamp.now(tz="Asia/Taipei").strftime("%Y-%m-%d")


def load_holdings_frame(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, encoding="utf-8-sig", dtype=str)
    rename = {}
    for column in df.columns:
        label = str(column).replace("\ufeff", "").strip()
        if any(key in label for key in ["股票代號", "證券代號", "代號", "代碼"]):
            rename[column] = "股票代號"
        elif any(key in label for key in ["股票名稱", "名稱"]):
            rename[column] = "股票名稱"
        elif any(key in label for key in ["持股權重", "投資比例", "比重", "權重"]):
            rename[column] = "持股權重"
        elif any(key in label for key in ["股數", "持有股數"]):
            rename[column] = "股數"
    if rename:
        df = df.rename(columns=rename)

    for required in ["股票代號", "股票名稱", "股數", "持股權重"]:
        if required not in df.columns:
            df[required] = "" if required in {"股票代號", "股票名稱"} else 0

    df["股票代號"] = df["股票代號"].astype(str).str.extract(r"([1-9]\d{3})", expand=False)
    df = df.dropna(subset=["股票代號"]).drop_duplicates("股票代號")
    df["股票名稱"] = df["股票名稱"].astype(str).str.strip()
    df["股數"] = pd.to_numeric(df["股數"], errors="coerce").fillna(0).astype(int)
    df["持股權重"] = pd.to_numeric(df["持股權重"], errors="coerce").fillna(0.0)
    return df[["股票代號", "股票名稱", "股數", "持股權重"]].sort_values("股票代號").reset_index(drop=True)


def find_prev_snapshot_date(snapshot_dir: Path, report_date: str) -> str | None:
    snapshot_dates = sorted(path.stem for path in snapshot_dir.glob("*.csv"))
    for candidate in reversed(snapshot_dates):
        if candidate < report_date:
            return candidate
    return None


def load_required_codes(data_dir: Path, snapshot_dir: Path, date_str: str) -> pd.DataFrame:
    current_path = data_dir / f"{date_str}.csv"
    current_df = load_holdings_frame(current_path)

    prev_snapshot_date = find_prev_snapshot_date(snapshot_dir, date_str)
    if not prev_snapshot_date:
        return current_df

    prev_snapshot_path = snapshot_dir / f"{prev_snapshot_date}.csv"
    if not prev_snapshot_path.exists():
        return current_df

    prev_df = load_holdings_frame(prev_snapshot_path)
    union_df = pd.concat([current_df, prev_df], ignore_index=True)
    union_df = union_df.sort_values(["股票代號", "股數"], ascending=[True, False])
    union_df = union_df.drop_duplicates("股票代號", keep="first").reset_index(drop=True)
    return union_df


def resolve_target_dates(data_dir: Path, requested_dates: list[str], through_date: str) -> list[str]:
    if requested_dates:
        return sorted({normalize_date(value) for value in requested_dates if normalize_date(value) <= through_date})

    candidates = []
    for path in data_dir.glob("*.csv"):
        if path.stem.endswith("_with_price"):
            continue
        date_str = normalize_date(path.stem)
        if date_str <= through_date:
            candidates.append(date_str)
    return sorted(set(candidates))


def price_file_complete(price_path: Path, holdings_df: pd.DataFrame) -> bool:
    if not price_path.exists():
        return False
    df = pd.read_csv(price_path, encoding="utf-8-sig", dtype=str)
    if df.empty:
        return False
    columns = [str(column).replace("\ufeff", "").strip() for column in df.columns]
    df.columns = columns
    if "股票代號" not in df.columns or "收盤價" not in df.columns:
        return False
    df["股票代號"] = df["股票代號"].astype(str).str.extract(r"([1-9]\d{3})", expand=False)
    df["收盤價"] = pd.to_numeric(df["收盤價"], errors="coerce")
    merged = holdings_df[["股票代號"]].merge(df[["股票代號", "收盤價"]], on="股票代號", how="left")
    return merged["收盤價"].notna().all()


def resolve_contract(api: sj.Shioaji, code: str):
    try:
        return api.Contracts.Stocks[code]
    except Exception:
        pass

    for market_name in ("TSE", "OTC", "OES"):
        market = getattr(api.Contracts.Stocks, market_name, None)
        if market is None:
            continue
        try:
            return market[code]
        except Exception:
            continue
    return None


def fetch_daily_closes(api: sj.Shioaji, code: str, start_date: str, end_date: str) -> dict[str, float]:
    contract = resolve_contract(api, code)
    if contract is None:
        return {}

    end_exclusive = (pd.Timestamp(end_date) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    kbars = api.kbars(contract=contract, start=start_date, end=end_exclusive)
    if not kbars:
        return {}

    frame = pd.DataFrame({**kbars})
    if frame.empty or "ts" not in frame.columns or "Close" not in frame.columns:
        return {}

    frame["ts"] = pd.to_datetime(frame["ts"], unit="ns", utc=True).dt.tz_convert("Asia/Taipei")
    frame["trade_date"] = frame["ts"].dt.strftime("%Y-%m-%d")
    frame["Close"] = pd.to_numeric(frame["Close"], errors="coerce")
    frame = frame.dropna(subset=["Close"])
    return frame.groupby("trade_date", sort=True)["Close"].last().to_dict()


def login_shioaji() -> sj.Shioaji:
    load_dotenv(ROOT / ".env")
    api_key = os.getenv("SHIOAJI_API_KEY")
    secret_key = os.getenv("SHIOAJI_SECRET_KEY")
    if not api_key or not secret_key:
        raise SystemExit("缺少 SHIOAJI_API_KEY / SHIOAJI_SECRET_KEY，無法透過 Shioaji 補齊股價")

    simulation_raw = os.getenv("SHIOAJI_SIMULATION", "true").strip().lower()
    simulation = simulation_raw not in {"0", "false", "no"}
    api = sj.Shioaji(simulation=simulation)
    api.login(api_key=api_key, secret_key=secret_key)
    return api


def main() -> None:
    parser = argparse.ArgumentParser(description="透過 Shioaji API 補齊每日持股收盤價")
    parser.add_argument("--date", action="append", default=[], help="指定單一日期，可重複帶入")
    parser.add_argument("--through-date", default=today_str(), help="只處理 <= 此日期的 data/*.csv")
    parser.add_argument("--skip-complete", action="store_true", help="已完整補價的日期直接跳過")
    parser.add_argument("--strict", action="store_true", help="若有任一日期缺價則回傳失敗")
    parser.add_argument("--write-with-price", action="store_true", help="同步輸出 data/*_with_price.csv")
    parser.add_argument("--sleep-seconds", type=float, default=0.2, help="每支股票查詢後等待秒數")
    args = parser.parse_args()

    data_dir = ROOT / "data"
    snapshot_dir = ROOT / "data_snapshots"
    prices_dir = ROOT / "prices"
    prices_dir.mkdir(parents=True, exist_ok=True)

    target_dates = resolve_target_dates(data_dir, args.date, normalize_date(args.through_date))
    if not target_dates:
        print("[prices] no eligible dates to fill")
        return

    holdings_by_date: dict[str, pd.DataFrame] = {}
    missing_dates = []
    start_date = target_dates[0]
    end_date = target_dates[-1]

    for date_str in target_dates:
        path = data_dir / f"{date_str}.csv"
        if not path.exists():
            missing_dates.append(date_str)
            continue
        holdings_df = load_required_codes(data_dir, snapshot_dir, date_str)
        price_path = prices_dir / f"{date_str}.csv"
        if args.skip_complete and price_file_complete(price_path, holdings_df):
            continue
        holdings_by_date[date_str] = holdings_df

    if not holdings_by_date:
        print("[prices] all target dates already complete")
        return

    unique_codes = sorted({code for df in holdings_by_date.values() for code in df["股票代號"].tolist()})
    api = login_shioaji()
    prices_by_code: dict[str, dict[str, float]] = {}
    unresolved_codes = []

    for index, code in enumerate(unique_codes, start=1):
        try:
            prices_by_code[code] = fetch_daily_closes(api, code, start_date, end_date)
        except Exception as exc:
            print(f"[prices] failed to fetch {code}: {exc}")
            prices_by_code[code] = {}
        if not prices_by_code[code]:
            unresolved_codes.append(code)
        print(f"[prices] fetched {code} ({index}/{len(unique_codes)})")
        time.sleep(args.sleep_seconds)

    try:
        api.logout()
    except Exception:
        pass

    missing_records: list[str] = []
    for date_str, holdings_df in sorted(holdings_by_date.items()):
        priced_df = holdings_df.copy()
        priced_df["收盤價"] = priced_df["股票代號"].map(
            lambda code: prices_by_code.get(code, {}).get(date_str)
        )

        price_df = priced_df[["股票代號", "收盤價"]].copy()
        price_df.to_csv(prices_dir / f"{date_str}.csv", index=False, encoding="utf-8-sig")

        if args.write_with_price:
            priced_df.to_csv(data_dir / f"{date_str}_with_price.csv", index=False, encoding="utf-8-sig")

        missing_codes = priced_df.loc[priced_df["收盤價"].isna(), "股票代號"].tolist()
        if missing_codes:
            missing_records.append(f"{date_str}: {', '.join(missing_codes)}")
            print(f"[prices] missing {date_str}: {', '.join(missing_codes)}")
        else:
            print(f"[prices] completed {date_str} ({len(priced_df)} codes)")

    if missing_dates:
        missing_records.extend(f"{date_str}: missing holdings CSV" for date_str in missing_dates)

    if args.strict and missing_records:
        raise SystemExit(
            "以下日期仍有缺價或缺資料，已停止：\n" + "\n".join(missing_records)
        )

    if unresolved_codes:
        print(f"[prices] unresolved contracts: {', '.join(unresolved_codes)}")


if __name__ == "__main__":
    main()
