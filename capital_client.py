from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
from playwright.sync_api import Page

INFO_URL = "https://www.capitalfund.com.tw/etf/product/detail/500/basic"
API_URL = "https://www.capitalfund.com.tw/CFWeb/api/etf/buyback"
FUND_ID = "500"

ARCHIVE_DIR = Path("archive")
DATA_DIR = Path("data")
SNAPSHOT_DIR = Path("data_snapshots")
MANIFEST_DIR = Path("manifest")


@dataclass
class HoldingsSnapshot:
    requested_date: str | None
    effective_date: str
    frame: pd.DataFrame
    payload: dict


def normalize_date(raw: str | None) -> str | None:
    if raw is None:
        return None
    value = str(raw).strip()
    if not value:
        return None
    if len(value) == 8 and value.isdigit():
        return f"{value[:4]}-{value[4:6]}-{value[6:]}"
    if len(value) >= 10:
        return value[:10]
    return value


def today_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def iter_business_dates(start_date: str, end_date: str):
    cursor = date.fromisoformat(start_date)
    stop = date.fromisoformat(end_date)
    while cursor <= stop:
        if cursor.weekday() < 5:
            yield cursor.strftime("%Y-%m-%d")
        cursor += timedelta(days=1)


def open_holdings_page(page: Page) -> None:
    page.goto(INFO_URL, wait_until="networkidle")


def fetch_payload(page: Page, requested_date: str | None) -> dict:
    normalized = normalize_date(requested_date)
    return page.evaluate(
        """
        async ({ apiUrl, fundId, dateStr }) => {
          const res = await fetch(apiUrl, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ fundId, date: dateStr })
          });
          return await res.json();
        }
        """,
        {"apiUrl": API_URL, "fundId": FUND_ID, "dateStr": normalized},
    )


def is_valid_payload(payload: dict | None) -> bool:
    if not payload or payload.get("code") != 200:
        return False
    data = payload.get("data") or {}
    return bool(data.get("stocks"))


def parse_snapshot(payload: dict, requested_date: str | None = None) -> HoldingsSnapshot:
    if not is_valid_payload(payload):
        raise ValueError("Capital payload missing holdings data")

    data = payload.get("data") or {}
    pcf = data.get("pcf") or {}
    effective_date = normalize_date(pcf.get("date1")) or normalize_date(requested_date)
    if not effective_date:
        raise ValueError("Unable to resolve effective date from Capital payload")

    records = []
    for stock in data.get("stocks") or []:
        records.append(
            {
                "股票代號": str(stock.get("stocNo", "")).strip(),
                "股票名稱": str(stock.get("stocName", "")).strip(),
                "股數": int(float(stock.get("share") or 0)),
                "持股權重": float(stock.get("weight") or 0.0),
            }
        )

    frame = pd.DataFrame(records)
    if frame.empty:
        raise ValueError(f"Capital payload for {effective_date} returned no holdings rows")

    frame = frame.dropna(subset=["股票代號"]).drop_duplicates("股票代號")
    frame["股票代號"] = frame["股票代號"].astype(str).str.extract(r"([1-9]\d{3})", expand=False)
    frame = frame.dropna(subset=["股票代號"]).sort_values("股票代號").reset_index(drop=True)
    return HoldingsSnapshot(
        requested_date=normalize_date(requested_date),
        effective_date=effective_date,
        frame=frame,
        payload=payload,
    )


def fetch_snapshot(page: Page, requested_date: str | None, fallback_latest: bool = False) -> HoldingsSnapshot:
    payload = fetch_payload(page, requested_date)
    if is_valid_payload(payload):
        return parse_snapshot(payload, requested_date=requested_date)
    if fallback_latest:
        latest_payload = fetch_payload(page, None)
        return parse_snapshot(latest_payload, requested_date=None)
    raise ValueError(f"No holdings found for requested date {requested_date}")


def snapshot_exists(effective_date: str) -> bool:
    yyyymm = effective_date[:7]
    yyyymmdd = effective_date.replace("-", "")
    out_xlsx = ARCHIVE_DIR / yyyymm / f"ETF_Investment_Portfolio_{yyyymmdd}.xlsx"
    out_csv = DATA_DIR / f"{effective_date}.csv"
    return out_xlsx.exists() and out_csv.exists()


def write_snapshot_artifacts(
    snapshot: HoldingsSnapshot,
    *,
    write_manifest: bool,
    write_daily_csv: bool,
    write_snapshot_csv: bool,
) -> dict[str, Path]:
    effective_date = snapshot.effective_date
    yyyymm = effective_date[:7]
    yyyymmdd = effective_date.replace("-", "")

    outdir = ARCHIVE_DIR / yyyymm
    outdir.mkdir(parents=True, exist_ok=True)

    holdings_xlsx = outdir / f"ETF_Investment_Portfolio_{yyyymmdd}.xlsx"
    with pd.ExcelWriter(holdings_xlsx, engine="openpyxl") as writer:
        snapshot.frame.to_excel(writer, sheet_name="holdings", index=False)
        with_prices = snapshot.frame.copy()
        with_prices["收盤價"] = pd.NA
        with_prices.to_excel(writer, sheet_name="with_prices", index=False)

    outputs: dict[str, Path] = {"xlsx": holdings_xlsx}

    if write_daily_csv:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        daily_csv = DATA_DIR / f"{effective_date}.csv"
        snapshot.frame.to_csv(daily_csv, index=False, encoding="utf-8-sig")
        outputs["data_csv"] = daily_csv

    if write_snapshot_csv:
        SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
        snapshot_csv = SNAPSHOT_DIR / f"{effective_date}.csv"
        snapshot.frame.to_csv(snapshot_csv, index=False, encoding="utf-8-sig")
        outputs["snapshot_csv"] = snapshot_csv

    if write_manifest:
        MANIFEST_DIR.mkdir(parents=True, exist_ok=True)
        manifest_path = MANIFEST_DIR / "effective_date.txt"
        manifest_path.write_text(effective_date, encoding="utf-8")
        outputs["manifest"] = manifest_path

    return outputs
