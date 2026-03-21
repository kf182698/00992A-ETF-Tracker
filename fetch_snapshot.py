import os, io, json
from pathlib import Path
from datetime import datetime
import pandas as pd
from playwright.sync_api import sync_playwright

INFO_URL = "https://www.capitalfund.com.tw/etf/product/detail/500/basic"
ARCHIVE = Path("archive")
MANIFEST_DIR = Path("manifest")

def _date_str_default() -> str:
    raw = (os.getenv("REPORT_DATE") or "").strip()
    if len(raw) >= 8: return raw
    return datetime.now().strftime("%Y-%m-%d")

def fetch_from_capital(page, date_str):
    js_code = f"""
    () => {{
        return fetch("https://www.capitalfund.com.tw/CFWeb/api/etf/buyback", {{
            method: "POST",
            headers: {{ "Content-Type": "application/json" }},
            body: JSON.stringify({{ fundId: "500", date: "{date_str}" }})
        }}).then(res => res.json());
    }}
    """
    return page.evaluate(js_code)

def fetch_snapshot():
    # 預設用 workflow 的 REPORT_DATE
    req_date = _date_str_default()
    if len(req_date) == 8: # YYYYMMDD
        req_date = f"{req_date[:4]}-{req_date[4:6]}-{req_date[6:]}"
        
    print(f"Fetching snapshot for {req_date}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(INFO_URL, wait_until="networkidle")
        
        data = fetch_from_capital(page, req_date)
        
        # fallback to latest if current date returns empty
        if not (data and data.get("code") == 200 and data.get("data") and data["data"].get("stocks")):
            print("No data, trying null (latest)...")
            js_code = """
            () => {
                return fetch("https://www.capitalfund.com.tw/CFWeb/api/etf/buyback", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ fundId: "500", date: null })
                }).then(res => res.json());
            }
            """
            data = page.evaluate(js_code)
            
        browser.close()

    if not data or data.get("code") != 200 or not data.get("data") or not data["data"].get("stocks"):
        raise SystemExit("官方頁仍無法取得有效資料（下載/API皆失敗）。")

    stocks = data["data"]["stocks"]
    real_target_date = data["data"].get("pcf", {}).get("date1", req_date)
    print(f"Actual report date from Capital: {real_target_date}")
    effective_date = real_target_date

    # 建立 DataFrame
    records = []
    for s in stocks:
        records.append({
            "股票代號": s["stocNo"].strip(),
            "股票名稱": s["stocName"].strip(),
            "股數": s["share"],
            "持股權重": s["weight"]
        })
    df = pd.DataFrame(records)

    # 存檔
    yyyymm = effective_date[:7]
    yyyymmdd = effective_date.replace("-", "")
    outdir = ARCHIVE / yyyymm
    outdir.mkdir(parents=True, exist_ok=True)
    out_xlsx = outdir / f"ETF_Investment_Portfolio_{yyyymmdd}.xlsx"

    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as w:
        df.to_excel(w, sheet_name="holdings", index=False)
        df2 = df.copy()
        df2["收盤價"] = pd.NA
        df2.to_excel(w, sheet_name="with_prices", index=False)

    MANIFEST_DIR.mkdir(exist_ok=True, parents=True)
    (MANIFEST_DIR / "effective_date.txt").write_text(effective_date, encoding="utf-8")
    print(f"[fetch] EFFECTIVE_DATE={effective_date}")
    print(f"[fetch] saved {out_xlsx} rows={len(df)} (report_date={effective_date})")

if __name__ == "__main__":
    fetch_snapshot()
