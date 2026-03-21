import os
import time
import json
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

from playwright.sync_api import sync_playwright
import shioaji as sj

# Load env variables (might be in tw-limitup-broker-watch or here)
load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", "tw-limitup-broker-watch", ".env"))
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

def extract_holdings_for_date(page, date_str):
    """Call the Capital Fund API via Playwright's page.evaluate to bypass WAF."""
    js_code = f"""
    () => {{
        return fetch("https://www.capitalfund.com.tw/CFWeb/api/etf/buyback", {{
            method: "POST",
            headers: {{
                "Content-Type": "application/json"
            }},
            body: JSON.stringify({{
                fundId: "500",
                date: "{date_str}"
            }})
        }}).then(res => res.json());
    }}
    """
    try:
        response = page.evaluate(js_code)
        return response
    except Exception as e:
        print(f"Error fetching for {date_str}: {e}")
        return None

def fetch_shioaji_kbars(api, code, start_date, end_date):
    """Fetch daily close prices from Shioaji for a specific stock."""
    contract = api.Contracts.Stocks.TSE.get(code)
    if not contract:
        contract = api.Contracts.Stocks.OTC.get(code)
    if not contract:
        print(f"Contract not found for {code}")
        return {}
        
    kbars = api.kbars(
        contract=contract,
        start=start_date,
        end=end_date
    )
    if kbars and getattr(kbars, 'ts', None):
        import numpy as np
        ts_arr = np.array(kbars.ts)
        close_arr = np.array(kbars.Close)
        res = {}
        for idx, ts in enumerate(ts_arr):
            dt = datetime.fromtimestamp(ts / 1e9)
            dt_str = dt.strftime('%Y-%m-%d')
            res[dt_str] = close_arr[idx]
        return res
    return {}

def main():
    start_date = "2025-12-30"
    end_date = datetime.now().strftime("%Y-%m-%d")
    
    # 1. Start Shioaji API
    api_key = os.getenv("SHIOAJI_API_KEY")
    secret_key = os.getenv("SHIOAJI_SECRET_KEY")
    if not api_key or not secret_key:
        print("Error: Please set SHIOAJI_API_KEY and SHIOAJI_SECRET_KEY in .env")
        return
        
    print("Logging into Shioaji...")
    api = sj.Shioaji()
    api.login(api_key, secret_key)
    print("Shioaji login successful.")
    
    # 2. Start Playwright
    print("Starting Playwright...")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto("https://www.capitalfund.com.tw/etf/product/detail/500/basic", wait_until="networkidle")
        print("Page loaded, WAF bypassed.")
        
        # Iterating through business days (roughly)
        current_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        
        # We will collect all unique stocks first, to batch fetch Shioaji data
        days_data = {}
        all_unique_stocks = set()
        
        while current_date <= end_dt:
            # Skip weekends
            if current_date.weekday() >= 5:
                current_date += timedelta(days=1)
                continue
                
            dt_str = current_date.strftime("%Y-%m-%d")
            print(f"Fetching portfolio for {dt_str}...")
            
            data = extract_holdings_for_date(page, dt_str)
            if data and data.get("code") == 200 and data.get("data") and data["data"].get("stocks"):
                stocks = data["data"]["stocks"]
                days_data[dt_str] = stocks
                for stock in stocks:
                    all_unique_stocks.add(stock["stocNo"].strip())
            else:
                print(f"No data for {dt_str}")
                
            current_date += timedelta(days=1)
            time.sleep(1) # delay to be nice
            
        # 3. Fetch Shioaji prices for all unique stocks over the entire period
        print(f"Fetching Shioaji prices for {len(all_unique_stocks)} stocks...")
        prices_cache = {} # {stock_code: {date_str: close_price}}
        
        for idx, code in enumerate(list(all_unique_stocks)):
            # some code might have a space or formatting issue, clean it
            clean_code = code.strip()
            print(f"Fetching prices for {clean_code} ({idx+1}/{len(all_unique_stocks)})")
            
            px_dict = fetch_shioaji_kbars(api, clean_code, start_date, end_date)
            prices_cache[clean_code] = px_dict
            
        # 4. Assemble the final CSVs
        # Output directory
        out_dir = os.path.join(os.path.dirname(__file__), "..", "data_snapshots")
        os.makedirs(out_dir, exist_ok=True)
        
        for dt_str, stocks in days_data.items():
            records = []
            for stock in stocks:
                code = stock["stocNo"].strip()
                name = stock["stocName"].strip()
                weight = stock["weight"]
                shares = stock["share"]
                
                # Get close price for this date
                close_px = prices_cache.get(code, {}).get(dt_str, None)
                
                records.append({
                    "股票代號": code,
                    "股票名稱": name,
                    "股數": shares,
                    "持股權重": weight,
                    "收盤價": close_px
                })
                
            df = pd.DataFrame(records)
            out_csv = os.path.join(out_dir, f"{dt_str}.csv")
            df.to_csv(out_csv, index=False, encoding="utf-8-sig")
            print(f"Saved {out_csv} with {len(df)} records.")

if __name__ == "__main__":
    main()
