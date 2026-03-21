import sys
from playwright.sync_api import sync_playwright

def test_dates():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto("https://www.capitalfund.com.tw/etf/product/detail/500/basic", wait_until="networkidle")
        
        js_code = """
        (dateStr) => {
            return fetch("https://www.capitalfund.com.tw/CFWeb/api/etf/buyback", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ fundId: "500", date: dateStr })
            }).then(res => res.json());
        }
        """
        
        for d in [None, "2026-03-20T16:00:00.000Z", "2026-03-20T00:00:00.000Z", "2026-03-20"]:
            res = page.evaluate(js_code, d)
            try:
                stocks = res.get("data", {}).get("stocks", [])
                date1 = res.get("data", {}).get("pcf", {}).get("date1", "")
                print(f"Test {d} -> date1: {date1}, stocks: {len(stocks)}")
            except Exception as e:
                print(f"Test {d} Error: {e}")
        
        browser.close()

if __name__ == "__main__":
    test_dates()
