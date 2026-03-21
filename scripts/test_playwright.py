import sys
from playwright.sync_api import sync_playwright

url = "https://www.capitalfund.com.tw/etf/product/detail/500/basic"

def fetch():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url)
        page.wait_for_load_state("networkidle")
        content = page.content()
        with open("capital_basic.html", "w", encoding="utf-8") as f:
            f.write(content)
        browser.close()

if __name__ == "__main__":
    fetch()
