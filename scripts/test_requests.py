import requests
url = "https://www.capitalfund.com.tw/etf/product/detail/500/basic"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"
}
r = requests.get(url, headers=headers)
print("Status:", r.status_code)
if r.status_code == 200:
    with open("capital_basic.html", "w", encoding="utf-8") as f:
        f.write(r.text)
