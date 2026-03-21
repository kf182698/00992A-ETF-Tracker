import requests
url = "https://www.capitalfund.com.tw/CFWeb/api/etf/buyback"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Referer": "https://www.capitalfund.com.tw/etf/product/detail/500/basic",
    "Content-Type": "application/json"
}
payload = {"fundId": "500", "date": None}
try:
    r = requests.post(url, json=payload, headers=headers)
    print("Status:", r.status_code)
    print("Response:", r.text[:1000])
except Exception as e:
    print("Error:", e)
