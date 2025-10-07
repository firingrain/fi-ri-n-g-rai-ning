import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ===============================================
# JP Auto Crawler - 自动版 by 影風
# 功能：
# 1. 自动读取 generate_watchlist.py 生成的 watchlist_jp.txt
# 2. 抓取日股实时行情
# 3. 输出 jp_latest.csv（含时间戳）
# ===============================================

# 日本时间
JST = timezone(timedelta(hours=9))
BASE_URL = "https://finance.yahoo.co.jp/quote/{}"
CSV_FILE = "jp_latest.csv"

# === 读取自动生成的 watchlist ===
watchlist_file = Path("watchlist_jp.txt")
if watchlist_file.exists():
    symbols = [
        l.strip() for l in watchlist_file.read_text(encoding="utf-8").splitlines()
        if l.strip() and not l.startswith("#")
    ]
else:
    symbols = ["6501.T", "6857.T", "8035.T", "6954.T", "6758.T"]  # 默认备用清单

print(f"🚀 JP Auto Crawler started ({len(symbols)} symbols)")
print("-" * 60)

results = []
for code in symbols:
    url = BASE_URL.format(code)
    try:
        headers = {"User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X)"}
        r = requests.get(url, headers=headers, timeout=10)
        r.raise_for_status()

        soup = BeautifulSoup(r.text, "html.parser")
        price = soup.select_one('fin-streamer[data-field="regularMarketPrice"]')
        chg = soup.select_one('fin-streamer[data-field="regularMarketChange"]')
        pct = soup.select_one('fin-streamer[data-field="regularMarketChangePercent"]')

        results.append({
            "Timestamp": datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S"),
            "Symbol": code,
            "Price": price.text.strip() if price else "N/A",
            "Change": chg.text.strip() if chg else "N/A",
            "Change%": pct.text.strip() if pct else "N/A"
        })
        print(f"[OK] {code:<8} → {price.text.strip() if price else 'N/A'} ({chg.text.strip() if chg else 'N/A'}, {pct.text.strip() if pct else 'N/A'})")

    except Exception as e:
        print(f"[Error] {code}: {e}")

# === 保存为 CSV ===
with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["Timestamp", "Symbol", "Price", "Change", "Change%"])
    writer.writeheader()
    writer.writerows(results)

print("-" * 60)
print(f"✅ Saved {len(results)} records to {CSV_FILE}")
print(f"🕒 Done at {datetime.now(JST).strftime('%Y-%m-%d %H:%M:%S')}")
