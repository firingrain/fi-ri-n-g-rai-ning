import yfinance as yf
import pandas as pd
from datetime import datetime
import pytz

symbols = []
with open("symbols_jp.txt", "r") as f:
    symbols = [line.strip() for line in f.readlines() if line.strip()]

rows = []
for s in symbols:
    t = yf.Ticker(s)
    hist = t.history(period="5d")
    if not hist.empty:
        last = hist["Close"].iloc[-1]
        prev = hist["Close"].iloc[-2]
        change = last - prev
        change_pct = (last / prev - 1) * 100
        rows.append({
            "Symbol": s,
            "Last": round(last,2),
            "Change": round(change,2),
            "Change%": round(change_pct,2)
        })

df = pd.DataFrame(rows)
df = df.sort_values(by="Change%", ascending=False)
tokyo = pytz.timezone("Asia/Tokyo")
timestamp = datetime.now(tokyo).strftime("%Y-%m-%d %H:%M:%S")
df.insert(0, "Timestamp", timestamp)
df.to_csv("jp_latest.csv", index=False, encoding="utf-8-sig")

print(df)
print("\n✅ Saved as jp_latest.csv")
