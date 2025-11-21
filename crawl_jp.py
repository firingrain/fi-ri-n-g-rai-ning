# -*- coding: utf-8 -*-
"""
crawl_jp.py — 日本股票爬虫（Yahoo Finance 版本 · v4.0）
 - 稳定、不封禁
 - 支持大规模股票（数百～上千）
 - 自动重试
 - 返回完整行情字段
 - 输出 jp_latest.csv
"""

import time
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime

SYMBOL_FILE = Path("symbols_jp.txt")
OUT_CSV = Path("jp_latest.csv")

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

# ================
# Yahoo Quote API
# ================
def fetch_quote(symbol: str):
    url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={symbol}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=10).json()
        q = r["quoteResponse"]["result"][0]

        return {
            "Last": q.get("regularMarketPrice", 0),
            "Change": q.get("regularMarketChange", 0),
            "Change%": q.get("regularMarketChangePercent", 0),
            "Volume": q.get("regularMarketVolume", 0),
            "Value": q.get("regularMarketVolume", 0) * q.get("regularMarketPrice", 0),
        }
    except:
        return {}


# ========================
#   Yahoo Chart (MOM5)
# ========================
def fetch_history(symbol: str):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range=6d&interval=1d"
    try:
        r = requests.get(url, headers=HEADERS, timeout=10).json()
        closes = r["chart"]["result"][0]["indicators"]["quote"][0]["close"]
        closes = [c for c in closes if c is not None]
        return closes
    except:
        return []


def calc_mom5(symbol: str, last: float) -> float:
    closes = fetch_history(symbol)
    if len(closes) < 2:
        return 0.0
    old = closes[0]
    if old == 0:
        return 0.0
    return (last - old) / old * 100


# =======================
#   Fetch one stock
# =======================
def fetch_one(symbol: str) -> dict:
    # 自动重试 3 次
    for _ in range(3):
        q = fetch_quote(symbol)
        if q:
            break
        time.sleep(1)

    if not q:
        # 返回空行（并不影响后续）
        return {
            "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "symbol": symbol,
            "Last": 0,
            "Change": 0,
            "Change%": 0,
            "MOM5%": 0,
            "Volume": 0,
            "Value(億JPY)": 0,
            "Turnover%": 0,
        }

    last = q["Last"]
    mom5 = calc_mom5(symbol, last)

    return {
        "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "symbol": symbol,
        "Last": last,
        "Change": q["Change"],
        "Change%": q["Change%"],
        "MOM5%": mom5,
        "Volume": q["Volume"],
        "Value(億JPY)": q["Value"] / 1e8,
        "Turnover%": 0,  # 可扩展
    }


# =======================
#        MAIN
# =======================
def main():
    if not SYMBOL_FILE.exists():
        print("❌ symbols_jp.txt 不存在")
        return

    symbols = [s.strip() for s in SYMBOL_FILE.read_text().splitlines() if s.strip()]

    rows = []
    for idx, sym in enumerate(symbols, 1):
        print(f"[{idx}/{len(symbols)}] Fetching {sym} ...")
        row = fetch_one(sym)
        rows.append(row)
        time.sleep(0.3)   # 限速保护，防被封

    df = pd.DataFrame(rows)
    df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    print("✅ jp_latest.csv 已更新（Yahoo 数据源）")


if __name__ == "__main__":
    main()
