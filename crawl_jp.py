# -*- coding: utf-8 -*-
"""
crawl_jp.py — 日本股票爬虫（yfinance 版本 · v5.0）
 - Yahoo API 已封禁，本版本使用 yfinance 稳定抓取
 - 支持大规模股票（数百～上千）
 - 自动重试
 - 输出 jp_latest.csv
"""

import time
import yfinance as yf
import pandas as pd
from pathlib import Path
from datetime import datetime

SYMBOL_FILE = Path("symbols_jp.txt")
OUT_CSV = Path("jp_latest.csv")


# =======================
#   获取主行情数据
# =======================
def fetch_quote(symbol: str):
    try:
        s = yf.Ticker(symbol)
        info = s.fast_info     # 更快、更稳定

        last = info.get("last_price", 0)
        prev = info.get("previous_close", 0)
        volume = info.get("last_volume", 0)

        change = last - prev if prev else 0
        change_pct = (change / prev * 100) if prev else 0
        value = (last * volume)

        return {
            "Last": last or 0,
            "Change": change or 0,
            "Change%": change_pct or 0,
            "Volume": volume or 0,
            "Value": value or 0,
        }

    except Exception:
        return {}


# =======================
#   获取历史数据 — MOM5
# =======================
def calc_mom5(symbol: str, last: float) -> float:
    try:
        s = yf.Ticker(symbol)
        hist = s.history(period="6d")

        closes = hist["Close"].dropna().tolist()

        if len(closes) < 2:
            return 0.0

        old = closes[0]
        if old == 0:
            return 0.0

        return (last - old) / old * 100
    except:
        return 0.0


# =======================
#   单支股票
# =======================
def fetch_one(symbol: str) -> dict:
    # 自动重试 3 次
    q = {}
    for _ in range(3):
        q = fetch_quote(symbol)
        if q:
            break
        time.sleep(1)

    if not q:
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
        "Turnover%": 0,
    }


# =======================
#        MAIN
# =======================
def main():
    if not SYMBOL_FILE.exists():
        print("❌ symbols_jp.txt 不存在")
        return

    symbols = [s.strip() for s in SYMBOL_FILE.read_text().splitlines() if s.strip()]

    print(f"📌 共 {len(symbols)} 支股票")
    rows = []

    for idx, sym in enumerate(symbols, 1):
        print(f"[{idx}/{len(symbols)}] Fetching {sym} ...")
        row = fetch_one(sym)
        rows.append(row)
        time.sleep(0.2)   # 稍微限速，防止被封

    df = pd.DataFrame(rows)
    df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    print("✅ jp_latest.csv 已更新（yfinance 数据源）")


if __name__ == "__main__":
    main()
