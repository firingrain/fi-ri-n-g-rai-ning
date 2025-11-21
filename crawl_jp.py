# -*- coding: utf-8 -*-
"""
crawl_jp.py — 日本股票爬虫（yfinance 版本 · v6）
 - Yahoo API 已封禁，本版本改为 yfinance
 - 支持 800〜1500 支股票稳定循环
 - 自动重试、异常保护
 - 输出 jp_latest.csv
"""

import time
import yfinance as yf
import pandas as pd
from pathlib import Path
from datetime import datetime

SYMBOL_FILE = Path("symbols_jp.txt")
OUT_CSV = Path("jp_latest.csv")


# ================================
#   获取快速行情数据
# ================================
def fetch_quote(symbol: str):
    try:
        s = yf.Ticker(symbol)
        info = s.fast_info

        last = info.get("last_price", 0) or 0
        prev = info.get("previous_close", 0) or 0
        volume = info.get("last_volume", 0) or 0

        change = last - prev if prev else 0
        pct = (change / prev * 100) if prev else 0
        value = last * volume

        return {
            "Last": last,
            "Change": change,
            "Change%": pct,
            "Volume": volume,
            "Value": value,
        }
    except Exception:
        return {}


# ================================
#        近 5 天动能 MOM5
# ================================
def calc_mom5(symbol: str, last_price: float) -> float:
    try:
        hist = yf.Ticker(symbol).history(period="6d")
        closes = hist["Close"].dropna().tolist()

        if len(closes) < 2:
            return 0.0

        old = closes[0]
        if old == 0:
            return 0.0

        return (last_price - old) / old * 100
    except:
        return 0.0


# ================================
#        单支抓取逻辑
# ================================
def fetch_one(symbol: str) -> dict:
    # 尝试 3 次，避免网络抖动
    quote = {}
    for _ in range(3):
        quote = fetch_quote(symbol)
        if quote:
            break
        time.sleep(1)

    # 报错或获取失败
    if not quote:
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

    last = quote["Last"]
    mom5 = calc_mom5(symbol, last)

    return {
        "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "symbol": symbol,
        "Last": last,
        "Change": quote["Change"],
        "Change%": quote["Change%"],
        "MOM5%": mom5,
        "Volume": quote["Volume"],
        "Value(億JPY)": quote["Value"] / 1e8,
        "Turnover%": 0,
    }


# ================================
#              MAIN
# ================================
def main():
    if not SYMBOL_FILE.exists():
        print("❌ symbols_jp.txt 不存在")
        return

    symbols = [
        s.strip() for s in SYMBOL_FILE.read_text().splitlines()
        if s.strip()
    ]

    print(f"📌 开始抓取日本股票：共 {len(symbols)} 支")

    rows = []

    for idx, sym in enumerate(symbols, 1):
        print(f"[{idx}/{len(symbols)}] Fetching {sym} ...")
        row = fetch_one(sym)
        rows.append(row)
        time.sleep(0.20)  # 限速，避免被封锁

    df = pd.DataFrame(rows)
    df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")

    print("\n✅ jp_latest.csv 已成功更新（使用 yfinance 数据源）")


if __name__ == "__main__":
    main()
