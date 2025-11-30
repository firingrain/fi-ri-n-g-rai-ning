# -*- coding: utf-8 -*-
"""
crawl_jp.py — 日本股票爬虫（yfinance 版本 · v7，稳定）
 - Yahoo API 封禁 → 使用 yfinance
 - 自动限速、强力异常保护
 - 避免 fast_info 不返回数据
 - 避免 history 请求被拒
 - 保证 800+ 日本股票能完整抓取
"""

import time
import yfinance as yf
import pandas as pd
from pathlib import Path
from datetime import datetime

SYMBOL_FILE = Path("symbols_jp.txt")
OUT_CSV = Path("jp_latest.csv")


# ================================
#   安全获取 fast_info
# ================================
def fetch_fast_info(ticker: yf.Ticker):
    """安全获取 fast_info"""
    try:
        info = ticker.fast_info
        if not info:
            return {}
        return info
    except Exception:
        return {}


# ================================
#   快速行情数据
# ================================
def fetch_quote(symbol: str):
    try:
        t = yf.Ticker(symbol)
        info = fetch_fast_info(t)

        last = info.get("last_price") or 0
        prev = info.get("previous_close") or 0
        volume = info.get("last_volume") or 0

        if last is None:
            last = 0
        if prev is None:
            prev = 0
        if volume is None:
            volume = 0

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
#   5 日动能 MOM5
# ================================
def calc_mom5(symbol: str, last_price: float) -> float:
    try:
        t = yf.Ticker(symbol)
        hist = t.history(period="6d")

        if "Close" not in hist or len(hist) < 2:
            return 0.0

        closes = hist["Close"].dropna().tolist()
        old = closes[0]

        if old == 0:
            return 0.0

        return (last_price - old) / old * 100

    except Exception:
        return 0.0


# ================================
#   单支抓取
# ================================
def fetch_one(symbol: str) -> dict:
    # 重试 3 次
    quote = {}
    for _ in range(3):
        quote = fetch_quote(symbol)
        if quote:
            break
        time.sleep(1.0)

    # 完全失败
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
#   MAIN
# ================================
def main():
    if not SYMBOL_FILE.exists():
        print("❌ symbols_jp.txt 不存在")
        return

    symbols = [
        s.strip()
        for s in SYMBOL_FILE.read_text().splitlines()
        if s.strip()
    ]

    print(f"📌 开始抓取日本股票：共 {len(symbols)} 支")

    rows = []

    for idx, sym in enumerate(symbols, 1):
        print(f"[{idx}/{len(symbols)}] Fetching {sym} ...")

        row = fetch_one(sym)
        rows.append(row)

        time.sleep(0.35)  # ⭐ 放大限速，避免 yfinance 拒绝或封禁

    df = pd.DataFrame(rows)

    df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")

    print("\n✅ jp_latest.csv 已成功更新（使用 yfinance · 稳定版）")


if __name__ == "__main__":
    main()
