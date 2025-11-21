# -*- coding: utf-8 -*-
"""
crawl_jp.py — 日本市场数据抓取器（修复版 v3.6）

功能改进：
 - 修复深夜 Yahoo 不返回实时行情导致全为 0 的问题
 - 深夜 / 节假日自动 fallback 使用“前一日收盘价”
 - Yahoo 限流自动重试
 - MOM5 计算逻辑优化为稳定版
 - 修复 Value(億JPY) 计算逻辑
 - 全字段容错处理
"""

import time
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime

SYMBOL_FILE = Path("symbols_jp.txt")
OUT_CSV = Path("jp_latest.csv")

HEADERS = {"User-Agent": "Mozilla/5.0"}


# ----------- Yahoo quote 实时行情（带重试） -----------
def fetch_yahoo(symbol, retry=3):
    url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={symbol}"

    for _ in range(retry):
        try:
            r = requests.get(url, headers=HEADERS, timeout=10).json()
            data = r.get("quoteResponse", {}).get("result", [])
            if data:
                return data[0]
        except:
            time.sleep(1)

    return None


# ----------- 获取昨日收盘价（chart API） -----------
def fetch_last_close(symbol):
    url = (
        "https://query1.finance.yahoo.com/v8/finance/chart/"
        f"{symbol}?range=2d&interval=1d"
    )
    try:
        j = requests.get(url, headers=HEADERS, timeout=10).json()
        result = j["chart"]["result"][0]
        closes = result["indicators"]["quote"][0]["close"]
        closes = [c for c in closes if c is not None]
        if closes:
            return closes[-1]   # 最后一条为昨日收盘
    except:
        pass
    return None


# ----------- MOM5 算过去 5 天涨幅 -----------
def fetch_history(symbol):
    url = (
        "https://query1.finance.yahoo.com/v8/finance/chart/"
        f"{symbol}?range=6d&interval=1d"
    )
    try:
        j = requests.get(url, headers=HEADERS, timeout=10).json()
        res = j["chart"]["result"][0]
        closes = res["indicators"]["quote"][0]["close"]
        closes = [c for c in closes if c is not None]
        return closes
    except:
        return []


def calc_mom5(symbol, ref_price):
    closes = fetch_history(symbol)
    if len(closes) < 2:
        return 0.0

    old = closes[0]
    if not old:
        return 0.0

    return (ref_price - old) / old * 100


# ----------- 获取单只股票 -----------
def fetch_one(symbol: str) -> dict:
    now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    d = fetch_yahoo(symbol)

    # Yahoo 没返回有效数据（深夜 / 节假日 / 限流）
    if not d or d.get("regularMarketPrice") in (None, 0):
        last = fetch_last_close(symbol) or 0
        vol = d.get("regularMarketVolume", 0) if d else 0
        chg = 0
        chg_pct = 0
    else:
        last = d.get("regularMarketPrice", 0)
        vol = d.get("regularMarketVolume", 0)
        chg = d.get("regularMarketChange", 0)
        chg_pct = d.get("regularMarketChangePercent", 0)

    # Value 单位：万股 × 价格 → 换算成“亿日元”
    value_jpy_oku = float(last) * float(vol) / 1e8

    # MOM5
    mom5 = calc_mom5(symbol, last)

    return {
        "Timestamp": now_ts,
        "symbol": symbol,
        "Last": last,
        "Change": chg,
        "Change%": chg_pct,
        "MOM5%": mom5,
        "Volume": vol,
        "Value(億JPY)": value_jpy_oku,
        "Turnover%": d.get("regularMarketDayHigh", 0) if d else 0,
    }


# ----------- 主程序 -----------
def main():
    if not SYMBOL_FILE.exists():
        print("❌ symbols_jp.txt 不存在")
        return

    symbols = [
        s.strip()
        for s in SYMBOL_FILE.read_text().splitlines()
        if s.strip()
    ]

    if not symbols:
        print("❌ symbols_jp.txt 为空")
        return

    rows = []
    for idx, sym in enumerate(symbols, 1):
        print(f"[{idx}/{len(symbols)}] fetching {sym} ...")
        row = fetch_one(sym)
        rows.append(row)
        time.sleep(1.0)

    df = pd.DataFrame(rows)
    df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    print("✅ DONE — jp_latest.csv 已生成（v3.6 修复版）")


if __name__ == "__main__":
    main()
