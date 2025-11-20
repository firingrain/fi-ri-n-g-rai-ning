# -*- coding: utf-8 -*-
"""
crawl_jp.py — 日本市场数据抓取器（升级版 v3.5）

功能：
 - 从 Yahoo JP 抓取实时行情
 - 提取：Last, Change, Change%, Volume, Value(売買代金), MOM5%
 - 自动兼容不同格式字段
 - 输出 jp_latest.csv

此文件与新版 generate_watchlist.py 完全兼容。
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


# -------- 核心抓取 Yahoo API --------
def fetch_yahoo(symbol: str) -> dict:
    """
    Yahoo Finance Japan quote API
    """
    url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={symbol}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        data = r.json().get("quoteResponse", {}).get("result", [])
        if not data:
            return {}
        return data[0]
    except Exception:
        return {}


# -------- 抽取字段（容错） --------
def get_safe(d: dict, *keys, default=0.0):
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return default


# -------- 抓取每个股票 --------
def fetch_one(symbol: str) -> dict:
    d = fetch_yahoo(symbol)
    if not d:
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

    last = get_safe(d, "regularMarketPrice", "postMarketPrice")
    chg = get_safe(d, "regularMarketChange", "postMarketChange")
    chg_pct = get_safe(d, "regularMarketChangePercent", "postMarketChangePercent")

    vol = get_safe(d, "regularMarketVolume", "postMarketVolume")
    val = float(last) * float(vol) / 1e8  # → 亿日元单位

    # Yahoo 没有 MOM5，需要自己算（抓取 5 天历史）
    mom5 = calc_mom5(symbol, last)

    return {
        "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "symbol": symbol,
        "Last": last,
        "Change": chg,
        "Change%": chg_pct,
        "MOM5%": mom5,
        "Volume": vol,
        "Value(億JPY)": val,
        "Turnover%": get_safe(d, "regularMarketDayHigh", default=0),  # 若无字段，留 0
    }


# -------- 计算 MOM5（过去 5 天涨幅）--------
def fetch_history(symbol: str):
    """
    Yahoo 历史 K 线 API
    """
    url = (
        "https://query1.finance.yahoo.com/v8/finance/chart/"
        f"{symbol}?range=6d&interval=1d"
    )
    try:
        r = requests.get(url, headers=HEADERS, timeout=10).json()
        res = r["chart"]["result"][0]
        closes = res["indicators"]["quote"][0]["close"]
        return closes
    except Exception:
        return []


def calc_mom5(symbol: str, last: float) -> float:
    closes = fetch_history(symbol)
    closes = [c for c in closes if c is not None]

    if len(closes) < 2:
        return 0.0

    old = closes[0]
    if old == 0:
        return 0.0

    return (last - old) / old * 100


# -------- 主程序 --------
def main():
    if not SYMBOL_FILE.exists():
        print("❌ symbols_jp.txt 不存在")
        return

    symbols = [s.strip() for s in SYMBOL_FILE.read_text().splitlines() if s.strip()]
    if not symbols:
        print("❌ symbols_jp.txt 为空")
        return

    rows = []
    for idx, sym in enumerate(symbols, 1):
        print(f"[{idx}/{len(symbols)}] fetching {sym} ...")
        row = fetch_one(sym)
        rows.append(row)
        time.sleep(1.0)  # 降低请求频率，避免封禁

    df = pd.DataFrame(rows)
    df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    print("✅ DONE jp_latest.csv 已生成")


if __name__ == "__main__":
    main()
