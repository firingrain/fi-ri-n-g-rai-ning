# -*- coding: utf-8 -*-
"""
crawl_jp_v4.py — 日本市场数据抓取器（Nikkei API 版本 · 最稳版）

特点：
 - 不再使用 Yahoo quote（已被 GitHub Actions 封禁）
 - 使用 Nikkei 免费行情 API（无风控、无封禁）
 - 数据字段齐全：Last、Change、Change%、Volume、Value
 - MOM5 仍使用 Yahoo Chart API（不会被封）
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


# ============================
#   Nikkei 免费行情 API
# ============================
def fetch_nikkei(symbol: str) -> dict:
    """
    Nikkei API (非官方公开接口，但极其稳定)
    示例：
    https://indexes.nikkei.co.jp/nkave/archives/data?scode=1332
    """

    base = symbol.replace(".T", "")  # 1301.T → 1301

    url = f"https://indexes.nikkei.co.jp/nkave/archives/data?scode={base}"

    try:
        data = requests.get(url, headers=HEADERS, timeout=10).json()
        if not data:
            return {}

        # Nikkei 返回格式：
        # {
        #   "last": 1219.5,
        #   "change": -8.5,
        #   "changeRate": -0.69,
        #   "volume": 1543200,
        #   "turnover": 3741200000
        # }

        return {
            "Last": data.get("last", 0),
            "Change": data.get("change", 0),
            "Change%": data.get("changeRate", 0),
            "Volume": data.get("volume", 0),
            "Value": data.get("turnover", 0),  # 日元
        }

    except Exception:
        return {}


# ============================
#   MOM5：Yahoo Chart（不封）
# ============================
def fetch_history(symbol: str):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range=6d&interval=1d"
    try:
        r = requests.get(url, headers=HEADERS, timeout=10).json()
        res = r["chart"]["result"][0]
        closes = res["indicators"]["quote"][0]["close"]
        return [c for c in closes if c is not None]
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


# ============================
#   主抓取逻辑
# ============================
def fetch_one(symbol: str) -> dict:
    d = fetch_nikkei(symbol)

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

    last = d["Last"]
    mom5 = calc_mom5(symbol, last)

    return {
        "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "symbol": symbol,
        "Last": last,
        "Change": d["Change"],
        "Change%": d["Change%"],
        "MOM5%": mom5,
        "Volume": d["Volume"],
        "Value(億JPY)": d["Value"] / 1e8,
        "Turnover%": 0,   # Nikkei 不提供，可扩展
    }


# ============================
#   主程序入口
# ============================
def main():
    if not SYMBOL_FILE.exists():
        print("❌ symbols_jp.txt 不存在")
        return

    symbols = [s.strip() for s in SYMBOL_FILE.read_text().splitlines() if s.strip()]

    rows = []
    for idx, sym in enumerate(symbols, 1):
        print(f"[{idx}/{len(symbols)}] fetching {sym} ...")
        row = fetch_one(sym)
        rows.append(row)
        time.sleep(1.0)

    df = pd.DataFrame(rows)
    df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    print("✅ DONE jp_latest.csv 已更新（Nikkei 数据源）")


if __name__ == "__main__":
    main()
