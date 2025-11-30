# -*- coding: utf-8 -*-
"""
crawl_jp.py — 日本股票爬虫（yfinance 版本 · v7 · 稳定）
 - 不再使用 fast_info（已大面积失效）
 - 所有数据改用 history() 获取，稳定可用
 - MOM5、Change、Change% 全从历史数据计算
 - 支持 800〜1500 支无异常全量抓取
"""

import time
import yfinance as yf
import pandas as pd
from pathlib import Path
from datetime import datetime

SYMBOL_FILE = Path("symbols_jp.txt")
OUT_CSV = Path("jp_latest.csv")


# ================================
#  从 history 获取行情（稳定）
# ================================
def fetch_history(symbol: str):
    """
    获取近 7 天历史数据，确保：
    - last_price
    - previous_close
    - volume
    - mom5
    都能从 history() 计算出来。
    """
    try:
        # 获取近 7 天（含今天），避免停牌日导致缺值
        hist = yf.Ticker(symbol).history(period="7d")

        if hist.empty:
            return None

        closes = hist["Close"].dropna().tolist()
        volumes = hist["Volume"].fillna(0).tolist()

        # 最新收盘价
        last = closes[-1]

        # 昨日收盘价（若只有一天数据，则 previous_close = last）
        prev = closes[-2] if len(closes) >= 2 else last

        # 今日成交量
        volume = volumes[-1]

        # 计算变动
        change = last - prev
        pct = (change / prev * 100) if prev else 0

        # 5 日动能（MOM5）
        if len(closes) >= 6:
            old = closes[0]
            mom5 = (last - old) / old * 100 if old else 0
        else:
            mom5 = 0

        return {
            "Last": float(last),
            "Previous": float(prev),
            "Change": float(change),
            "Change%": float(pct),
            "Volume": int(volume),
            "MOM5%": float(mom5),
            "Value": float(last * volume),
        }

    except Exception:
        return None


# ================================
#         单支抓取逻辑
# ================================
def fetch_one(symbol: str) -> dict:
    data = None

    # 自动重试 3 次，避免网络抖动
    for _ in range(3):
        data = fetch_history(symbol)
        if data:
            break
        time.sleep(1)

    if not data:
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

    return {
        "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "symbol": symbol,
        "Last": data["Last"],
        "Change": data["Change"],
        "Change%": data["Change%"],
        "MOM5%": data["MOM5%"],
        "Volume": data["Volume"],
        "Value(億JPY)": data["Value"] / 1e8,
        "Turnover%": 0,   # 如需可扩展
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
        rows.append(fetch_one(sym))
        time.sleep(0.15)  # 限速，防封锁

    df = pd.DataFrame(rows)
    df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")

    print("\n✅ jp_latest.csv 已成功更新（使用 history() 数据源）")


if __name__ == "__main__":
    main()
