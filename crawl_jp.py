# -*- coding: utf-8 -*-
"""
crawl_jp.py
JP stocks quick screener v3
✔ 自动读取 symbols_jp.txt
✔ 自动读取 config_jp.txt（容错：全角、注释、空格）
✔ 计算涨跌幅、成交额、换手率
✔ 空表自动保护（不报错）
✔ 输出 jp_latest.csv
"""

import re
import yfinance as yf
import pandas as pd
from datetime import datetime
import pytz
from pathlib import Path
import unicodedata

# ---------- 通用安全转换 ----------
def to_number_safe(s, default=0.0):
    """把各种奇怪字符转成数字"""
    if s is None:
        return default
    s = unicodedata.normalize("NFKC", str(s))  # 全角→半角
    s = s.split("#", 1)[0].strip()  # 去掉注释
    if not s:
        return default
    try:
        if "." in s:
            return float(s)
        return float(int(s))
    except Exception:
        return default

# ---------- 读取配置 ----------
def load_config(path="config_jp.txt"):
    cfg = {
        "MIN_CHANGE": 0.0,      # 最小涨跌幅 %
        "MIN_TURNOVER": 0.0,    # 最小换手率 %
        "MIN_VALUE": 0.0,       # 最小成交额（亿日元）
        "TOP_LIMIT": 20         # 输出前N名
    }
    p = Path(path)
    if not p.exists():
        return cfg

    pat = re.compile(r"^\s*([A-Z_]+)\s*=\s*(.+)$")
    for raw in p.read_text(encoding="utf-8").splitlines():
        m = pat.match(raw)
        if not m:
            continue
        k, v = m.group(1), m.group(2)
        if k in cfg:
            cfg[k] = to_number_safe(v, cfg[k])

    # 转换TOP_LIMIT为整数
    try:
        cfg["TOP_LIMIT"] = max(1, int(cfg["TOP_LIMIT"]))
    except Exception:
        cfg["TOP_LIMIT"] = 20

    return cfg

# ---------- 读取股票代码 ----------
def load_symbols(path="symbols_jp.txt"):
    p = Path(path)
    if not p.exists():
        print("⚠️ 未找到 symbols_jp.txt")
        return []
    syms = [ln.strip() for ln in p.read_text(encoding="utf-8").splitlines() if ln.strip()]
    # 自动补.T
    fixed = [s if "." in s else f"{s}.T" for s in syms]
    return fixed

# ---------- 主逻辑 ----------
cfg = load_config()
symbols = load_symbols()

if not symbols:
    print("⚠️ 无股票代码可抓取。")
    raise SystemExit(0)

rows = []
for s in symbols:
    try:
        t = yf.Ticker(s)
        hist = t.history(period="5d", auto_adjust=False)
        if hist is None or hist.empty or len(hist) < 2:
            continue

        last = float(hist["Close"].iloc[-1])
        prev = float(hist["Close"].iloc[-2])
        change = last - prev
        change_pct = (last / prev - 1.0) * 100.0

        vol = float(hist["Volume"].iloc[-1]) if "Volume" in hist.columns else 0.0
        value_jpy = last * vol
        value_oku = value_jpy / 1e8  # 亿日元

        shares_out = None
        try:
            shares_out = getattr(t, "fast_info", {}).get("shares_outstanding", None)
        except Exception:
            pass
        if not shares_out:
            try:
                info = t.info or {}
                shares_out = info.get("sharesOutstanding", None)
            except Exception:
                shares_out = None

        turnover_pct = (vol / shares_out * 100.0) if shares_out and shares_out > 0 else None

        rows.append({
            "Symbol": s,
            "Last": round(last, 2),
            "Change": round(change, 2),
            "Change%": round(change_pct, 2),
            "Volume": int(vol),
            "Value(億JPY)": round(value_oku, 2),
            "Turnover%": round(turnover_pct, 2) if turnover_pct is not None else None
        })
    except Exception as e:
        print(f"⚠️ {s} 抓取失败: {e}")
        continue

df = pd.DataFrame(rows)

if df.empty:
    print("⚠️ 没有符合条件的数据。")
    raise SystemExit(0)

# ---------- 筛选 ----------
def pass_threshold(row):
    if row["Change%"] < cfg["MIN_CHANGE"]:
        return False
    if row["Value(億JPY)"] < cfg["MIN_VALUE"]:
        return False
    if row["Turnover%"] is not None and row["Turnover%"] < cfg["MIN_TURNOVER"]:
        return False
    return True

df = df[df.apply(pass_threshold, axis=1)]
if df.empty:
    print("⚠️ 所有股票都被过滤掉。")
    raise SystemExit(0)

df = df.sort_values(by=["Change%", "Value(億JPY)"], ascending=[False, False]).head(cfg["TOP_LIMIT"])

# ---------- 输出 ----------
tokyo = pytz.timezone("Asia/Tokyo")
timestamp = datetime.now(tokyo).strftime("%Y-%m-%d %H:%M:%S")
df.insert(0, "Timestamp", timestamp)
df.to_csv("jp_latest.csv", index=False, encoding="utf-8-sig")

print(f"✅ {timestamp} 筛选完成，共 {len(df)} 条；已保存 jp_latest.csv")
