# -*- coding: utf-8 -*-
"""
crawl_jp.py — JP stocks quick screener v3.2
✔ 读取 symbols_jp.txt / config_jp.txt（全角/注释/空格容错）
✔ 计算涨跌幅、成交额（亿JPY）、换手率
✔ 详细日志：打印配置、代码数量、抓取成功/失败
✔ 空表友好退出（供工作流判断）
"""

import re, unicodedata
from pathlib import Path
from datetime import datetime
import pandas as pd
import yfinance as yf
import pytz
import time

def to_number_safe(s, default=0.0):
    if s is None: return default
    s = unicodedata.normalize("NFKC", str(s))
    s = s.split("#", 1)[0].strip()
    if not s: return default
    try:
        if "." in s: return float(s)
        return float(int(s))
    except Exception:
        return default

def load_config(path="config_jp.txt"):
    cfg = {"MIN_CHANGE":0.0,"MIN_TURNOVER":0.0,"MIN_VALUE":0.0,"TOP_LIMIT":20}
    p = Path(path)
    if not p.exists(): return cfg
    pat = re.compile(r"^\s*([A-Z_]+)\s*=\s*(.+)$")
    for raw in p.read_text(encoding="utf-8").splitlines():
        m = pat.match(raw); 
        if not m: continue
        k,v = m.group(1), m.group(2)
        if k in cfg: cfg[k] = to_number_safe(v, cfg[k])
    try: cfg["TOP_LIMIT"] = max(1, int(cfg["TOP_LIMIT"]))
    except Exception: cfg["TOP_LIMIT"] = 20
    return cfg

def load_symbols(path="symbols_jp.txt"):
    p = Path(path)
    if not p.exists(): 
        print("⚠️ 未找到 symbols_jp.txt"); 
        return []
    syms = [ln.strip() for ln in p.read_text(encoding="utf-8").splitlines() if ln.strip()]
    return [s if "." in s else f"{s}.T" for s in syms]

cfg = load_config()
symbols = load_symbols()
print(f"🔧 配置: {cfg}")
print(f"📌 待抓取代码数: {len(symbols)} → 示例: {symbols[:5]}")

if not symbols:
    print("⚠️ 无股票代码可抓取。"); raise SystemExit(0)

rows = []
for s in symbols:
    try:
        t = yf.Ticker(s)
        # 简单重试（网络抖动时更稳）
        hist = None
        for _ in range(2):
            hist = t.history(period="5d", auto_adjust=False)
            if hist is not None and not hist.empty: break
            time.sleep(1)
        if hist is None or hist.empty or len(hist) < 2:
            print(f"… {s} 无最近数据，跳过")
            continue

        last = float(hist["Close"].iloc[-1]); prev = float(hist["Close"].iloc[-2])
        change = last - prev; change_pct = (last/prev - 1.0) * 100.0
        vol = float(hist.get("Volume", pd.Series([0])) .iloc[-1])
        value_oku = (last * vol) / 1e8

        shares_out = None
        try: shares_out = getattr(t, "fast_info", {}).get("shares_outstanding", None)
        except Exception: pass
        if not shares_out:
            try: shares_out = (t.info or {}).get("sharesOutstanding", None)
            except Exception: shares_out = None
        turnover = (vol / shares_out * 100.0) if shares_out and shares_out > 0 else None

        rows.append({
            "Symbol": s, "Last": round(last,2), "Change": round(change,2),
            "Change%": round(change_pct,2), "Volume": int(vol),
            "Value(億JPY)": round(value_oku,2),
            "Turnover%": round(turnover,2) if turnover is not None else None
        })
        print(f"✅ {s} ok  收={last}  涨幅={round(change_pct,2)}%  成交额(亿)={round(value_oku,2)}  换手={None if turnover is None else round(turnover,2)}%")
    except Exception as e:
        print(f"⚠️ {s} 抓取失败: {e}")

df = pd.DataFrame(rows)
if df.empty:
    print("⚠️ 抓取结果为空。"); raise SystemExit(0)

def pass_threshold(row):
    if row["Change%"] < cfg["MIN_CHANGE"]: return False
    if row["Value(億JPY)"] < cfg["MIN_VALUE"]: return False
    if row["Turnover%"] is not None and row["Turnover%"] < cfg["MIN_TURNOVER"]: return False
    return True

df = df[df.apply(pass_threshold, axis=1)]
if df.empty:
    print("⚠️ 所有股票被阈值过滤。"); raise SystemExit(0)

df = df.sort_values(by=["Change%","Value(億JPY)"], ascending=[False,False]).head(cfg["TOP_LIMIT"])

tokyo = pytz.timezone("Asia/Tokyo")
ts = datetime.now(tokyo).strftime("%Y-%m-%d %H:%M:%S")
df.insert(0, "Timestamp", ts)
df.to_csv("jp_latest.csv", index=False, encoding="utf-8-sig")
print(f"\n🎯 最终输出 {len(df)} 条 → jp_latest.csv")
