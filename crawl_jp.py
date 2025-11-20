# -*- coding: utf-8 -*-
"""
crawl_jp.py — JP stocks quick screener v3.4（含 MOM5%）
- 保留你所有原有的容错、重试、日志
- 新增动能指标 MOM5%（尽力计算模式：有几天算几天）
- MOM5% 将写入 jp_latest.csv，列名为 "MOM5%"
"""

import re
import unicodedata
from pathlib import Path
from datetime import datetime
import time

import pandas as pd
import yfinance as yf
import pytz


# ---------- 工具函数 ----------

def to_number_safe(s, default=0.0):
    """把字符串安全地转成 float，支持全角字符，忽略 # 后面的注释。"""
    if s is None:
        return default
    s = unicodedata.normalize("NFKC", str(s))

    s = s.split("#", 1)[0].strip()
    if not s:
        return default

    s = s.replace(",", "").replace("％", "").replace("%", "")
    try:
        if "." in s:
            return float(s)
        return float(int(s))
    except Exception:
        return default


# ---------- 配置 & 代码列表 ----------

def load_config(path="config_jp.txt"):
    cfg = {
        "MIN_CHANGE": 0.0,
        "MIN_TURNOVER": 0.0,
        "MIN_VALUE": 0.0,
        "TOP_LIMIT": 20,
    }
    p = Path(path)
    if not p.exists():
        print(f"⚠️ 未找到 {path}，使用默认配置: {cfg}")
        return cfg

    pat = re.compile(r"^\s*([A-Z_]+)\s*=\s*(.+)$")

    for raw in p.read_text(encoding="utf-8").splitlines():
        m = pat.match(raw)
        if not m:
            continue
        k, v = m.group(1), m.group(2)
        if k in cfg:
            cfg[k] = to_number_safe(v, cfg[k])

    try:
        cfg["TOP_LIMIT"] = max(1, int(cfg["TOP_LIMIT"]))
    except Exception:
        cfg["TOP_LIMIT"] = 20

    print(f"🔧 配置读取完成: {cfg}")
    return cfg


def load_symbols(path="symbols_jp.txt"):
    p = Path(path)
    if not p.exists():
        print(f"⚠️ 未找到 {path}")
        return []

    syms = []
    for ln in p.read_text(encoding="utf-8").splitlines():
        s = ln.strip()
        if not s or s.startswith("#"):
            continue
        s = unicodedata.normalize("NFKC", s)
        syms.append(s)

    syms = [s if "." in s else f"{s}.T" for s in syms]
    return syms


cfg = load_config()
symbols = load_symbols()

print(f"📌 待抓取代码数: {len(symbols)} → 示例: {symbols[:5]}")

if not symbols:
    print("⚠️ 无股票代码可抓取。")
    raise SystemExit(0)


# ---------- 主循环：拉取行情数据 ----------

rows = []

for s in symbols:
    try:
        t = yf.Ticker(s)

        # --- 历史数据，带简单重试 ---
        hist = None
        for i in range(3):
            try:
                hist = t.history(period="6d", auto_adjust=False)
            except Exception as e_hist:
                print(f"⚠️ {s} 第 {i+1} 次 history() 调用失败: {e_hist}")
                hist = None
            if hist is not None and not hist.empty:
                break
            time.sleep(1.0)

        if hist is None or hist.empty or len(hist) < 1:
            print(f"… {s} 无最近数据，跳过")
            continue

        # ---------- 价格与涨跌 ----------
        last = float(hist["Close"].iloc[-1])
        prev = float(hist["Close"].iloc[-2]) if len(hist) >= 2 else last
        change = last - prev
        change_pct = (last / prev - 1.0) * 100.0 if prev != 0 else 0.0

        # ---------- MOM5% 动能（方案 B：能算多少算多少） ----------
        if len(hist) >= 6:
            mom5 = (last / float(hist["Close"].iloc[-6]) - 1.0) * 100.0
        elif len(hist) >= 4:
            mom5 = (last / float(hist["Close"].iloc[-4]) - 1.0) * 100.0
        elif len(hist) >= 2:
            mom5 = (last / float(hist["Close"].iloc[-2]) - 1.0) * 100.0
        else:
            mom5 = 0.0

        # ---------- 成交量 / 成交额 ----------
        vol = float(hist["Volume"].iloc[-1]) if "Volume" in hist.columns else 0.0
        value_oku = (last * vol) / 1e8

        # ---------- 流通股数：fast_info / info ----------
        shares_out = None
        try:
            fi = getattr(t, "fast_info", None)
            if fi is not None:
                if isinstance(fi, dict):
                    shares_out = fi.get("shares_outstanding") or fi.get("sharesOutstanding")
                else:
                    shares_out = getattr(fi, "shares_outstanding", None) or \
                                 getattr(fi, "sharesOutstanding", None)
        except Exception:
            pass

        if not shares_out:
            try:
                info = getattr(t, "info", None)
                if isinstance(info, dict):
                    shares_out = info.get("sharesOutstanding") or info.get("shares_outstanding")
            except Exception:
                shares_out = None

        turnover = None
        try:
            if shares_out and float(shares_out) > 0:
                turnover = float(vol) / float(shares_out) * 100.0
        except Exception:
            turnover = None

        rows.append({
            "symbolSymbol": s,
            "Last": round(last, 2),
            "Change": round(change, 2),
            "Change%": round(change_pct, 2),
            "MOM5%": round(mom5, 2),
            "Volume": int(vol),
            "Value(億JPY)": round(value_oku, 2),
            "Turnover%": round(turnover, 2) if turnover is not None else None,
        })

        print(
            f"✅ {s} 收={last:.2f} 涨幅={change_pct:.2f}% MOM5={mom5:.2f}% "
            f"成交额(亿)={value_oku:.2f}"
        )

    except Exception as e:
        print(f"⚠️ {s} 抓取失败: {e}")


# ---------- 汇总 & 过滤 ----------

df = pd.DataFrame(rows)
if df.empty:
    print("⚠️ 抓取结果为空。")
    raise SystemExit(0)


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
    print("⚠️ 所有股票被过滤。")
    raise SystemExit(0)

df = df.sort_values(
    by=["Change%", "Value(億JPY)"],
    ascending=[False, False],
).head(cfg["TOP_LIMIT"])

# ---------- 输出 ----------

tokyo = pytz.timezone("Asia/Tokyo")
ts = datetime.now(tokyo).strftime("%Y-%m-%d %H:%M:%S")
df.insert(0, "Timestamp", ts)

out_path = "jp_latest.csv"
df.to_csv(out_path, index=False, encoding="utf-8-sig")

print(f"\n🎯 最终输出 {len(df)} 条 → {out_path}")
