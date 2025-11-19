# -*- coding: utf-8 -*-
"""
crawl_jp.py — JP stocks quick screener v3.3（修正版）
- 配置读取容错（全角 / 注释 / 空行）
- yfinance 拉取历史数据带重试 & 明确报错位置
- fast_info / info 取 shares_outstanding 时做了类型与接口保护
- 所有关键步骤增加防呆判断，避免 None / 空表导致异常
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
    # 去掉常见的尾部符号：%、全角百分号、逗号等
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

    # TOP_LIMIT 必须是整数
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
        # 全角转半角，容错
        s = unicodedata.normalize("NFKC", s)
        syms.append(s)

    # 没有 . 的统一加 .T
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
                hist = t.history(period="5d", auto_adjust=False)
            except Exception as e_hist:
                print(f"⚠️ {s} 第 {i+1} 次 history() 调用失败: {e_hist}")
                hist = None
            if hist is not None and not hist.empty:
                break
            time.sleep(1.0)

        if hist is None or hist.empty or len(hist) < 2:
            print(f"… {s} 无最近数据（history 为空或不足2条），跳过")
            continue

        # 价格与涨跌
        last = float(hist["Close"].iloc[-1])
        prev = float(hist["Close"].iloc[-2])
        change = last - prev
        change_pct = (last / prev - 1.0) * 100.0 if prev != 0 else 0.0

        # 成交量 / 成交额
        if "Volume" in hist.columns:
            vol = float(hist["Volume"].iloc[-1])
        else:
            vol = 0.0
        value_oku = (last * vol) / 1e8  # 亿 JPY

        # --- 流通股数：fast_info / info 双保险 ---
        shares_out = None

        # 新版 yfinance: fast_info 可能是 FastInfo 对象，没有 get 方法
        try:
            fi = getattr(t, "fast_info", None)
            if fi is not None:
                # 有些版本 fast_info 是 dict，有些是属性对象
                if isinstance(fi, dict):
                    shares_out = fi.get("shares_outstanding") or fi.get("sharesOutstanding")
                else:
                    shares_out = getattr(fi, "shares_outstanding", None) or \
                                 getattr(fi, "sharesOutstanding", None)
        except Exception as e_fi:
            print(f"⚠️ {s} fast_info 读取失败: {e_fi}")

        # 旧接口：info
        if not shares_out:
            try:
                info = getattr(t, "info", None)
                if isinstance(info, dict):
                    shares_out = info.get("sharesOutstanding") or info.get("shares_outstanding")
            except Exception as e_info:
                print(f"⚠️ {s} info 读取失败: {e_info}")
                shares_out = None

        # 换手率（可能为 None）
        turnover = None
        try:
            if shares_out and float(shares_out) > 0:
                turnover = float(vol) / float(shares_out) * 100.0
        except Exception as e_to:
            print(f"⚠️ {s} 换手率计算失败: {e_to}")
            turnover = None

        row = {
            "Symbol": s,
            "Last": round(last, 2),
            "Change": round(change, 2),
            "Change%": round(change_pct, 2),
            "Volume": int(vol),
            "Value(億JPY)": round(value_oku, 2),
            "Turnover%": round(turnover, 2) if turnover is not None else None,
        }
        rows.append(row)

        print(
            f"✅ {s} ok  收={last:.2f}  涨幅={change_pct:.2f}%  "
            f"成交额(亿)={value_oku:.2f}  换手={None if turnover is None else round(turnover,2)}%"
        )

    except Exception as e:
        # 这里包住整个单票循环，保证单票炸了不会影响其他股票
        print(f"⚠️ {s} 抓取失败: {e}")

# ---------- 汇总 & 过滤 ----------

df = pd.DataFrame(rows)
if df.empty:
    print("⚠️ 抓取结果为空（所有代码都失败或无数据）。")
    raise SystemExit(0)


def pass_threshold(row):
    """按配置过滤：涨幅、成交额、换手率（若有）。"""
    if row["Change%"] < cfg["MIN_CHANGE"]:
        return False
    if row["Value(億JPY)"] < cfg["MIN_VALUE"]:
        return False
    if row["Turnover%"] is not None and row["Turnover%"] < cfg["MIN_TURNOVER"]:
        return False
    return True


df = df[df.apply(pass_threshold, axis=1)]

if df.empty:
    print("⚠️ 所有股票被阈值过滤。")
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
