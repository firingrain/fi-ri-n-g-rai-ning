# -*- coding: utf-8 -*-
"""
generate_watchlist.py — 从 jp_latest.csv 生成 watchlist_jp.txt
✔ 读取 TOP_LIMIT（容错）
✔ 强制数值化 Change%（防止字符串/NaN）
✔ 空表友好退出
"""
import re, unicodedata
from pathlib import Path
from datetime import datetime
import pandas as pd
import pytz

CSV_FILE, OUT_FILE, CFG_FILE = "jp_latest.csv", "watchlist_jp.txt", "config_jp.txt"

def to_number_safe(s, default=0.0):
    if s is None: return default
    s = unicodedata.normalize("NFKC", str(s)).split("#",1)[0].strip()
    if not s: return default
    try:
        if "." in s: return float(s)
        return float(int(s))
    except Exception:
        return default

def load_top_limit(default=5):
    p = Path(CFG_FILE)
    if not p.exists(): return default
    pat = re.compile(r"^\s*TOP_LIMIT\s*=\s*(.+)$")
    for raw in p.read_text(encoding="utf-8").splitlines():
        m = pat.match(raw)
        if m:
            val = to_number_safe(m.group(1), default)
            try: return max(1, int(val))
            except Exception: return default
    return default

if not Path(CSV_FILE).exists():
    print("⚠️ 缺少 jp_latest.csv，先运行 crawl_jp.py"); raise SystemExit(0)

df = pd.read_csv(CSV_FILE)
if df.empty:
    print("⚠️ jp_latest.csv 为空，跳过生成自选。"); raise SystemExit(0)

# 强制把 Change% 转成数值
if "Change%" not in df.columns:
    print("⚠️ 缺少 Change% 列，跳过生成自选。"); raise SystemExit(0)
df["Change%"] = pd.to_numeric(df["Change%"], errors="coerce")
df = df.dropna(subset=["Change%"])
if df.empty:
    print("⚠️ Change% 全为无效值，跳过生成自选。"); raise SystemExit(0)

topn = load_top_limit(5)
df = df.sort_values(by="Change%", ascending=False).head(topn)

tokyo = pytz.timezone("Asia/Tokyo")
ts = datetime.now(tokyo).strftime("%Y-%m-%d %H:%M:%S")

lines = [f"# Top {topn} Gainers ({ts})"]
for _, r in df.iterrows():
    lines.append(f"{r['Symbol']}  +{round(float(r['Change%']),2)}%")
Path(OUT_FILE).write_text("\n".join(lines), encoding="utf-8")
print(f"✅ 已生成 {OUT_FILE}")
