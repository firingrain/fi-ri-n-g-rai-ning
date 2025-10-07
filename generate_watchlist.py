# -*- coding: utf-8 -*-
"""
generate_watchlist.py
从 jp_latest.csv 读取数据，按 Change% 排序，生成 watchlist_jp.txt
✔ 自动读取 TOP_LIMIT
✔ 自动忽略全角数字、空格、注释、中文说明
✔ 没数据也不会报错
"""

import pandas as pd
from datetime import datetime
import pytz
from pathlib import Path
import re, unicodedata

CSV_FILE = "jp_latest.csv"
OUT_FILE = "watchlist_jp.txt"
CFG_FILE = "config_jp.txt"

# ---------- 通用安全转换函数 ----------
def to_number_safe(s, default=0.0):
    """自动将字符串转为 float/int，兼容全角、空格、注释"""
    if s is None:
        return default
    s = unicodedata.normalize("NFKC", str(s))
    s = s.split("#", 1)[0].strip()
    if not s:
        return default
    try:
        if "." in s:
            return float(s)
        return float(int(s))
    except Exception:
        return default

# ---------- 读取配置中的 TOP_LIMIT ----------
def load_top_limit():
    top = 5
    p = Path(CFG_FILE)
    if not p.exists():
        return top
    pat = re.compile(r"^\s*TOP_LIMIT\s*=\s*(.+)$")
    for raw in p.read_text(encoding="utf-8").splitlines():
        m = pat.match(raw)
        if m:
            val = to_number_safe(m.group(1), top)
            try:
                return max(1, int(val))
            except Exception:
                return top
    return top

# ---------- 读取行情数据 ----------
if not Path(CSV_FILE).exists():
    print(f"⚠️ 找不到 {CSV_FILE}，请先运行 crawl_jp.py")
    raise SystemExit(1)

df = pd.read_csv(CSV_FILE)
if df.empty:
    print("⚠️ jp_latest.csv 没有数据。")
    raise SystemExit(1)

# ---------- 生成自选榜 ----------
topn = load_top_limit()
df_sorted = df.sort_values(by="Change%", ascending=False).head(topn)

tokyo = pytz.timezone("Asia/Tokyo")
timestamp = datetime.now(tokyo).strftime("%Y-%m-%d %H:%M:%S")

lines = [f"# Top {topn} Gainers ({timestamp})"]
for _, row in df_sorted.iterrows():
    chg = row.get("Change%", None)
    if pd.isna(chg):
        chg = row.get("ChangePct", None)
    lines.append(f"{row['Symbol']}  +{chg}%")

Path(OUT_FILE).write_text("\n".join(lines), encoding="utf-8")
print(f"✅ 已生成 {OUT_FILE}")
