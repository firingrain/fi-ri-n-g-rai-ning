# -*- coding: utf-8 -*-
"""
generate_watchlist.py
从 jp_latest.csv 生成三份自选清单：
  - watchlist_jp.txt（综合）
  - watchlist_jp_growth.txt（成长型）
  - watchlist_jp_value.txt（高配/防御型）

设计目标：
1) 强容错：配置里的全角数字、逗号、空格、引号都能解析；缺列自动降级。
2) 零依赖外网：只依赖仓库内 jp_latest.csv。
3) 可配置：config_jp.txt 可覆盖默认参数。
"""

from __future__ import annotations
import re
import unicodedata
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd

# === 路径 ===
CSV_FILE = Path("jp_latest.csv")
CFG_FILE = Path("config_jp.txt")
OUT_MAIN = Path("watchlist_jp.txt")
OUT_GROWTH = Path("watchlist_jp_growth.txt")
OUT_VALUE = Path("watchlist_jp_value.txt")

# === 默认参数（可被 config_jp.txt 覆盖） ===
DEFAULTS: Dict[str, str] = {
    "TOP_LIMIT": "40",               # 每个清单输出前 N 个
    "MIN_PRICE": "200",              # 最低股价（日元）
    "MAX_PRICE": "20000",            # 最高股价（日元）
    "MIN_TURNOVER": "3e8",           # 最小成交额（日元）
    "SECTOR_DEFENSIVE": "Utilities, Consumer Staples, Healthcare, Telecommunications",
    "EXCLUDE_SYMBOLS": "",           # 逗号分隔，如： 1570, 1357
    "INCLUDE_SECTORS": "",           # 只选某些行业（留空表示不限）
    "EXCLUDE_SECTORS": "",           # 排除某些行业
    # 因子权重（综合榜）
    "W_CHANGE": "0.45",              # 涨跌幅（%）
    "W_TURNOVER": "0.30",            # 成交额
    "W_MCAP": "0.15",                # 市值
    "W_DIV": "0.10",                 # 股息率
    "W_PE": "-0.10",                 # 市盈率（越低越好，给负权重）
    # 成长榜权重
    "GW_CHANGE": "0.6",
    "GW_TURNOVER": "0.3",
    "GW_MCAP": "0.1",
    # 价值/防御榜权重
    "VW_DIV": "0.45",
    "VW_PE": "-0.35",
    "VW_MCAP": "0.20",
}


# ---------- 工具函数：安全数字解析 ----------
def to_halfwidth(s: str) -> str:
    return unicodedata.normalize("NFKC", s)


def parse_int(val, default: int = 0) -> int:
    if val is None:
        return default
    if isinstance(val, (int, float)):
        return int(val)
    s = to_halfwidth(str(val)).strip()
    s = s.replace(",", "").replace(" ", "")
    m = re.search(r"[-+]?\d+", s)
    return int(m.group()) if m else default


def parse_float(val, default: float = 0.0) -> float:
    if val is None:
        return default
    if isinstance(val, (int, float)):
        return float(val)
    s = to_halfwidth(str(val)).strip()
    s = s.replace("%", "")
    s = s.replace(",", "").replace(" ", "")
    s = s.replace("＋", "+").replace("－", "-")
    m = re.search(r"[-+]?\d*\.?\d+(e[-+]?\d+)?", s, re.I)
    try:
        return float(m.group()) if m else default
    except Exception:
        return default


def parse_list(val: str) -> List[str]:
    if not val:
        return []
    s = to_halfwidth(str(val))
    parts = [p.strip() for p in re.split(r"[,\u3001/|;]", s) if p.strip()]
    return parts


# ---------- 读取配置 ----------
def load_config(path: Path) -> Dict[str, str]:
    cfg = DEFAULTS.copy()
    if not path.exists():
        return cfg
    for raw in path.read_text(encoding="utf-8").splitlines():
        if not raw or raw.strip().startswith("#"):
            continue
        if "=" in raw:
            k, v = raw.split("=", 1)
        elif ":" in raw:
            k, v = raw.split(":", 1)
        else:
            continue
        k = to_halfwidth(k).strip().upper()
        v = v.strip().strip("'").strip('"')
        cfg[k] = v
    return cfg


# ---------- 列名匹配 ----------
def normalize_col(c: str) -> str:
    c2 = to_halfwidth(c).strip().lower()
    c2 = re.sub(r"[\s\-_]+", "", c2)
    return c2


ALIASES = {
    "symbol": ["code", "ticker", "銘柄コード", "証券コード", "コード", "symbol"],
    "name": ["name", "銘柄名", "会社名"],
    "price": ["price", "last", "close", "終値", "現値", "株価"],
    "change_pct": ["change%", "pctchange", "changepercent", "chg%", "前日比%", "騰落率", "変動率"],
    "change": ["change", "chg", "前日比", "値上がり", "値下がり"],
    "volume": ["volume", "出来高", "出来高株", "売買高"],
    "turnover": ["turnover", "value", "売買代金", "売買代金円"],
    "marketcap": ["marketcap", "mktcap", "時価総額", "時価総額円"],
    "pe": ["pe", "per", "per(倍)", "株価収益率"],
    "div_yield": ["dividendyield", "yield", "配当利回り"],
    "sector": ["sector", "業種", "セクター", "分類"],
}


def find_col(df: pd.DataFrame, key: str) -> Optional[str]:
    wanted = [normalize_col(x) for x in ALIASES.get(key, [])]
    mapping = {normalize_col(c): c for c in df.columns}
    for w in wanted:
        if w in mapping:
            return mapping[w]
    return None


# ---------- 读取与清洗数据 ----------
def load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise SystemExit("❌ 找不到 jp_latest.csv，请先运行 crawl_jp.py 生成。")
    df = pd.read_csv(path)
    if df.empty:
        raise SystemExit("❌ jp_latest.csv 为空，无法生成清单。")
    return df


def ensure_fields(df: pd.DataFrame) -> pd.DataFrame:
    # 基本字段
    col_sym = find_col(df, "symbol") or df.columns[0]
    col_name = find_col(df, "name") or (df.columns[1] if len(df.columns) > 1 else col_sym)
    df = df.copy()
    df.rename(columns={col_sym: "_symbol", col_name: "_name"}, inplace=True)

    # 价格
    c_price = find_col(df, "price")
    if c_price is None:
        # 尝试用 close/last 已在别名覆盖；若仍没有，置零
        df["_price"] = 0.0
    else:
        df["_price"] = df[c_price].apply(parse_float)

    # 涨跌幅（%）
    c_chg_pct = find_col(df, "change_pct")
    if c_chg_pct is not None:
        df["_chg_pct"] = df[c_chg_pct].apply(parse_float)
    else:
        # 退化为“涨跌额/价格”
        c_chg = find_col(df, "change")
        if c_chg is not None and df["_price"].abs().sum() > 0:
            df["_chg_pct"] = df[c_chg].apply(parse_float) / df["_price"] * 100.0
        else:
            df["_chg_pct"] = 0.0

    # 成交额：优先取现成 turnover；否则 price * volume
    c_tov = find_col(df, "turnover")
    if c_tov is not None:
        df["_turnover"] = df[c_tov].apply(parse_float)
    else:
        c_vol = find_col(df, "volume")
        if c_vol is not None:
            vol = df[c_vol].apply(parse_float)
            df["_turnover"] = df["_price"] * vol
        else:
            df["_turnover"] = 0.0

    # 其他因子
    c_mcap = find_col(df, "marketcap")
    df["_mcap"] = df[c_mcap].apply(parse_float) if c_mcap else 0.0
    c_pe = find_col(df, "pe")
    df["_pe"] = df[c_pe].apply(parse_float) if c_pe else 0.0
    c_div = find_col(df, "div_yield")
    df["_div"] = df[c_div].apply(parse_float) if c_div else 0.0
    c_sec = find_col(df, "sector")
    df["_sector"] = df[c_sec] if c_sec else ""

    # 标准化 symbol：保留数字或大小写字母
    df["_symbol"] = df["_symbol"].astype(str).str.strip()
    return df


# ---------- 过滤 ----------
def apply_filters(df: pd.DataFrame, cfg: Dict[str, str]) -> pd.DataFrame:
    min_price = parse_float(cfg.get("MIN_PRICE"), 0)
    max_price = parse_float(cfg.get("MAX_PRICE"), 1e12)
    min_turnover = parse_float(cfg.get("MIN_TURNOVER"), 0)

    include_secs = set([s.lower() for s in parse_list(cfg.get("INCLUDE_SECTORS", ""))])
    exclude_secs = set([s.lower() for s in parse_list(cfg.get("EXCLUDE_SECTORS", ""))])
    exclude_syms = set([s.upper() for s in parse_list(cfg.get("EXCLUDE_SYMBOLS", ""))])

    out = df[
        (df["_price"] >= min_price) &
        (df["_price"] <= max_price) &
        (df["_turnover"] >= min_turnover) &
        (~df["_symbol"].str.upper().isin(exclude_syms))
    ].copy()

    if include_secs:
        out = out[out["_sector"].astype(str).str.lower().isin(include_secs)]
    if exclude_secs:
        out = out[~out["_sector"].astype(str).str.lower().isin(exclude_secs)]

    # 去重
    out = out.drop_duplicates(subset=["_symbol"], keep="first")
    return out


# ---------- 打分与排序 ----------
def zscore(s: pd.Series) -> pd.Series:
    s = s.astype(float)
    m = s.mean()
    sd = s.std(ddof=0)
    return (s - m) / (sd if sd != 0 else 1.0)


def rank_score(s: pd.Series, ascending: bool = False) -> pd.Series:
    # 排名归一到 [0,1]
    r = s.rank(method="average", ascending=ascending)
    return (r - r.min()) / (r.max() - r.min() if r.max() != r.min() else 1.0)


def compute_score_general(df: pd.DataFrame, cfg: Dict[str, str]) -> pd.Series:
    w_change = parse_float(cfg["W_CHANGE"], 0.45)
    w_tov = parse_float(cfg["W_TURNOVER"], 0.30)
    w_mcap = parse_float(cfg["W_MCAP"], 0.15)
    w_div = parse_float(cfg["W_DIV"], 0.10)
    w_pe = parse_float(cfg["W_PE"], -0.10)

    s = (
        w_change * rank_score(df["_chg_pct"], ascending=False) +
        w_tov    * rank_score(df["_turnover"], ascending=False) +
        w_mcap   * rank_score(df["_mcap"], ascending=False) +
        w_div    * rank_score(df["_div"], ascending=False) +
        w_pe     * rank_score(df["_pe"], ascending=True)
    )
    return s


def compute_score_growth(df: pd.DataFrame, cfg: Dict[str, str]) -> pd.Series:
    w_change = parse_float(cfg["GW_CHANGE"], 0.6)
    w_tov = parse_float(cfg["GW_TURNOVER"], 0.3)
    w_mcap = parse_float(cfg["GW_MCAP"], 0.1)
    return (
        w_change * rank_score(df["_chg_pct"], ascending=False) +
        w_tov    * rank_score(df["_turnover"], ascending=False) +
        w_mcap   * rank_score(df["_mcap"], ascending=False)
    )


def compute_score_value(df: pd.DataFrame, cfg: Dict[str, str]) -> pd.Series:
    w_div = parse_float(cfg["VW_DIV"], 0.45)
    w_pe = parse_float(cfg["VW_PE"], -0.35)
    w_mcap = parse_float(cfg["VW_MCAP"], 0.20)
    return (
        w_div  * rank_score(df["_div"], ascending=False) +
        w_pe   * rank_score(df["_pe"], ascending=True) +
        w_mcap * rank_score(df["_mcap"], ascending=False)
    )


# ---------- 组装清单 ----------
def pick_top(df: pd.DataFrame, score: pd.Series, n: int) -> pd.DataFrame:
    out = df.copy()
    out["_score"] = score
    out = out.sort_values("_score", ascending=False).head(n)
    return out


def write_watchlist(path: Path, title: str, picked: pd.DataFrame, cfg: Dict[str, str]) -> None:
    path.write_text("", encoding="utf-8")  # 清空
    ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    header = [
        f"# {title}",
        f"# generated at {ts} (UTC)",
        f"# filters: MIN_PRICE={cfg.get('MIN_PRICE')}  MAX_PRICE={cfg.get('MAX_PRICE')}  MIN_TURNOVER={cfg.get('MIN_TURNOVER')}",
        f"# total={len(picked)}",
        "# ---- symbols below ----",
    ]
    with path.open("w", encoding="utf-8") as f:
        f.write("\n".join(header) + "\n")
        for _, row in picked.iterrows():
            sym = str(row["_symbol"]).strip()
            f.write(sym + "\n")


def main() -> None:
    cfg = load_config(CFG_FILE)
    top_n = max(1, parse_int(cfg.get("TOP_LIMIT"), 40))

    df = load_csv(CSV_FILE)
    df = ensure_fields(df)
    df = apply_filters(df, cfg)

    if df.empty:
        # 也写出空清单，但不报错，方便流水线继续
        for p, t in [(OUT_MAIN, "JP watchlist (general)"),
                     (OUT_GROWTH, "JP watchlist (growth)"),
                     (OUT_VALUE, "JP watchlist (value/defensive)")]:
            write_watchlist(p, t, df, cfg)
        print("⚠️ 过滤后无候选，已写出空清单。")
        return

    # 综合清单
    score_g = compute_score_general(df, cfg)
    top_g = pick_top(df, score_g, top_n)
    write_watchlist(OUT_MAIN, "JP watchlist (general)", top_g, cfg)

    # 成长清单（可选：排除防御行业）
    defensive = {s.lower() for s in parse_list(cfg.get("SECTOR_DEFENSIVE", ""))}
    df_growth = df[~df["_sector"].astype(str).str.lower().isin(defensive)] if defensive else df
    score_gr = compute_score_growth(df_growth, cfg)
    top_gr = pick_top(df_growth, score_gr, top_n)
    write_watchlist(OUT_GROWTH, "JP watchlist (growth)", top_gr, cfg)

    # 价值/防御清单（可选：只保留防御行业，如果定义了）
    df_value = df[df["_sector"].astype(str).str.lower().isin(defensive)] if defensive else df
    score_v = compute_score_value(df_value, cfg)
    top_v = pick_top(df_value, score_v, top_n)
    write_watchlist(OUT_VALUE, "JP watchlist (value/defensive)", top_v, cfg)

    print(f"✅ watchlists generated: {OUT_MAIN}, {OUT_GROWTH}, {OUT_VALUE}")


if __name__ == "__main__":
    main()
