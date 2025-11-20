# -*- coding: utf-8 -*-
"""
generate_watchlist.py — JP long-term + short-term unified screener (A+B final)

A = crawl_jp 动能体系（Change%、Turnover%、Value、MOM5%）
B = 原 generate_watchlist 长期价值/成长体系

本版融合两套体系，正式启用短线动能 MOM5%
并扩展 3 套评分体系：
 - 综合榜：短线 + 中线 + 长线因子
 - 成长榜：强化动能 + 成交额 + 规模
 - 价值榜：仍主要围绕估值（不加入 MOM）
"""

from __future__ import annotations
import re
import unicodedata
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
import pandas as pd


# === 文件路径 ===
CSV_FILE = Path("jp_latest.csv")
CFG_FILE = Path("config_jp.txt")
OUT_MAIN = Path("watchlist_jp.txt")
OUT_GROWTH = Path("watchlist_jp_growth.txt")
OUT_VALUE = Path("watchlist_jp_value.txt")


# === 默认参数（新增短线动能权重） ===
DEFAULTS: Dict[str, str] = {
    "TOP_LIMIT": "40",

    # -------- 基本过滤 --------
    "MIN_PRICE": "200",
    "MAX_PRICE": "20000",
    "MIN_TURNOVER": "3e8",

    # -------- 行业/排除 --------
    "SECTOR_DEFENSIVE": "Utilities, Consumer Staples, Healthcare, Telecommunications",
    "EXCLUDE_SYMBOLS": "",
    "INCLUDE_SECTORS": "",
    "EXCLUDE_SECTORS": "",

    # -------- 综合榜权重（加入短线动能） --------
    "W_CHANGE": "0.35",
    "W_TURNOVER": "0.25",
    "W_MCAP": "0.15",
    "W_DIV": "0.10",
    "W_PE": "-0.05",
    "W_MOM5": "0.20",   # ★ 动能关键

    # -------- 成长榜权重（加入短线动能） --------
    "GW_CHANGE": "0.40",
    "GW_TURNOVER": "0.30",
    "GW_MCAP": "0.10",
    "GW_MOM5": "0.20",  # ★ 动能关键

    # -------- 价值榜不使用 MOM --------
    "VW_DIV": "0.45",
    "VW_PE": "-0.35",
    "VW_MCAP": "0.20",
}


# -------- 工具函数 --------
def to_halfwidth(s: str) -> str:
    return unicodedata.normalize("NFKC", s)


def parse_float(val, default: float = 0.0) -> float:
    if val is None:
        return default
    if isinstance(val, (int, float)):
        return float(val)
    s = to_halfwidth(str(val)).strip()
    s = s.replace("%", "").replace(",", "").replace(" ", "")
    s = s.replace("＋", "+").replace("－", "-")
    m = re.search(r"[-+]?\d*\.?\d+(e[-+]?\d+)?", s, re.I)
    try:
        return float(m.group()) if m else default
    except Exception:
        return default


def parse_int(val, default: int = 0) -> int:
    if val is None:
        return default
    if isinstance(val, (int, float)):
        return int(val)
    s = to_halfwidth(str(val)).replace(",", "")
    m = re.search(r"[-+]?\d+", s)
    return int(m.group()) if m else default


def parse_list(val: str) -> List[str]:
    if not val:
        return []
    s = to_halfwidth(str(val))
    parts = [p.strip() for p in re.split(r"[,\u3001/|;]", s) if p.strip()]
    return parts


# -------- 读取配置 --------
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


# -------- 列名匹配 --------
def normalize_col(c: str) -> str:
    c2 = to_halfwidth(c).strip().lower()
    return re.sub(r"[\s\-_]+", "", c2)


ALIASES = {
    "symbol": ["code", "ticker", "symbol", "銘柄コード"],
    "price": ["price", "last", "close"],
    "change_pct": ["change%", "chg%", "前日比%", "騰落率"],
    "change": ["change", "前日比"],
    "volume": ["volume", "出来高"],
    "turnover": ["value", "turnover", "売買代金"],
    "marketcap": ["marketcap", "時価総額"],
    "pe": ["pe", "per"],
    "div_yield": ["dividendyield", "配当利回り"],
    "sector": ["sector", "業種"],
    "mom5": ["mom5%", "mom5"],   # ★ 动能
}


def find_col(df: pd.DataFrame, key: str) -> Optional[str]:
    wanted = [normalize_col(x) for x in ALIASES.get(key, [])]
    mapping = {normalize_col(c): c for c in df.columns}
    for w in wanted:
        if w in mapping:
            return mapping[w]
    return None


# -------- 读取 CSV --------
def load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise SystemExit("❌ 找不到 jp_latest.csv，请先运行 crawl_jp.py")
    df = pd.read_csv(path)
    if df.empty:
        raise SystemExit("❌ jp_latest.csv 为空")
    return df


# -------- 字段标准化 --------
def ensure_fields(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # symbol
    col_sym = find_col(df, "symbol") or df.columns[0]
    df.rename(columns={col_sym: "_symbol"}, inplace=True)
    df["_symbol"] = df["_symbol"].astype(str).str.strip()

    # price
    price_col = find_col(df, "price")
    df["_price"] = df[price_col].apply(parse_float) if price_col else 0.0

    # change%
    pct_col = find_col(df, "change_pct")
    if pct_col:
        df["_chg_pct"] = df[pct_col].apply(parse_float)
    else:
        chg = find_col(df, "change")
        df["_chg_pct"] = df[chg].apply(parse_float) / df["_price"] * 100 if chg else 0.0

    # turnover
    tov = find_col(df, "turnover")
    if tov:
        df["_turnover"] = df[tov].apply(parse_float)
    else:
        vol_col = find_col(df, "volume")
        vol = df[vol_col].apply(parse_float) if vol_col else 0
        df["_turnover"] = df["_price"] * vol

    # mcap / pe / div
    mcap = find_col(df, "marketcap")
    df["_mcap"] = df[mcap].apply(parse_float) if mcap else 0.0

    pe = find_col(df, "pe")
    df["_pe"] = df[pe].apply(parse_float) if pe else 0.0

    div = find_col(df, "div_yield")
    df["_div"] = df[div].apply(parse_float) if div else 0.0

    # sector
    sec = find_col(df, "sector")
    df["_sector"] = df[sec] if sec else ""

    # MOM5（关键）
    mom5col = find_col(df, "mom5")
    df["_mom5"] = df[mom5col].apply(parse_float) if mom5col else 0.0

    return df


# -------- 过滤 --------
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

    return out.drop_duplicates(subset=["_symbol"], keep="first")


# -------- 排名函数 --------
def rank_score(s: pd.Series, ascending: bool = False) -> pd.Series:
    r = s.rank(method="average", ascending=ascending)
    return (r - r.min()) / (r.max() - r.min() if r.max() != r.min() else 1.0)


# -------- 综合榜（General）★ 加入 MOM5 --------
def compute_score_general(df: pd.DataFrame, cfg: Dict[str, str]) -> pd.Series:
    w_change = parse_float(cfg["W_CHANGE"], 0.35)
    w_tov    = parse_float(cfg["W_TURNOVER"], 0.25)
    w_mcap   = parse_float(cfg["W_MCAP"], 0.15)
    w_div    = parse_float(cfg["W_DIV"], 0.10)
    w_pe     = parse_float(cfg["W_PE"], -0.05)
    w_mom5   = parse_float(cfg["W_MOM5"], 0.20)

    return (
        w_change * rank_score(df["_chg_pct"], ascending=False) +
        w_tov    * rank_score(df["_turnover"], ascending=False) +
        w_mcap   * rank_score(df["_mcap"], ascending=False) +
        w_div    * rank_score(df["_div"], ascending=False) +
        w_pe     * rank_score(df["_pe"], ascending=True) +
        w_mom5   * rank_score(df["_mom5"], ascending=False)
    )


# -------- 成长榜（Growth）★ 加入 MOM5 --------
def compute_score_growth(df: pd.DataFrame, cfg: Dict[str, str]) -> pd.Series:
    w_change = parse_float(cfg["GW_CHANGE"], 0.40)
    w_tov    = parse_float(cfg["GW_TURNOVER"], 0.30)
    w_mcap   = parse_float(cfg["GW_MCAP"], 0.10)
    w_mom5   = parse_float(cfg["GW_MOM5"], 0.20)

    return (
        w_change * rank_score(df["_chg_pct"], ascending=False) +
        w_tov    * rank_score(df["_turnover"], ascending=False) +
        w_mcap   * rank_score(df["_mcap"], ascending=False) +
        w_mom5   * rank_score(df["_mom5"], ascending=False)
    )


# -------- 价值榜（Value）★ 保持原样，无 MOM5 --------
def compute_score_value(df: pd.DataFrame, cfg: Dict[str, str]) -> pd.Series:
    w_div  = parse_float(cfg["VW_DIV"], 0.45)
    w_pe   = parse_float(cfg["VW_PE"], -0.35)
    w_mcap = parse_float(cfg["VW_MCAP"], 0.20)

    return (
        w_div  * rank_score(df["_div"], ascending=False) +
        w_pe   * rank_score(df["_pe"], ascending=True) +
        w_mcap * rank_score(df["_mcap"], ascending=False)
    )


# -------- 取前 N --------
def pick_top(df: pd.DataFrame, score: pd.Series, n: int) -> pd.DataFrame:
    out = df.copy()
    out["_score"] = score
    return out.sort_values("_score", ascending=False).head(n)


# -------- 输出 --------
def write_watchlist(path: Path, title: str, picked: pd.DataFrame, cfg: Dict[str, str]) -> None:
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
            f.write(str(row["_symbol"]).strip() + "\n")


# -------- 主入口 --------
def main() -> None:
    cfg = load_config(CFG_FILE)
    top_n = max(1, parse_int(cfg.get("TOP_LIMIT"), 40))

    df = load_csv(CSV_FILE)
    df = ensure_fields(df)
    df = apply_filters(df, cfg)

    if df.empty:
        for p, t in [
            (OUT_MAIN, "JP watchlist (general)"),
            (OUT_GROWTH, "JP watchlist (growth)"),
            (OUT_VALUE, "JP watchlist (value/defensive)")
        ]:
            write_watchlist(p, t, df, cfg)
        print("⚠️ 无候选，已写出空列表")
        return

    # 综合榜
    score_g = compute_score_general(df, cfg)
    write_watchlist(OUT_MAIN, "JP watchlist (general)", pick_top(df, score_g, top_n), cfg)

    # 成长榜：排除防御行业
    defensive = {s.lower() for s in parse_list(cfg.get("SECTOR_DEFENSIVE", ""))}
    df_growth = df[~df["_sector"].astype(str).str.lower().isin(defensive)] if defensive else df
    score_gr = compute_score_growth(df_growth, cfg)
    write_watchlist(OUT_GROWTH, "JP watchlist (growth)", pick_top(df_growth, score_gr, top_n), cfg)

    # 价值榜：保留防御行业
    df_value = df[df["_sector"].astype(str).str.lower().isin(defensive)] if defensive else df
    score_v = compute_score_value(df_value, cfg)
    write_watchlist(OUT_VALUE, "JP watchlist (value/defensive)", pick_top(df_value, score_v, top_n), cfg)

    print("✅ watchlists generated")


if __name__ == "__main__":
    main()
