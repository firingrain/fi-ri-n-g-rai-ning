import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime

# === 读取配置参数 ===
params = {}
with open("config_jp.txt", "r", encoding="utf-8") as f:
    for line in f:
        if "=" in line and not line.strip().startswith("#"):
            k, v = [x.strip() for x in line.split("=")]
            try:
                params[k] = float(v)
            except:
                params[k] = v

MIN_CHANGE = params.get("MIN_CHANGE", 3)
MIN_TURNOVER = params.get("MIN_TURNOVER", 5)
MIN_VALUE = params.get("MIN_VALUE", 5)
TOP_LIMIT = int(params.get("TOP_LIMIT", 20))

# === 数据源（Yahoo!ファイナンス 値上がり率ランキング） ===
url = "https://finance.yahoo.co.jp/ranking/price_up"
r = requests.get(url, timeout=10)
r.raise_for_status()
soup = BeautifulSoup(r.text, "html.parser")

rows = soup.select("table tbody tr")
watchlist = []

for tr in rows:
    tds = tr.select("td")
    if len(tds) < 5:
        continue

    code = tds[1].get_text(strip=True)
    change_text = tds[3].get_text(strip=True).replace("%", "").replace("+", "")
    value_text = tds[4].get_text(strip=True).replace(",", "")

    try:
        change = float(change_text)
    except:
        change = 0.0
    try:
        value = float(re.findall(r"\d+", value_text)[0]) / 1e8  # 转换成亿日元
    except:
        value = 0.0

    # 筛选逻辑（根据 config_jp.txt）
    if change >= MIN_CHANGE and value >= MIN_VALUE:
        watchlist.append(code)

# 只保留前 TOP_LIMIT 只股票
watchlist = watchlist[:TOP_LIMIT]

# 写入 watchlist_jp.txt
with open("watchlist_jp.txt", "w", encoding="utf-8") as f:
    f.write("# Auto-generated watchlist\n")
    f.write(f"# {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    for c in watchlist:
        f.write(f"{c}.T\n")

print(f"✅ Generated {len(watchlist)} symbols into watchlist_jp.txt")
