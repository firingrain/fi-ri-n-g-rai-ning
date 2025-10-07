# analyze_jp.py
# 读取 jp_latest.csv → 依据涨跌幅分三档：Buy / Watch / Avoid
# 输出文字报告到 jp_analysis.txt

import csv
from datetime import datetime

def parse_change_pct(s: str) -> float:
    if s is None:
        return 0.0
    s = str(s).strip().replace('%', '').replace('+', '')
    # 兼容 "−" 或全角符号
    s = s.replace('－', '-').replace('−', '-')
    try:
        return float(s)
    except:
        return 0.0

rows = []
try:
    with open("jp_latest.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            r["Change%_num"] = parse_change_pct(r.get("Change%"))
            rows.append(r)
except FileNotFoundError:
    with open("jp_analysis.txt", "w", encoding="utf-8") as f:
        f.write("⚠️ 未找到 jp_latest.csv，请先运行抓取脚本。\n")
    raise SystemExit(0)

# 规则（可按需微调）
buy    = [r for r in rows if r["Change%_num"] >= 3.0]
watch  = [r for r in rows if 0.0 <= r["Change%_num"] < 3.0]
avoid  = [r for r in rows if r["Change%_num"] < 0.0]

# 排序：按涨幅降序
buy.sort(key=lambda x: x["Change%_num"], reverse=True)
watch.sort(key=lambda x: x["Change%_num"], reverse=True)
avoid.sort(key=lambda x: x["Change%_num"])

now_str = datetime.now().strftime("%Y-%m-%d %H:%M")

with open("jp_analysis.txt", "w", encoding="utf-8") as f:
    f.write(f"📊 JP Market Analysis — {now_str}\n")
    f.write("（标准：Change% ≥ +3 → Buy，0~3 → Watch，<0 → Avoid）\n\n")

    def section(title, data):
        f.write(title + "\n")
        if not data:
            f.write("  - （空）\n\n")
            return
        for r in data:
            f.write(f"  - {r.get('Symbol','?'):>8}  "
                    f"{r.get('Price','N/A'):>8}  "
                    f"{r.get('Change','N/A'):>8}  "
                    f"{r.get('Change%','N/A'):>8}\n")
        f.write("\n")

    section("✅ 【Buy Candidates】（强势可买）", buy[:15])
    section("⚠️ 【Watchlist】（观察）", watch[:20])
    section("❌ 【Avoid】（暂不建议）", avoid[:20])

print("✅ Analysis complete → jp_analysis.txt")
