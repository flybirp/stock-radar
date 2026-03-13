#!/usr/bin/env python3.10
# -*- coding: utf-8 -*-
"""
股票新闻采集脚本
读取选股结果 CSV，对每只股票抓取近期新闻，生成 JSON 数据供网站展示。

用法:
    python3.10 fetch_stock_news.py
    python3.10 fetch_stock_news.py --csv-dir /path/to/csv_dir
    python3.10 fetch_stock_news.py --top 20
"""

import os
import sys
import glob
import json
import time
import re
import argparse
import requests
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ============================================================
# 默认配置
# ============================================================

_LOCAL_CSV_DIR = os.path.expanduser(
    "~/Documents/quantTutorial/fastdtw_long114plusdays_purepricevol_ret_csv"
)
_REPO_CSV_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "csv_data")

# 优先用本地目录，找不到则回退到仓库内的 csv_data/
DEFAULT_CSV_DIR = _LOCAL_CSV_DIR if os.path.isdir(_LOCAL_CSV_DIR) else _REPO_CSV_DIR
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_JSON = os.path.join(SCRIPT_DIR, "stock_data.json")
MAX_NEWS = 8
TOP_N = 30

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36"
}


# ============================================================
# 获取股票名称 + 实时行情
# ============================================================

def get_stock_info(code: str) -> dict:
    """通过东方财富接口获取股票名称、最新价、涨跌幅"""
    info = {"name": code, "price": "", "change_pct": ""}
    try:
        secid = f"1.{code}" if code.startswith(("6", "9")) else f"0.{code}"
        url = "https://push2.eastmoney.com/api/qt/stock/get"
        params = {
            "secid": secid,
            "fields": "f57,f58,f43,f170,f44,f45,f46,f60,f47,f48,f168",
            "ut": "fa5fd1943c7b386f172d6893dbbd1d0c",
        }
        resp = requests.get(url, params=params, headers=HEADERS, timeout=5)
        data = resp.json().get("data", {})
        if data:
            info["name"] = data.get("f58", code)
            price = data.get("f43")
            if price and price != "-":
                info["price"] = str(round(price / 100, 2)) if isinstance(price, (int, float)) and price > 100 else str(price)
            change = data.get("f170")
            if change is not None and change != "-":
                val = round(change / 100, 2) if isinstance(change, (int, float)) and abs(change) > 30 else change
                info["change_pct"] = f"{val}%"
    except Exception:
        pass
    return info


# ============================================================
# 抓取新闻：东方财富股吧资讯
# ============================================================

def fetch_guba_news(code: str, max_count: int = MAX_NEWS) -> list:
    """从东方财富股吧提取个股资讯（高质量帖子）"""
    news = []
    try:
        # 抓取股吧 HTML
        url = f"https://guba.eastmoney.com/list,{code},1,f.html"
        resp = requests.get(url, headers=HEADERS, timeout=10)
        resp.encoding = "utf-8"
        html = resp.text

        # 提取链接、标题和时间
        # 模式: <a data-postid="xxx" href="/news,CODE,ID.html">TITLE</a> ... <div class="update">DATE</div>
        rows = re.findall(
            r'<tr class="listitem">(.*?)</tr>',
            html,
            re.DOTALL,
        )

        for row in rows:
            # 提取标题和链接
            title_match = re.search(
                r'class="title"><a[^>]*href="(/news,' + code + r',(\d+)\.html)"[^>]*>(.*?)</a>',
                row,
            )
            if not title_match:
                continue

            href = title_match.group(1)
            post_id = title_match.group(2)
            title = title_match.group(3).strip()

            # 过滤太短的标题（通常是水帖）
            if len(title) < 8:
                continue

            # 提取时间
            date_match = re.search(r'class="update">(.*?)</div>', row)
            date_str = date_match.group(1).strip() if date_match else ""

            # 提取作者
            author_match = re.search(r'class="author"><a[^>]*>(.*?)</a>', row)
            source = author_match.group(1).strip() if author_match else "东方财富"

            news.append({
                "title": title,
                "date": date_str,
                "source": source,
                "url": f"https://guba.eastmoney.com{href}",
            })

            if len(news) >= max_count:
                break

    except Exception as e:
        print(f"  [WARN] 抓取 {code} 股吧新闻失败: {e}")

    return news


# ============================================================
# 处理单只股票
# ============================================================

def process_stock(code: str, row: dict) -> dict:
    """处理单只股票：获取信息 + 抓新闻"""
    stock_info = get_stock_info(code)
    name = stock_info["name"]
    print(f"  {code} {name}", end="", flush=True)

    news = fetch_guba_news(code)
    print(f" -> {len(news)} 条新闻")

    time.sleep(0.3)  # 请求间隔

    return {
        "code": code,
        "name": name,
        "price": stock_info["price"],
        "change_pct": stock_info["change_pct"],
        "target_date": str(row.get("target_date", "")),
        "min_distance": round(float(row.get("min_distance", 0)), 4),
        "min_distance_name": str(row.get("min_distance_name", "")),
        "matching_period": str(row.get("matching_period", "")),
        "news": news,
    }


# ============================================================
# 主流程
# ============================================================

def find_latest_csv(csv_dir: str) -> str:
    """找到目录中日期最新的 CSV"""
    csvs = glob.glob(os.path.join(csv_dir, "*-ret.csv"))
    if not csvs:
        raise FileNotFoundError(f"在 {csv_dir} 中没有找到 *-ret.csv 文件")
    csvs.sort(reverse=True)
    return csvs[0]


def main():
    parser = argparse.ArgumentParser(description="股票新闻采集")
    parser.add_argument("--csv-dir", default=DEFAULT_CSV_DIR, help="CSV 目录路径")
    parser.add_argument("--top", type=int, default=TOP_N, help="取前 N 只股票")
    parser.add_argument("--max-news", type=int, default=MAX_NEWS, help="每只股票最多抓几条新闻")
    parser.add_argument("--output", default=OUTPUT_JSON, help="输出 JSON 路径")
    args = parser.parse_args()

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("=" * 60)
    print(f"  股票新闻采集 - {now}")
    print("=" * 60)

    # 1. 读 CSV
    csv_path = find_latest_csv(args.csv_dir)
    print(f"\n[1/3] 读取: {os.path.basename(csv_path)}")
    df = pd.read_csv(csv_path, dtype={"code": str})
    df["code"] = df["code"].str.zfill(6)
    df = df.head(args.top)
    print(f"      共 {len(df)} 只股票")

    # 2. 抓取
    print(f"\n[2/3] 抓取新闻 (max {args.max_news} 条/股)...")
    stocks = []
    for _, row in df.iterrows():
        code = row["code"]
        try:
            result = process_stock(code, row.to_dict())
            stocks.append(result)
        except Exception as e:
            print(f"  [ERROR] {code}: {e}")

    stocks.sort(key=lambda x: x["min_distance"])

    # 3. 输出
    output = {
        "generated_at": now,
        "csv_source": os.path.basename(csv_path),
        "total_stocks": len(stocks),
        "stocks": stocks,
    }

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    total_news = sum(len(s["news"]) for s in stocks)
    print(f"\n[3/3] 完成! JSON 已生成: {args.output}")
    print(f"      {len(stocks)} 只股票, {total_news} 条新闻")


if __name__ == "__main__":
    main()
