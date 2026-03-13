#!/bin/bash
# ============================================
# 一键更新数据 & 启动本地网站
# 用法: bash update_and_serve.sh
# ============================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PORT=8765

echo "======================================"
echo "  选股雷达 - 数据更新 & 网站启动"
echo "======================================"

# 1. 运行新闻采集脚本
echo ""
echo "[1] 采集股票新闻数据..."
cd "$SCRIPT_DIR"
python3.10 fetch_stock_news.py --top 30 --max-news 8

if [ $? -ne 0 ]; then
    echo "[ERROR] 新闻采集失败!"
    exit 1
fi

# 2. 停掉旧的 HTTP 服务（如果有）
echo ""
echo "[2] 启动本地服务器 (端口 $PORT)..."
lsof -ti :$PORT | xargs kill -9 2>/dev/null

# 3. 启动新的 HTTP 服务
cd "$SCRIPT_DIR"
python3.10 -m http.server $PORT &
SERVER_PID=$!

echo ""
echo "======================================"
echo "  网站已启动!"
echo "  访问: http://localhost:$PORT"
echo "  进程: $SERVER_PID"
echo "  停止: kill $SERVER_PID"
echo "======================================"

# 如果想前台运行，取消下面注释:
# wait $SERVER_PID
