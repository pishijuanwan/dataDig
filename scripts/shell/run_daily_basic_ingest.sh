#!/bin/zsh
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)

cd "$PROJECT_ROOT"

if [ -f ".venv/bin/activate" ]; then
  source .venv/bin/activate
fi

echo "======================================"
echo "Tushare每日指标数据下载工具（增量模式）"
echo "======================================"
echo "功能：下载股票的每日基本面指标数据"
echo "包含：市盈率、市净率、换手率、总市值等指标"
echo "模式：增量模式，从库内最大交易日续拉"
echo "======================================"

python -m src.daily_basic_app


