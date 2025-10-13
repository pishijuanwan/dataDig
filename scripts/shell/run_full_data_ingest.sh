#!/bin/zsh
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)

cd "$PROJECT_ROOT"

if [ -f ".venv/bin/activate" ]; then
  source .venv/bin/activate
fi

echo "======================================"
echo "全量历史数据下载工具"
echo "======================================"
echo "用法："
echo "  $0                    # 使用配置文件默认起始日期(2020-01-01)"
echo "  $0 2020-01-01        # 指定起始日期"
echo "  $0 2020-01-01 2023-12-31  # 指定起始和结束日期"
echo "======================================"

python -m src.full_data_ingest "$@"


