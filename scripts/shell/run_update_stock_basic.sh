#!/bin/bash

# 股票基础信息增强脚本
# 用于获取并更新股票的总股本、流通股、总市值、流通市值信息

# 获取脚本所在目录的父目录的父目录（即项目根目录）
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] 开始执行股票基础信息增强脚本"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] 项目根目录: ${PROJECT_ROOT}"

# 切换到项目根目录
cd "${PROJECT_ROOT}" || exit 1

# 检查Python脚本是否存在
if [ ! -f "update_stock_basic.py" ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] 错误: update_stock_basic.py 文件不存在"
    exit 1
fi

# 安装依赖（如果需要）
echo "[$(date '+%Y-%m-%d %H:%M:%S')] 检查并安装依赖包..."
pip install -q -r requirements.txt

# 执行股票基础信息增强
echo "[$(date '+%Y-%m-%d %H:%M:%S')] 开始执行股票基础信息增强..."

# 默认参数：批处理大小50，调用间隔0.8秒
python update_stock_basic.py \
    --batch-size 50 \
    --sleep-interval 0.8 \
    2>&1 | tee -a "logs/update_stock_basic.log"

# 检查执行结果
if [ $? -eq 0 ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] 股票基础信息增强完成"
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] 股票基础信息增强执行失败"
    exit 1
fi

echo "[$(date '+%Y-%m-%d %H:%M:%S')] 脚本执行结束"
