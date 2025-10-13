#!/bin/bash

# 股票筛选分析脚本启动器
# 运行股票筛选和表现分析工具

# 项目根目录
PROJECT_ROOT="/Users/nxm/PycharmProjects/dataDig"

# 切换到项目根目录
cd "${PROJECT_ROOT}"

# 设置Python路径
export PYTHONPATH="${PROJECT_ROOT}:${PYTHONPATH}"

# 日志文件
LOG_FILE="${PROJECT_ROOT}/logs/stock_screening_analysis.log"

# 创建日志目录（如果不存在）
mkdir -p "${PROJECT_ROOT}/logs"
mkdir -p "${PROJECT_ROOT}/results"

echo "=========================================="
echo "股票筛选和表现分析工具启动"
echo "启动时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo "项目路径: ${PROJECT_ROOT}"
echo "日志文件: ${LOG_FILE}"
echo "=========================================="

# 执行股票筛选分析
echo "[$(date '+%Y-%m-%d %H:%M:%S')] 开始执行股票筛选分析..." | tee -a "${LOG_FILE}"

python3 "${PROJECT_ROOT}/scripts/strategy/stock_screening_analysis.py" 2>&1 | tee -a "${LOG_FILE}"

# 检查执行结果
if [ $? -eq 0 ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] 股票筛选分析执行成功" | tee -a "${LOG_FILE}"
    echo "=========================================="
    echo "执行完成！结果文件已保存到 results/ 目录"
    echo "日志文件: ${LOG_FILE}"
    echo "=========================================="
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] 股票筛选分析执行失败" | tee -a "${LOG_FILE}"
    echo "=========================================="
    echo "执行失败！请检查日志文件获取详细错误信息"
    echo "日志文件: ${LOG_FILE}"
    echo "=========================================="
    exit 1
fi
