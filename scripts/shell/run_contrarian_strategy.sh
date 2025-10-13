#!/bin/bash

# 逆向投资策略分析脚本启动器
# 分析大盘上涨时被错杀的个股

# 项目根目录
PROJECT_ROOT="/Users/nxm/PycharmProjects/dataDig"

# 切换到项目根目录
cd "${PROJECT_ROOT}"

# 设置Python路径
export PYTHONPATH="${PROJECT_ROOT}:${PYTHONPATH}"

# 日志文件
LOG_FILE="${PROJECT_ROOT}/logs/contrarian_strategy_analysis.log"

# 创建日志目录（如果不存在）
mkdir -p "${PROJECT_ROOT}/logs"
mkdir -p "${PROJECT_ROOT}/results"

echo "=========================================="
echo "逆向投资策略分析工具启动"
echo "启动时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo "项目路径: ${PROJECT_ROOT}"
echo "日志文件: ${LOG_FILE}"
echo "=========================================="

# 执行逆向投资策略分析
echo "[$(date '+%Y-%m-%d %H:%M:%S')] 开始执行逆向投资策略分析..." | tee -a "${LOG_FILE}"

python3 "${PROJECT_ROOT}/scripts/strategy/contrarian_strategy_analysis.py" 2>&1 | tee -a "${LOG_FILE}"

# 检查执行结果
if [ $? -eq 0 ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] 逆向投资策略分析执行成功" | tee -a "${LOG_FILE}"
    echo "=========================================="
    echo "执行完成！结果文件已保存到 results/ 目录"
    echo "日志文件: ${LOG_FILE}"
    echo "=========================================="
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] 逆向投资策略分析执行失败" | tee -a "${LOG_FILE}"
    echo "=========================================="
    echo "执行失败！请检查日志文件获取详细错误信息"
    echo "日志文件: ${LOG_FILE}"
    echo "=========================================="
    exit 1
fi
