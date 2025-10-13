#!/bin/bash

# 完整的逆向投资策略分析流程
# 从数据导入到策略分析的一键运行脚本

# 项目根目录
PROJECT_ROOT="/Users/nxm/PycharmProjects/dataDig"

# 切换到项目根目录
cd "${PROJECT_ROOT}"

# 设置Python路径
export PYTHONPATH="${PROJECT_ROOT}:${PYTHONPATH}"

# 日志文件
MAIN_LOG_FILE="${PROJECT_ROOT}/logs/complete_analysis.log"

# 创建日志目录
mkdir -p "${PROJECT_ROOT}/logs"
mkdir -p "${PROJECT_ROOT}/results"

echo "=========================================="
echo "完整逆向投资策略分析流程启动"
echo "启动时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo "数据起始日期: 2024-01-01"
echo "项目路径: ${PROJECT_ROOT}"
echo "主日志文件: ${MAIN_LOG_FILE}"
echo "=========================================="

# 函数：记录日志
log_info() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "${MAIN_LOG_FILE}"
}

# 函数：检查命令执行结果
check_result() {
    if [ $? -eq 0 ]; then
        log_info "✅ $1 执行成功"
    else
        log_info "❌ $1 执行失败"
        echo "=========================================="
        echo "执行失败！请检查日志文件获取详细错误信息"
        echo "主日志文件: ${MAIN_LOG_FILE}"
        echo "=========================================="
        exit 1
    fi
}

log_info "开始完整的数据导入和策略分析流程"

# 第1步：导入股票基础信息和日线价格数据
echo ""
echo "🗃️  第1步：导入股票基础信息和日线价格数据（预计耗时：30-60分钟）"
echo "=========================================="
log_info "开始导入股票基础信息和日线价格数据"

./scripts/shell/run_full_data_ingest.sh 2024-01-01 2>&1 | tee -a "${MAIN_LOG_FILE}"
check_result "股票基础信息和日线价格数据导入"

# 第2步：导入每日指标数据（市盈率、市净率等）
echo ""
echo "📊 第2步：导入每日指标数据（预计耗时：45-90分钟）"
echo "=========================================="
log_info "开始导入每日指标数据"

./scripts/shell/run_full_daily_basic_ingest.sh 2024-01-01 2>&1 | tee -a "${MAIN_LOG_FILE}"
check_result "每日指标数据导入"

# 第3步：导入指数数据
echo ""
echo "📈 第3步：导入指数数据（预计耗时：5-10分钟）"
echo "=========================================="
log_info "开始导入指数数据"

./scripts/shell/run_index_ingest.sh 2024-01-01 2>&1 | tee -a "${MAIN_LOG_FILE}"
check_result "指数数据导入"

# 第4步：验证数据完整性
echo ""
echo "🔍 第4步：验证数据完整性"
echo "=========================================="
log_info "开始验证数据完整性"

python scripts/strategy/check_available_data.py 2>&1 | tee -a "${MAIN_LOG_FILE}"
check_result "数据完整性验证"

# 第5步：运行逆向投资策略分析
echo ""
echo "🎯 第5步：运行逆向投资策略分析"
echo "=========================================="
log_info "开始运行逆向投资策略分析"

python scripts/strategy/contrarian_strategy_analysis.py 2>&1 | tee -a "${MAIN_LOG_FILE}"
check_result "逆向投资策略分析"

# 第6步：运行通用股票筛选分析（额外福利）
echo ""
echo "💎 第6步：运行通用股票筛选分析（额外分析）"
echo "=========================================="
log_info "开始运行通用股票筛选分析"

python scripts/strategy/stock_screening_analysis.py 2>&1 | tee -a "${MAIN_LOG_FILE}"
check_result "通用股票筛选分析"

# 完成
echo ""
echo "=========================================="
echo "🎉 完整分析流程执行完成！"
echo "=========================================="
echo "执行总结："
echo "✅ 股票基础信息和日线价格数据导入完成"
echo "✅ 每日指标数据导入完成"
echo "✅ 指数数据导入完成"
echo "✅ 数据完整性验证通过"
echo "✅ 逆向投资策略分析完成"
echo "✅ 通用股票筛选分析完成"
echo ""
echo "📁 分析结果文件位置:"
echo "   ${PROJECT_ROOT}/results/"
echo ""
echo "📄 日志文件位置:"
echo "   主日志: ${MAIN_LOG_FILE}"
echo "   详细日志: ${PROJECT_ROOT}/logs/"
echo ""
echo "🎯 逆向投资策略说明:"
echo "   - 筛选条件: 大盘涨≥2%, 个股跌≥6%, 近20日涨幅≤20%"
echo "   - 分析维度: 5天和10天后的表现"
echo "   - 支持板块: 上证、深证、创业板、科创板"
echo ""
echo "💡 使用建议:"
echo "   1. 查看results目录下的CSV和TXT文件获取详细分析结果"
echo "   2. 结合市场环境和个股基本面做投资决策"
echo "   3. 设置合理的止损和仓位控制"
echo "   4. 可以定期重新运行策略分析获取最新机会"
echo "=========================================="

log_info "完整分析流程全部完成，耗时: $SECONDS 秒"
