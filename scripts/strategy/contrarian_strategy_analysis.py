#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
逆向投资策略分析脚本

筛选条件：
1. 对应盘面涨幅超过2%
2. 个股下跌6%以上
3. 个股在最近20个交易日涨幅低于20%

分析这些被错杀的股票在未来5个和10个交易日的表现

用法示例:
python contrarian_strategy_analysis.py
"""

import sys
import os
from datetime import datetime, timedelta
from typing import List

# 添加项目根路径到Python路径
sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))

from src.config.settings import load_settings
from src.db.mysql_client import MySQLClient
from src.app_logging.logger import get_logger
from src.services.stock_screener_service import StockScreenerService, ContrarianCondition


def get_recent_trading_dates_with_volume(mysql_client, days_ago: int = 1, required_days: int = 15) -> List[str]:
    """获取最近有交易量的交易日期列表"""
    logger = get_logger(__name__)
    
    with mysql_client.get_session() as session:
        from src.models.daily_price import DailyPrice
        from sqlalchemy import select, func, and_
        
        # 查询最近有足够交易量的交易日期
        stmt = select(DailyPrice.trade_date.distinct()).where(
            DailyPrice.vol > 0  # 确保有成交量
        ).order_by(DailyPrice.trade_date.desc()).limit(days_ago + required_days)
        
        result = session.execute(stmt).scalars().all()
        
        if len(result) > days_ago:
            selected_dates = result[days_ago:days_ago + required_days]
            logger.info(f"[获取交易日] 选择从第{days_ago}个交易日开始的{len(selected_dates)}个交易日")
            return list(selected_dates)
        elif result:
            logger.info(f"[获取交易日] 交易日不足，返回所有可用的{len(result)}个交易日")
            return list(result)
        else:
            logger.error("[获取交易日] 数据库中没有找到有效的交易日期")
            return []


def get_suitable_screening_date(mysql_client) -> str:
    """获取适合进行逆向投资筛选的日期"""
    logger = get_logger(__name__)
    
    # 获取最近的交易日期，确保有后续数据用于分析
    trading_dates = get_recent_trading_dates_with_volume(mysql_client, days_ago=15, required_days=20)
    
    if not trading_dates:
        logger.error("[选择筛选日期] 无法获取合适的交易日期")
        return ""
    
    # 选择第一个日期作为筛选日期
    screening_date = trading_dates[0]
    logger.info(f"[选择筛选日期] 选择{screening_date}作为筛选日期")
    
    return screening_date


def analyze_contrarian_strategy():
    """执行逆向投资策略分析"""
    logger = get_logger(__name__)
    logger.info("[逆向策略分析开始] 开始执行逆向投资策略分析")
    
    # 初始化服务
    settings = load_settings()
    mysql_client = MySQLClient(
        host=settings.database.host,
        port=settings.database.port,
        user=settings.database.user,
        password=settings.database.password,
        db_name=settings.database.name
    )
    
    with mysql_client.get_session() as session:
        screener = StockScreenerService(session, logger)
        
        # 获取合适的筛选日期
        screening_date = get_suitable_screening_date(mysql_client)
        if not screening_date:
            logger.error("[逆向策略失败] 无法获取合适的筛选日期")
            return
        
        print(f"\\n========== 逆向投资策略分析 ==========")
        print(f"策略描述: 寻找大盘上涨时被错杀的个股")
        print(f"筛选日期: {screening_date}")
        print("="*50)
        
        # 设置筛选条件
        contrarian_condition = ContrarianCondition(
            screener_service=screener,
            screening_date=screening_date,
            min_index_rise=2.0,      # 对应指数涨幅≥2%
            max_stock_fall=-6.0,     # 个股跌幅≤-6%（即跌6%以上）
            max_historical_rise=20.0, # 近20日涨幅≤20%
            historical_days=20
        )
        
        conditions = [contrarian_condition]
        condition_desc = "逆向策略：大盘涨≥2%，个股跌≥6%，近20日涨幅≤20%"
        
        # 执行筛选
        logger.info(f"[逆向策略筛选] 开始执行筛选，条件: {condition_desc}")
        screened_stocks = screener.screen_stocks(
            screening_date=screening_date,
            conditions=conditions,
            market_filter=None  # 不限制市场，包含所有板块
        )
        
        if screened_stocks.empty:
            print("\\n❌ 未找到符合逆向投资条件的股票")
            print("可能原因:")
            print("1. 当日大盘未出现足够的上涨")
            print("2. 没有个股出现足够的下跌")
            print("3. 符合条件的股票近期涨幅过大")
            logger.warning("[逆向策略结果] 没有股票符合逆向投资条件")
            return
        
        print(f"\\n✅ 筛选结果: 找到 {len(screened_stocks)} 只符合条件的股票")
        
        # 显示筛选结果详情
        print("\\n========== 筛选股票详情 ==========")
        display_cols = ['ts_code', 'name', 'board', 'pct_chg', 'corresponding_index', 'index_pct_chg', 'historical_performance']
        available_cols = [col for col in display_cols if col in screened_stocks.columns]
        
        # 重新排序以便更好显示
        screened_display = screened_stocks[available_cols].copy()
        screened_display['pct_chg'] = screened_display['pct_chg'].round(2)
        screened_display['index_pct_chg'] = screened_display['index_pct_chg'].round(2)
        screened_display['historical_performance'] = screened_display['historical_performance'].round(2)
        
        # 按个股跌幅排序（跌幅越大排越前）
        screened_display = screened_display.sort_values('pct_chg')
        
        print(screened_display.to_string(index=False))
        
        # 按板块统计
        if 'board' in screened_stocks.columns:
            board_stats = screened_stocks.groupby('board').agg({
                'ts_code': 'count',
                'pct_chg': 'mean',
                'index_pct_chg': 'mean'
            }).round(2)
            board_stats.columns = ['股票数量', '平均个股跌幅%', '平均指数涨幅%']
            
            print("\\n========== 板块分布统计 ==========")
            print(board_stats)
        
        # 分析5天后表现
        print("\\n========== 5个交易日后表现分析 ==========")
        logger.info("[逆向策略表现分析] 开始分析筛选股票5天后的表现")
        
        performance_5d = screener.analyze_performance(
            screened_stocks=screened_stocks,
            screening_date=screening_date,
            analysis_days=5,
            condition_description=condition_desc + " (5日后表现)"
        )
        
        print_performance_summary(performance_5d, "5个交易日")
        
        # 分析10天后表现
        print("\\n========== 10个交易日后表现分析 ==========")
        logger.info("[逆向策略表现分析] 开始分析筛选股票10天后的表现")
        
        performance_10d = screener.analyze_performance(
            screened_stocks=screened_stocks,
            screening_date=screening_date,
            analysis_days=10,
            condition_description=condition_desc + " (10日后表现)"
        )
        
        print_performance_summary(performance_10d, "10个交易日")
        
        # 对比分析
        print("\\n========== 策略效果对比分析 ==========")
        print_comparison_analysis(performance_5d, performance_10d)
        
        # 导出详细结果
        logger.info("[逆向策略导出] 开始导出分析结果")
        
        # 导出5日表现
        exported_files_5d = screener.export_analysis_result(performance_5d)
        
        # 导出10日表现
        exported_files_10d = screener.export_analysis_result(performance_10d)
        
        print("\\n========== 结果已导出 ==========")
        print("5日后表现分析文件:")
        for file_type, file_path in exported_files_5d.items():
            print(f"  {file_type}: {file_path}")
        print("\\n10日后表现分析文件:")
        for file_type, file_path in exported_files_10d.items():
            print(f"  {file_type}: {file_path}")
        
        logger.info("[逆向策略分析完成] 逆向投资策略分析全部完成")


def print_performance_summary(performance_result, period_name: str):
    """打印表现分析摘要"""
    if performance_result.total_screened == 0:
        print(f"❌ {period_name}后表现分析失败：无法获取足够的价格数据")
        return
    
    print(f"📊 分析期间: {performance_result.screening_date} -> {period_name}后")
    print(f"📈 分析股票数: {performance_result.total_screened}只")
    print(f"💰 平均收益率: {performance_result.avg_return:.2f}%")
    print(f"📊 中位数收益率: {performance_result.median_return:.2f}%")
    print(f"🎯 胜率: {performance_result.win_rate:.2f}%")
    print(f"🔥 最大收益率: {performance_result.max_return:.2f}%")
    print(f"❄️ 最小收益率: {performance_result.min_return:.2f}%")
    print(f"📈 上涨股票: {performance_result.positive_count}只")
    print(f"📉 下跌股票: {performance_result.negative_count}只")
    
    # 显示表现最好和最差的股票
    if performance_result.stock_performances:
        print(f"\\n🏆 表现最好的3只股票({period_name}后):")
        for i, stock in enumerate(performance_result.stock_performances[:3]):
            print(f"  {i+1}. {stock['name']}({stock['ts_code']}): {stock['return_pct']:.2f}%")
        
        print(f"\\n💔 表现最差的3只股票({period_name}后):")
        for i, stock in enumerate(performance_result.stock_performances[-3:]):
            print(f"  {i+1}. {stock['name']}({stock['ts_code']}): {stock['return_pct']:.2f}%")


def print_comparison_analysis(performance_5d, performance_10d):
    """打印对比分析"""
    if performance_5d.total_screened == 0 or performance_10d.total_screened == 0:
        print("❌ 对比分析失败：缺少必要的表现数据")
        return
    
    print("📊 逆向投资策略效果总结:")
    print(f"   5日平均收益率: {performance_5d.avg_return:.2f}%  |  10日平均收益率: {performance_10d.avg_return:.2f}%")
    print(f"   5日胜率: {performance_5d.win_rate:.2f}%           |  10日胜率: {performance_10d.win_rate:.2f}%")
    print(f"   5日最大收益: {performance_5d.max_return:.2f}%     |  10日最大收益: {performance_10d.max_return:.2f}%")
    
    # 策略评价
    print("\\n🎯 策略评价:")
    
    if performance_5d.avg_return > 0 and performance_10d.avg_return > 0:
        print("✅ 逆向策略整体有效：短期和中期都获得正收益")
    elif performance_5d.avg_return > 0:
        print("⚠️ 逆向策略短期有效：5日表现良好，但10日效果减弱")
    elif performance_10d.avg_return > 0:
        print("⚠️ 逆向策略中期有效：短期波动较大，但10日表现回升")
    else:
        print("❌ 逆向策略效果不佳：需要调整筛选条件或等待更好的市场时机")
    
    if performance_5d.win_rate > 60 or performance_10d.win_rate > 60:
        print("✅ 胜率表现良好：超过60%的股票获得正收益")
    elif performance_5d.win_rate > 50 or performance_10d.win_rate > 50:
        print("⚠️ 胜率一般：约半数股票获得正收益")
    else:
        print("❌ 胜率偏低：多数股票仍在下跌，可能需要更长时间恢复")


def main():
    """主函数"""
    logger = get_logger(__name__)
    logger.info("[主程序开始] 逆向投资策略分析脚本启动")
    
    print("\\n" + "="*60)
    print("              逆向投资策略分析工具")
    print("="*60)
    print("\\n策略理念: 在大盘上涨时寻找被错杀的个股")
    print("\\n筛选条件:")
    print("  1. 对应盘面(指数)涨幅 ≥ 2%")
    print("  2. 个股当日跌幅 ≥ 6%") 
    print("  3. 个股近20日涨幅 ≤ 20%")
    print("\\n分析维度:")
    print("  - 未来5个交易日表现")
    print("  - 未来10个交易日表现")
    print("\\n支持板块: 上证、深证、创业板、科创板")
    
    try:
        analyze_contrarian_strategy()
        
        print("\\n" + "="*60)
        print("              逆向投资策略分析完成")
        print("="*60)
        print("\\n📁 分析结果已保存到 /Users/nxm/PycharmProjects/dataDig/results/ 目录")
        print("💡 建议: 结合市场环境和个股基本面做最终投资决策")
        
        logger.info("[主程序完成] 逆向投资策略分析已完成")
        
    except Exception as e:
        logger.error(f"[主程序错误] 执行过程中发生错误: {str(e)}")
        print(f"\\n❌ 执行过程中发生错误: {str(e)}")
        print("📋 请检查日志文件获取详细错误信息")
        print(f"📄 日志文件位置: /Users/nxm/PycharmProjects/dataDig/logs/")


if __name__ == "__main__":
    main()
