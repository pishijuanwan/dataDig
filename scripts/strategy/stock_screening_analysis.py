#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票筛选和表现分析脚本

通过一定条件筛选股票，然后分析它们若干天后的表现，统计盈亏比例等数据

用法示例:
python stock_screening_analysis.py
"""

import sys
import os
from datetime import datetime, timedelta

# 添加项目根路径到Python路径
sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))

from src.config.settings import Settings
from src.db.mysql_client import MySQLClient
from src.app_logging.logger import get_logger
from src.services.stock_screener_service import StockScreenerService


def get_recent_trading_date(mysql_client, days_ago: int = 1) -> str:
    """获取最近的交易日期"""
    logger = get_logger(__name__)
    
    with mysql_client.get_session() as session:
        from src.models.daily_price import DailyPrice
        from sqlalchemy import select, func
        
        # 查询最近的交易日期
        stmt = select(DailyPrice.trade_date.distinct()).order_by(DailyPrice.trade_date.desc()).limit(days_ago + 5)
        result = session.execute(stmt).scalars().all()
        
        if len(result) > days_ago:
            selected_date = result[days_ago]
            logger.info(f"[获取交易日] 选择{days_ago}个交易日前的日期: {selected_date}")
            return selected_date
        elif result:
            selected_date = result[-1]
            logger.info(f"[获取交易日] 交易日不足，使用最早的日期: {selected_date}")
            return selected_date
        else:
            logger.error("[获取交易日] 数据库中没有找到交易日期")
            return ""


def demo_pe_pb_screening():
    """示例1：低市盈率、低市净率股票筛选"""
    logger = get_logger(__name__)
    logger.info("[示例1开始] 低市盈率、低市净率股票筛选分析")
    
    # 初始化服务
    settings = Settings()
    mysql_client = MySQLClient(settings, logger)
    
    with mysql_client.get_session() as session:
        screener = StockScreenerService(session, logger)
        
        # 获取最近的交易日期（5个交易日前，确保有后续数据分析）
        screening_date = get_recent_trading_date(mysql_client, days_ago=10)
        if not screening_date:
            logger.error("[示例1失败] 无法获取合适的筛选日期")
            return
        
        # 设置筛选条件
        conditions = [
            screener.create_pe_condition(max_pe=20),  # 市盈率小于等于20
            screener.create_pb_condition(max_pb=2),   # 市净率小于等于2
            screener.create_market_cap_condition(min_mv=100000),  # 市值大于等于10亿
            screener.create_volume_surge_condition(min_volume_ratio=1.2)  # 量比大于1.2
        ]
        
        condition_desc = "低市盈率(≤20) + 低市净率(≤2) + 大市值(≥10亿) + 相对放量(量比≥1.2)"
        
        # 执行筛选
        logger.info(f"[示例1筛选] 开始执行筛选，条件: {condition_desc}")
        screened_stocks = screener.screen_stocks(
            screening_date=screening_date,
            conditions=conditions,
            market_filter=None  # 不限制市场
        )
        
        if screened_stocks.empty:
            logger.warning("[示例1结果] 没有股票符合筛选条件")
            return
        
        logger.info(f"[示例1筛选结果] 筛选出{len(screened_stocks)}只股票")
        
        # 打印部分筛选结果
        print(f"\\n========== 筛选结果预览 ==========")
        print(f"筛选日期: {screening_date}")
        print(f"筛选条件: {condition_desc}")
        print(f"筛选结果: {len(screened_stocks)}只股票")
        print("\\n前10只股票:")
        display_cols = ['ts_code', 'name', 'pe', 'pb', 'total_mv', 'turnover_rate']
        available_cols = [col for col in display_cols if col in screened_stocks.columns]
        print(screened_stocks[available_cols].head(10).to_string(index=False))
        
        # 分析后续表现（5天后）
        logger.info("[示例1表现分析] 开始分析筛选股票5天后的表现")
        performance_result = screener.analyze_performance(
            screened_stocks=screened_stocks,
            screening_date=screening_date,
            analysis_days=5,
            condition_description=condition_desc
        )
        
        # 打印表现分析结果
        print(f"\\n========== 表现分析结果 ==========")
        print(f"分析期间: {screening_date} -> 5个交易日后")
        print(f"分析股票数: {performance_result.total_screened}只")
        print(f"平均收益率: {performance_result.avg_return:.2f}%")
        print(f"中位数收益率: {performance_result.median_return:.2f}%")
        print(f"胜率: {performance_result.win_rate:.2f}%")
        print(f"最大收益率: {performance_result.max_return:.2f}%")
        print(f"最小收益率: {performance_result.min_return:.2f}%")
        print(f"上涨股票: {performance_result.positive_count}只")
        print(f"下跌股票: {performance_result.negative_count}只")
        
        # 显示表现最好和最差的股票
        if performance_result.stock_performances:
            print("\\n表现最好的5只股票:")
            for i, stock in enumerate(performance_result.stock_performances[:5]):
                print(f"  {i+1}. {stock['name']}({stock['ts_code']}): {stock['return_pct']:.2f}%")
            
            print("\\n表现最差的5只股票:")
            for i, stock in enumerate(performance_result.stock_performances[-5:]):
                print(f"  {i+1}. {stock['name']}({stock['ts_code']}): {stock['return_pct']:.2f}%")
        
        # 导出结果
        logger.info("[示例1导出] 开始导出分析结果")
        exported_files = screener.export_analysis_result(performance_result)
        
        if exported_files:
            print(f"\\n========== 结果已导出 ==========")
            for file_type, file_path in exported_files.items():
                print(f"{file_type}: {file_path}")
        
        logger.info("[示例1完成] 低市盈率、低市净率股票筛选分析完成")


def demo_volume_surge_screening():
    """示例2：放量上涨股票筛选"""
    logger = get_logger(__name__)
    logger.info("[示例2开始] 放量上涨股票筛选分析")
    
    # 初始化服务
    settings = Settings()
    mysql_client = MySQLClient(settings, logger)
    
    with mysql_client.get_session() as session:
        screener = StockScreenerService(session, logger)
        
        # 获取最近的交易日期（10个交易日前）
        screening_date = get_recent_trading_date(mysql_client, days_ago=10)
        if not screening_date:
            logger.error("[示例2失败] 无法获取合适的筛选日期")
            return
        
        # 设置筛选条件：放量上涨
        conditions = [
            screener.create_price_change_condition(min_pct=3.0, max_pct=9.9),  # 涨幅3%-9.9%（避免涨停）
            screener.create_volume_surge_condition(min_volume_ratio=2.0),      # 量比大于2（明显放量）
            screener.create_market_cap_condition(min_mv=50000, max_mv=1000000), # 市值5-100亿（中等市值）
            screener.create_turnover_condition(min_turnover=3.0)               # 换手率大于3%（活跃交易）
        ]
        
        condition_desc = "放量上涨(涨幅3%-9.9%) + 明显放量(量比≥2) + 中等市值(5-100亿) + 活跃交易(换手率≥3%)"
        
        # 执行筛选
        logger.info(f"[示例2筛选] 开始执行筛选，条件: {condition_desc}")
        screened_stocks = screener.screen_stocks(
            screening_date=screening_date,
            conditions=conditions,
            market_filter='main'  # 限制主板股票
        )
        
        if screened_stocks.empty:
            logger.warning("[示例2结果] 没有股票符合筛选条件")
            return
        
        logger.info(f"[示例2筛选结果] 筛选出{len(screened_stocks)}只股票")
        
        # 打印筛选结果
        print(f"\\n========== 放量上涨股票筛选结果 ==========")
        print(f"筛选日期: {screening_date}")
        print(f"筛选条件: {condition_desc}")
        print(f"筛选结果: {len(screened_stocks)}只股票")
        print("\\n所有筛选股票:")
        display_cols = ['ts_code', 'name', 'pct_chg', 'volume_ratio', 'turnover_rate', 'total_mv']
        available_cols = [col for col in display_cols if col in screened_stocks.columns]
        print(screened_stocks[available_cols].to_string(index=False))
        
        # 分析后续表现（3天后）
        logger.info("[示例2表现分析] 开始分析筛选股票3天后的表现")
        performance_result = screener.analyze_performance(
            screened_stocks=screened_stocks,
            screening_date=screening_date,
            analysis_days=3,
            condition_description=condition_desc
        )
        
        # 打印表现分析结果
        print(f"\\n========== 放量上涨股票3天后表现 ==========")
        print(f"分析期间: {screening_date} -> 3个交易日后")
        print(f"分析股票数: {performance_result.total_screened}只")
        print(f"平均收益率: {performance_result.avg_return:.2f}%")
        print(f"中位数收益率: {performance_result.median_return:.2f}%")
        print(f"胜率: {performance_result.win_rate:.2f}%")
        print(f"最大收益率: {performance_result.max_return:.2f}%")
        print(f"最小收益率: {performance_result.min_return:.2f}%")
        print(f"上涨股票: {performance_result.positive_count}只")
        print(f"下跌股票: {performance_result.negative_count}只")
        
        # 导出结果
        logger.info("[示例2导出] 开始导出分析结果")
        exported_files = screener.export_analysis_result(performance_result)
        
        if exported_files:
            print(f"\\n========== 结果已导出 ==========")
            for file_type, file_path in exported_files.items():
                print(f"{file_type}: {file_path}")
        
        logger.info("[示例2完成] 放量上涨股票筛选分析完成")


def demo_custom_screening():
    """示例3：自定义筛选条件"""
    logger = get_logger(__name__)
    logger.info("[示例3开始] 自定义筛选条件分析")
    
    # 初始化服务
    settings = Settings()
    mysql_client = MySQLClient(settings, logger)
    
    with mysql_client.get_session() as session:
        screener = StockScreenerService(session, logger)
        
        # 获取最近的交易日期
        screening_date = get_recent_trading_date(mysql_client, days_ago=15)
        if not screening_date:
            logger.error("[示例3失败] 无法获取合适的筛选日期")
            return
        
        # 设置筛选条件：寻找相对被低估的成长股
        conditions = [
            screener.create_pe_condition(min_pe=10, max_pe=30),      # 市盈率10-30（合理估值）
            screener.create_pb_condition(min_pb=1, max_pb=3),        # 市净率1-3（不过度高估）
            screener.create_market_cap_condition(min_mv=200000),     # 市值大于20亿（一定规模）
            screener.create_price_change_condition(min_pct=-2.0, max_pct=2.0)  # 涨跌幅-2%到2%（相对平稳）
        ]
        
        condition_desc = "合理估值成长股(PE:10-30, PB:1-3, 市值≥20亿, 涨跌幅±2%内)"
        
        # 执行筛选
        logger.info(f"[示例3筛选] 开始执行筛选，条件: {condition_desc}")
        screened_stocks = screener.screen_stocks(
            screening_date=screening_date,
            conditions=conditions
        )
        
        if screened_stocks.empty:
            logger.warning("[示例3结果] 没有股票符合筛选条件")
            return
        
        logger.info(f"[示例3筛选结果] 筛选出{len(screened_stocks)}只股票")
        
        # 打印筛选结果
        print(f"\\n========== 合理估值成长股筛选结果 ==========")
        print(f"筛选日期: {screening_date}")
        print(f"筛选条件: {condition_desc}")
        print(f"筛选结果: {len(screened_stocks)}只股票")
        print("\\n前15只股票:")
        display_cols = ['ts_code', 'name', 'pe', 'pb', 'total_mv', 'pct_chg', 'industry']
        available_cols = [col for col in display_cols if col in screened_stocks.columns]
        print(screened_stocks[available_cols].head(15).to_string(index=False))
        
        # 分析后续表现（7天后）
        logger.info("[示例3表现分析] 开始分析筛选股票7天后的表现")
        performance_result = screener.analyze_performance(
            screened_stocks=screened_stocks,
            screening_date=screening_date,
            analysis_days=7,
            condition_description=condition_desc
        )
        
        # 打印表现分析结果
        print(f"\\n========== 合理估值成长股7天后表现 ==========")
        print(f"分析期间: {screening_date} -> 7个交易日后")
        print(f"分析股票数: {performance_result.total_screened}只")
        print(f"平均收益率: {performance_result.avg_return:.2f}%")
        print(f"中位数收益率: {performance_result.median_return:.2f}%")
        print(f"胜率: {performance_result.win_rate:.2f}%")
        print(f"最大收益率: {performance_result.max_return:.2f}%")
        print(f"最小收益率: {performance_result.min_return:.2f}%")
        print(f"上涨股票: {performance_result.positive_count}只")
        print(f"下跌股票: {performance_result.negative_count}只")
        
        # 按行业统计表现
        if performance_result.stock_performances:
            # 结合行业信息统计
            import pandas as pd
            perf_df = pd.DataFrame(performance_result.stock_performances)
            stock_info = screened_stocks[['ts_code', 'industry']].copy()
            perf_with_industry = pd.merge(perf_df, stock_info, on='ts_code', how='left')
            
            industry_stats = perf_with_industry.groupby('industry')['return_pct'].agg(['mean', 'count']).sort_values('mean', ascending=False)
            print("\\n各行业表现统计:")
            print(industry_stats.head(10))
        
        # 导出结果
        logger.info("[示例3导出] 开始导出分析结果")
        exported_files = screener.export_analysis_result(performance_result)
        
        if exported_files:
            print(f"\\n========== 结果已导出 ==========")
            for file_type, file_path in exported_files.items():
                print(f"{file_type}: {file_path}")
        
        logger.info("[示例3完成] 自定义筛选条件分析完成")


def main():
    """主函数"""
    logger = get_logger(__name__)
    logger.info("[主程序开始] 股票筛选和表现分析脚本启动")
    
    print("\\n" + "="*60)
    print("              股票筛选和表现分析工具")
    print("="*60)
    print("\\n本工具通过一定条件筛选股票，然后分析它们若干天后的表现")
    print("包含以下示例分析:")
    print("1. 低市盈率、低市净率股票筛选（价值投资风格）")
    print("2. 放量上涨股票筛选（短期动量策略）")
    print("3. 合理估值成长股筛选（成长投资风格）")
    
    try:
        # 运行示例1：价值投资风格筛选
        print("\\n" + "="*60)
        print("开始执行示例1：价值投资风格筛选")
        print("="*60)
        demo_pe_pb_screening()
        
        # 运行示例2：短期动量策略筛选  
        print("\\n" + "="*60)
        print("开始执行示例2：短期动量策略筛选")
        print("="*60)
        demo_volume_surge_screening()
        
        # 运行示例3：成长投资风格筛选
        print("\\n" + "="*60)
        print("开始执行示例3：成长投资风格筛选")
        print("="*60)
        demo_custom_screening()
        
        print("\\n" + "="*60)
        print("              所有分析完成")
        print("="*60)
        print("\\n分析结果已保存到 /Users/nxm/PycharmProjects/dataDig/results/ 目录")
        
        logger.info("[主程序完成] 所有股票筛选和表现分析已完成")
        
    except Exception as e:
        logger.error(f"[主程序错误] 执行过程中发生错误: {str(e)}")
        print(f"\\n执行过程中发生错误: {str(e)}")
        print("请检查日志文件获取详细错误信息")


if __name__ == "__main__":
    main()
