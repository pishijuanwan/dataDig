#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
逆向投资策略测试脚本

快速测试逆向投资策略的基本功能

用法示例:
python test_contrarian_strategy.py
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


def test_basic_functions():
    """测试基本功能"""
    logger = get_logger(__name__)
    logger.info("[基本功能测试] 开始测试逆向投资策略的基本功能")
    
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
        
        print("\\n========== 逆向投资策略功能测试 ==========")
        
        # 1. 测试板块判断功能
        print("\\n1. 测试板块判断功能:")
        test_codes = ['000001.SZ', '600036.SH', '300001.SZ', '688001.SH', '002001.SZ']
        for code in test_codes:
            board = screener.get_stock_board(code)
            index_code = screener.get_corresponding_index(code)
            print(f"   {code} -> 板块:{board}, 对应指数:{index_code}")
        
        # 2. 测试获取最近交易日期
        print("\\n2. 测试获取最近交易日期:")
        from src.models.daily_price import DailyPrice
        from sqlalchemy import select, func
        
        stmt = select(DailyPrice.trade_date.distinct()).order_by(DailyPrice.trade_date.desc()).limit(5)
        recent_dates = session.execute(stmt).scalars().all()
        
        if recent_dates:
            print(f"   最近5个交易日: {list(recent_dates)}")
            test_date = recent_dates[2] if len(recent_dates) > 2 else recent_dates[0]
            print(f"   选择测试日期: {test_date}")
            
            # 3. 测试指数表现查询
            print("\\n3. 测试指数表现查询:")
            test_indices = ['000001.SH', '399001.SZ', '399006.SZ']
            for index_code in test_indices:
                performance = screener.get_index_performance(index_code, test_date)
                if performance is not None:
                    print(f"   {index_code}在{test_date}的涨跌幅: {performance:.2f}%")
                else:
                    print(f"   {index_code}在{test_date}无数据")
            
            # 4. 测试历史涨幅计算
            print("\\n4. 测试历史涨幅计算:")
            test_stock = '000001.SZ'  # 平安银行
            historical_perf = screener.get_stock_historical_performance(test_stock, test_date, 20)
            if historical_perf is not None:
                print(f"   {test_stock}截止{test_date}的20日涨幅: {historical_perf:.2f}%")
            else:
                print(f"   {test_stock}历史数据不足")
            
            # 5. 测试筛选基础数据获取
            print("\\n5. 测试筛选基础数据获取:")
            base_data = screener._get_screening_base_data(test_date)
            if not base_data.empty:
                print(f"   {test_date}共有{len(base_data)}只股票的基础数据")
                print(f"   数据列: {list(base_data.columns)}")
                
                # 显示部分数据示例
                if len(base_data) > 0:
                    sample = base_data.head(3)[['ts_code', 'name', 'pct_chg', 'pe', 'pb']].copy()
                    print("   数据示例:")
                    print(sample.to_string(index=False))
            else:
                print(f"   {test_date}无基础数据")
                
        else:
            print("   未找到交易日期数据")
        
        print("\\n========== 功能测试完成 ==========")
        logger.info("[基本功能测试] 逆向投资策略基本功能测试完成")


def test_simple_contrarian_screening():
    """测试简单的逆向筛选"""
    logger = get_logger(__name__)
    logger.info("[简单筛选测试] 开始测试逆向筛选功能")
    
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
        
        print("\\n========== 简化逆向筛选测试 ==========")
        
        # 获取最近的交易日期（往前找几天，确保有数据分析）
        from src.models.daily_price import DailyPrice
        from sqlalchemy import select
        
        stmt = select(DailyPrice.trade_date.distinct()).order_by(DailyPrice.trade_date.desc()).limit(20)
        recent_dates = session.execute(stmt).scalars().all()
        
        if len(recent_dates) < 15:
            print("❌ 交易日期数据不足，无法进行测试")
            return
        
        # 选择一个测试日期（第10个交易日，确保有后续数据）
        test_date = recent_dates[10]
        print(f"测试日期: {test_date}")
        
        # 放宽筛选条件进行测试
        print("\\n使用放宽的筛选条件进行测试:")
        print("- 指数涨幅 ≥ 1% (放宽)")
        print("- 个股跌幅 ≥ 3% (放宽)")  
        print("- 近20日涨幅 ≤ 30% (放宽)")
        
        contrarian_condition = ContrarianCondition(
            screener_service=screener,
            screening_date=test_date,
            min_index_rise=1.0,      # 放宽到1%
            max_stock_fall=-3.0,     # 放宽到-3%
            max_historical_rise=30.0, # 放宽到30%
            historical_days=20
        )
        
        # 执行筛选
        conditions = [contrarian_condition]
        screened_stocks = screener.screen_stocks(
            screening_date=test_date,
            conditions=conditions
        )
        
        if screened_stocks.empty:
            print("\\n❌ 即使放宽条件也未找到符合的股票")
            print("可能原因:")
            print("1. 测试日期大盘未上涨")
            print("2. 指数数据缺失")
            print("3. 历史数据不足")
        else:
            print(f"\\n✅ 找到 {len(screened_stocks)} 只符合条件的股票")
            
            # 显示结果
            display_cols = ['ts_code', 'name', 'board', 'pct_chg', 'corresponding_index', 'index_pct_chg', 'historical_performance']
            available_cols = [col for col in display_cols if col in screened_stocks.columns]
            
            if available_cols:
                print("\\n筛选结果:")
                sample_data = screened_stocks[available_cols].head(10).copy()
                for col in ['pct_chg', 'index_pct_chg', 'historical_performance']:
                    if col in sample_data.columns:
                        sample_data[col] = sample_data[col].round(2)
                print(sample_data.to_string(index=False))
                
                # 简单的5日表现分析
                print("\\n进行5日后表现分析...")
                try:
                    performance_result = screener.analyze_performance(
                        screened_stocks=screened_stocks,
                        screening_date=test_date,
                        analysis_days=5,
                        condition_description="测试筛选条件"
                    )
                    
                    if performance_result.total_screened > 0:
                        print(f"✅ 表现分析成功")
                        print(f"   平均收益率: {performance_result.avg_return:.2f}%")
                        print(f"   胜率: {performance_result.win_rate:.2f}%")
                        print(f"   最大收益: {performance_result.max_return:.2f}%")
                        print(f"   最小收益: {performance_result.min_return:.2f}%")
                    else:
                        print("❌ 表现分析失败：缺少后续价格数据")
                        
                except Exception as e:
                    print(f"❌ 表现分析出错: {str(e)}")
            
        print("\\n========== 简化筛选测试完成 ==========")
        logger.info("[简单筛选测试] 逆向筛选功能测试完成")


def main():
    """主函数"""
    logger = get_logger(__name__)
    logger.info("[测试程序开始] 逆向投资策略测试脚本启动")
    
    print("\\n" + "="*60)
    print("              逆向投资策略功能测试")
    print("="*60)
    
    try:
        # 测试基本功能
        test_basic_functions()
        
        # 测试简单筛选
        test_simple_contrarian_screening()
        
        print("\\n" + "="*60)
        print("              所有测试完成")
        print("="*60)
        print("\\n💡 如果测试通过，可以运行完整的逆向投资策略分析:")
        print("   ./scripts/shell/run_contrarian_strategy.sh")
        
        logger.info("[测试程序完成] 所有测试已完成")
        
    except Exception as e:
        logger.error(f"[测试程序错误] 测试过程中发生错误: {str(e)}")
        print(f"\\n❌ 测试过程中发生错误: {str(e)}")
        print("📋 请检查日志文件获取详细错误信息")
        import traceback
        print("\\n详细错误信息:")
        traceback.print_exc()


if __name__ == "__main__":
    main()
