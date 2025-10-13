#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
检查数据库中可用数据的脚本

用法示例:
python check_available_data.py
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


def check_data_availability():
    """检查数据库中可用的数据"""
    logger = get_logger(__name__)
    logger.info("[数据检查] 开始检查数据库中的可用数据")
    
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
        from src.models.daily_price import DailyPrice, DailyBasic, IndexDaily, StockBasic
        from sqlalchemy import select, func, and_
        
        print("\\n========== 数据库数据概况检查 ==========")
        
        # 1. 检查股票基础信息
        stmt = select(func.count(StockBasic.ts_code))
        stock_count = session.execute(stmt).scalar()
        print(f"股票基础信息表: {stock_count} 只股票")
        
        # 2. 检查日线价格数据
        stmt = select(
            func.min(DailyPrice.trade_date),
            func.max(DailyPrice.trade_date),
            func.count(DailyPrice.id.distinct())
        )
        result = session.execute(stmt).fetchone()
        if result and result[0]:
            print(f"日线价格数据: {result[0]} 到 {result[1]}, 共 {result[2]:,} 条记录")
        else:
            print("日线价格数据: 无数据")
        
        # 3. 检查每日指标数据
        stmt = select(
            func.min(DailyBasic.trade_date),
            func.max(DailyBasic.trade_date),
            func.count(DailyBasic.id.distinct())
        )
        result = session.execute(stmt).fetchone()
        if result and result[0]:
            print(f"每日指标数据: {result[0]} 到 {result[1]}, 共 {result[2]:,} 条记录")
        else:
            print("每日指标数据: 无数据")
        
        # 4. 检查指数数据
        stmt = select(
            func.min(IndexDaily.trade_date),
            func.max(IndexDaily.trade_date),
            func.count(IndexDaily.id.distinct())
        )
        result = session.execute(stmt).fetchone()
        if result and result[0]:
            print(f"指数日线数据: {result[0]} 到 {result[1]}, 共 {result[2]:,} 条记录")
        else:
            print("指数日线数据: 无数据")
        
        # 5. 检查最近10个有完整数据的交易日
        print("\\n========== 完整数据可用日期检查 ==========")
        
        # 查询有完整数据的日期（同时有股票数据、基本面数据和指数数据）
        stmt = select(DailyPrice.trade_date.distinct()).where(
            DailyPrice.trade_date.in_(
                select(DailyBasic.trade_date.distinct())
            ),
            DailyPrice.trade_date.in_(
                select(IndexDaily.trade_date.distinct())
            )
        ).order_by(DailyPrice.trade_date.desc()).limit(15)
        
        complete_dates = session.execute(stmt).scalars().all()
        
        if complete_dates:
            print(f"最近 {len(complete_dates)} 个有完整数据的交易日:")
            for i, date in enumerate(complete_dates, 1):
                # 查询该日期的数据量
                stmt = select(func.count(DailyPrice.id)).where(DailyPrice.trade_date == date)
                price_count = session.execute(stmt).scalar()
                
                stmt = select(func.count(DailyBasic.id)).where(DailyBasic.trade_date == date)
                basic_count = session.execute(stmt).scalar()
                
                stmt = select(func.count(IndexDaily.id)).where(IndexDaily.trade_date == date)
                index_count = session.execute(stmt).scalar()
                
                print(f"  {i:2d}. {date}: 股票价格 {price_count:,} 条, 基本面 {basic_count:,} 条, 指数 {index_count:,} 条")
        else:
            print("❌ 未找到有完整数据的交易日")
        
        # 6. 检查指数涨跌幅情况
        if complete_dates:
            print("\\n========== 指数表现检查 (最近5个交易日) ==========")
            test_dates = complete_dates[:5]
            
            index_codes = ['000001.SH', '399001.SZ', '399006.SZ', '000688.SH']
            index_names = ['上证指数', '深证成指', '创业板指', '科创50']
            
            for date in test_dates:
                print(f"\\n{date} 指数表现:")
                for code, name in zip(index_codes, index_names):
                    stmt = select(IndexDaily.pct_chg).where(
                        and_(IndexDaily.ts_code == code, IndexDaily.trade_date == date)
                    )
                    pct_chg = session.execute(stmt).scalar()
                    if pct_chg is not None:
                        print(f"  {name}({code}): {pct_chg:+.2f}%")
                    else:
                        print(f"  {name}({code}): 无数据")
        
        print("\\n========== 数据检查完成 ==========")
        logger.info("[数据检查] 数据库数据检查完成")


def main():
    """主函数"""
    logger = get_logger(__name__)
    logger.info("[数据检查程序开始] 数据库数据检查脚本启动")
    
    print("\\n" + "="*60)
    print("              数据库数据检查工具")
    print("="*60)
    
    try:
        check_data_availability()
        
        print("\\n" + "="*60)
        print("              数据检查完成")
        print("="*60)
        print("\\n💡 如果发现有完整数据的日期，可以在逆向投资策略脚本中使用这些日期进行测试")
        
        logger.info("[数据检查程序完成] 数据库数据检查已完成")
        
    except Exception as e:
        logger.error(f"[数据检查程序错误] 检查过程中发生错误: {str(e)}")
        print(f"\\n❌ 检查过程中发生错误: {str(e)}")
        print("📋 请检查日志文件获取详细错误信息")
        import traceback
        print("\\n详细错误信息:")
        traceback.print_exc()


if __name__ == "__main__":
    main()
