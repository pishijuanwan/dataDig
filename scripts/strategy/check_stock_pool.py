#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票池检查脚本
用于查看数据库中可用股票的数量和信息

使用示例：
python scripts/strategy/check_stock_pool.py
"""

import sys
import os
from datetime import datetime

# 添加项目根目录到Python路径
sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))

from src.config.settings import load_settings
from src.app_logging.logger import setup_logger
from src.db.mysql_client import MySQLClient
from src.strategy.engines.backtest_engine import BacktestEngine


def check_stock_pool():
    """检查数据库中的股票池信息"""
    
    print("数据库股票池检查工具")
    print("=" * 50)
    
    try:
        # 1. 初始化配置
        settings = load_settings()
        logger = setup_logger("INFO", "/Users/nxm/PycharmProjects/dataDig/logs", "check_stock_pool.log")
        
        # 2. 初始化数据库连接
        mysql_client = MySQLClient(
            host=settings.database.host,
            port=settings.database.port,
            user=settings.database.user,
            password=settings.database.password,
            db_name=settings.database.name,
        )
        
        # 创建会话
        SessionFactory = mysql_client.session_factory()
        session = SessionFactory()
        
        # 3. 创建回测引擎来获取股票信息
        backtest_engine = BacktestEngine(session, logger)
        
        # 4. 获取不同规模的股票池
        all_symbols = backtest_engine.get_available_symbols()
        
        if not all_symbols:
            print("❌ 数据库中未找到任何股票数据")
            print("\\n请先运行数据采集脚本:")
            print("  source .venv/bin/activate && python src/app.py")
            return
        
        # 5. 显示股票池统计信息
        print(f"✅ 数据库连接成功")
        print(f"\\n📊 股票池统计信息:")
        print("=" * 30)
        print(f"总可用股票数: {len(all_symbols)} 只")
        
        # 显示前10只股票作为示例
        print(f"\\n前10只股票示例:")
        for i, symbol in enumerate(all_symbols[:10], 1):
            print(f"  {i:2d}. {symbol}")
        
        if len(all_symbols) > 10:
            print(f"  ... 还有 {len(all_symbols) - 10} 只股票")
        
        # 6. 显示不同规模股票池的选择建议
        print(f"\\n🎯 回测建议:")
        print("=" * 30)
        
        if len(all_symbols) <= 10:
            print("📈 股票数量较少，建议全部使用")
        elif len(all_symbols) <= 50:
            print("📈 股票数量适中，可以全部使用或选择部分")
        elif len(all_symbols) <= 200:
            print("⚡ 股票数量较多，建议:")
            print("   - 测试时使用前50只股票")
            print("   - 正式回测可使用全部股票")
        else:
            print("🚀 股票数量很多，建议:")
            print("   - 快速测试: 前50只股票")
            print("   - 中等规模: 前100-200只股票") 
            print("   - 全市场回测: 全部股票（耗时较长）")
        
        # 7. 显示脚本使用方法
        print(f"\\n🛠️  回测脚本使用方法:")
        print("=" * 30)
        print("1. 50只股票回测:   python scripts/strategy/run_backtest.py 1")
        print("2. 多策略对比:     python scripts/strategy/run_backtest.py 2")
        print("3. 全市场回测:     python scripts/strategy/run_backtest.py 3")
        
        # 8. 显示性能预估
        print(f"\\n⏱️  性能预估:")
        print("=" * 30)
        estimated_time_per_stock = 0.1  # 每只股票大约0.1秒
        total_estimated_time = len(all_symbols) * estimated_time_per_stock
        
        print(f"全市场回测预计耗时: {total_estimated_time:.1f} 秒 (~{total_estimated_time/60:.1f} 分钟)")
        
        if total_estimated_time > 300:  # 超过5分钟
            print("⚠️  全市场回测可能需要较长时间，建议先用小规模测试")
        
        session.close()
        print(f"\\n✅ 检查完成")
        
    except Exception as e:
        print(f"\\n❌ 检查过程中发生错误: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    check_stock_pool()
