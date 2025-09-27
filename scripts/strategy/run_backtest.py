#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
策略回测运行脚本
提供简单的接口来运行策略回测

使用示例：
python scripts/strategy/run_backtest.py
"""

import sys
import os
from datetime import datetime, timedelta

# 添加项目根目录到Python路径
sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))

from src.config.settings import load_settings
from src.app_logging.logger import setup_logger
from src.db.mysql_client import MySQLClient
from src.strategy.services.strategy_service import StrategyService
from src.strategy.strategies.combined_strategies.simple_ma_strategy import SimpleMAStrategy, SimpleMAStrategyConfig


def create_results_directory():
    """创建结果输出目录"""
    results_dir = "/Users/nxm/PycharmProjects/dataDig/results"
    os.makedirs(results_dir, exist_ok=True)
    return results_dir


def run_simple_ma_strategy_backtest():
    """运行简单移动平均策略回测"""
    
    # 1. 初始化配置和日志
    settings = load_settings()
    logger = setup_logger("INFO", "/Users/nxm/PycharmProjects/dataDig/logs", "strategy_backtest.log")
    logger.info("[脚本启动] 开始运行策略回测")
    
    # 2. 初始化数据库连接
    mysql_client = MySQLClient(
        host=settings.database.host,
        port=settings.database.port,
        user=settings.database.user,
        password=settings.database.password,
        db_name=settings.database.name,
    )
    
    # 确保数据库存在
    mysql_client.create_database_if_not_exists()
    
    # 创建会话
    SessionFactory = mysql_client.session_factory()
    session = SessionFactory()
    
    # 3. 初始化策略服务
    strategy_service = StrategyService(session, logger)
    
    try:
        # 4. 配置策略参数
        strategy_config = {
            'short_window': 5,        # 5日均线
            'long_window': 20,        # 20日均线  
            'initial_cash': 100000.0, # 10万初始资金
            'max_position_pct': 0.95, # 最大95%仓位
            'max_stocks': 3,          # 最多同时持有3只股票
            'position_per_stock': 0.3 # 每只股票分配30%资金
        }
        
        # 5. 设置回测参数
        start_date = "20240102"  # 从2022年开始
        end_date = "20250924"    # 到2023年结束
        
        # 股票池选择选项：
        # 1. 指定特定股票
        specific_symbols = [
            "000001.SZ",  # 平安银行
            "600036.SH",  # 招商银行
            "600519.SH",  # 贵州茅台
            "000858.SZ",  # 五粮液
            "600000.SH",  # 浦发银行
        ]
        
        # 2. 获取数据库中的所有股票（全市场回测）
        all_symbols = strategy_service.backtest_engine.get_available_symbols()
        
        # 3. 获取指定数量的股票
        top_500_symbols = strategy_service.backtest_engine.get_available_symbols(limit=500)
        top_100_symbols = strategy_service.backtest_engine.get_available_symbols(limit=100)
        top_50_symbols = strategy_service.backtest_engine.get_available_symbols(limit=50)
        
        # 选择使用哪个股票池（修改这里来改变股票池）
        # symbols = specific_symbols      # 使用指定的5只股票
        symbols = top_100_symbols        # 使用前100只股票
        # symbols = top_500_symbols      # 使用前500只股票（中等规模）  
        # symbols = all_symbols          # 使用全部5435只股票（注意：需要9分钟）
        
        logger.info(f"[策略配置] 回测时间范围: {start_date} ~ {end_date}")
        logger.info(f"[策略配置] 股票池数量: {len(symbols)}")
        logger.info(f"[策略配置] 初始资金: {strategy_config['initial_cash']:,.0f}")
        
        # 6. 运行回测
        result = strategy_service.run_single_strategy_backtest(
            strategy_class=SimpleMAStrategy,
            strategy_config=strategy_config,
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            commission_rate=0.002  # 0.2% 手续费
        )
        
        # 7. 输出回测结果
        strategy_service.print_backtest_summary(result)
        
        # 8. 导出结果到文件
        results_dir = create_results_directory()
        exported_files = strategy_service.export_backtest_result(result, results_dir)
        
        if exported_files:
            logger.info("[文件导出] 回测结果已导出到以下文件:")
            for file_type, file_path in exported_files.items():
                logger.info(f"  {file_type}: {file_path}")
        
        # 9. 显示交易记录摘要
        trades_df = result.get_trades_df()
        if not trades_df.empty:
            print(f"\\n交易记录摘要（共{len(trades_df)}笔）:")
            print("=" * 60)
            buy_trades = trades_df[trades_df['action'] == 'buy']
            sell_trades = trades_df[trades_df['action'] == 'sell']
            print(f"买入交易: {len(buy_trades)}笔")
            print(f"卖出交易: {len(sell_trades)}笔")
            
            if len(buy_trades) > 0:
                print("\\n最近5笔买入交易:")
                print(buy_trades.head().to_string(index=False))
            
            if len(sell_trades) > 0:
                print("\\n最近5笔卖出交易:")
                print(sell_trades.head().to_string(index=False))
        
        logger.info("[脚本完成] 策略回测运行完成")
        
    except Exception as e:
        logger.error(f"[脚本错误] 回测过程中发生错误: {str(e)}")
        raise
    
    finally:
        # 关闭数据库连接
        session.close()


def run_full_market_backtest():
    """运行全市场股票回测"""
    
    # 1. 初始化配置和日志
    settings = load_settings()
    logger = setup_logger("INFO", "/Users/nxm/PycharmProjects/dataDig/logs", "full_market_backtest.log")
    logger.info("[全市场回测] 开始运行全市场策略回测")
    
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
    strategy_service = StrategyService(session, logger)
    
    try:
        # 3. 获取全部可用股票
        all_symbols = strategy_service.backtest_engine.get_available_symbols()
        
        if not all_symbols:
            logger.warning("[全市场回测] 数据库中未找到任何股票数据")
            print("数据库中未找到股票数据，请先运行数据采集: python src/app.py")
            return
        
        logger.info(f"[全市场回测] 发现 {len(all_symbols)} 只可用股票")
        print(f"\\n即将对 {len(all_symbols)} 只股票进行全市场回测")
        
        # 4. 配置策略参数  
        strategy_config = {
            'short_window': 5,        # 5日均线
            'long_window': 20,        # 20日均线  
            'initial_cash': 1000000.0, # 100万初始资金（全市场需要更多资金）
            'max_position_pct': 0.95,  # 最大95%仓位
            'max_stocks': 20,          # 最多同时持有20只股票
            'position_per_stock': 0.05 # 每只股票分配5%资金
        }
        
        # 5. 设置回测参数
        start_date = "20240102"
        end_date = "20250924"
        
        logger.info(f"[全市场回测] 回测时间范围: {start_date} ~ {end_date}")
        logger.info(f"[全市场回测] 初始资金: {strategy_config['initial_cash']:,.0f}")
        
        # 6. 运行全市场回测
        print("\\n开始执行全市场回测，这可能需要较长时间...")
        result = strategy_service.run_single_strategy_backtest(
            strategy_class=SimpleMAStrategy,
            strategy_config=strategy_config,
            symbols=all_symbols,  # 使用全部股票
            start_date=start_date,
            end_date=end_date,
            commission_rate=0.002  # 0.2% 手续费
        )
        
        # 7. 输出回测结果
        print("\\n" + "="*60)
        print(f"全市场回测完成！覆盖 {len(all_symbols)} 只股票")
        print("="*60)
        strategy_service.print_backtest_summary(result)
        
        # 8. 导出结果到文件
        results_dir = create_results_directory()
        exported_files = strategy_service.export_backtest_result(result, results_dir)
        
        if exported_files:
            logger.info("[文件导出] 全市场回测结果已导出到以下文件:")
            for file_type, file_path in exported_files.items():
                logger.info(f"  {file_type}: {file_path}")
        
        # 9. 显示重要统计信息
        trades_df = result.get_trades_df()
        if not trades_df.empty:
            print(f"\\n全市场交易统计:")
            print("=" * 40)
            print(f"总交易笔数: {len(trades_df)}")
            print(f"涉及股票数: {trades_df['symbol'].nunique()}")
            print(f"买入交易: {len(trades_df[trades_df['action'] == 'buy'])}笔")
            print(f"卖出交易: {len(trades_df[trades_df['action'] == 'sell'])}笔")
            
            # 显示交易最活跃的股票
            symbol_counts = trades_df['symbol'].value_counts().head(10)
            print(f"\\n交易最活跃的前10只股票:")
            for symbol, count in symbol_counts.items():
                print(f"  {symbol}: {count}笔")
        
        logger.info("[全市场回测完成] 全市场策略回测运行完成")
        
    except Exception as e:
        logger.error(f"[全市场回测错误] 回测过程中发生错误: {str(e)}")
        raise
    
    finally:
        session.close()


def run_strategy_comparison_example():
    """运行多策略对比示例"""
    
    settings = load_settings()
    logger = setup_logger("INFO", "/Users/nxm/PycharmProjects/dataDig/logs", "strategy_comparison.log")
    logger.info("[多策略对比] 开始运行策略对比")
    
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
    strategy_service = StrategyService(session, logger)
    
    try:
        # 定义多个策略配置
        strategies = [
            {
                'name': 'MA_5_20',
                'class': SimpleMAStrategy,
                'config': {
                    'short_window': 5,
                    'long_window': 20,
                    'initial_cash': 100000.0,
                    'max_position_pct': 0.95
                }
            },
            {
                'name': 'MA_10_30', 
                'class': SimpleMAStrategy,
                'config': {
                    'short_window': 10,
                    'long_window': 30,
                    'initial_cash': 100000.0,
                    'max_position_pct': 0.95
                }
            },
            {
                'name': 'MA_5_60',
                'class': SimpleMAStrategy,
                'config': {
                    'short_window': 5,
                    'long_window': 60,
                    'initial_cash': 100000.0,
                    'max_position_pct': 0.95
                }
            }
        ]
        
        # 运行对比回测
        results = strategy_service.run_strategy_comparison(
            strategies=strategies,
            start_date="20220101",
            end_date="20231231",
            symbols=["000001.SZ", "600036.SH", "600519.SH"]
        )
        
        # 生成对比表
        comparison_df = strategy_service.get_performance_comparison(results)
        print("\\n========== 策略性能对比 ==========")
        print(comparison_df.to_string(index=False))
        
        # 导出对比结果
        results_dir = create_results_directory()
        comparison_file = os.path.join(results_dir, f"strategy_comparison_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
        comparison_df.to_csv(comparison_file, index=False, encoding='utf-8-sig')
        
        logger.info(f"[对比完成] 策略对比结果已保存到: {comparison_file}")
        
    except Exception as e:
        logger.error(f"[对比错误] 策略对比过程中发生错误: {str(e)}")
        raise
    
    finally:
        session.close()


def main():
    """主函数"""
    import sys
    
    print("策略回测工具")
    print("=" * 50)
    
    # 支持命令行参数，默认运行策略1
    if len(sys.argv) > 1:
        choice = sys.argv[1]
    else:
        print("1. 运行简单移动平均策略回测（50只股票）")
        print("2. 运行多策略对比")  
        print("3. 运行全市场回测（所有股票）")
        print("=" * 50)
        print("默认运行策略1（移动平均策略回测）")
        print("如需选择其他策略，请使用: python scripts/strategy/run_backtest.py [1|2|3]")
        print("=" * 50)
        choice = "1"
    
    try:
        if choice == "1":
            print("\\n[开始运行] 简单移动平均策略回测（50只股票）")
            run_simple_ma_strategy_backtest()
        elif choice == "2":
            print("\\n[开始运行] 多策略对比")
            run_strategy_comparison_example()
        elif choice == "3":
            print("\\n[开始运行] 全市场回测（所有股票）")
            run_full_market_backtest()
        else:
            print(f"\\n无效选择: {choice}，默认运行简单移动平均策略回测")
            run_simple_ma_strategy_backtest()
            
    except KeyboardInterrupt:
        print("\\n用户中断操作")
    except Exception as e:
        print(f"\\n运行错误: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
