#!/usr/bin/env python3
"""
红三兵策略测试脚本

测试内容：
1. 红三兵形态识别逻辑测试
2. 完整策略回测测试
3. 结果分析
"""

import sys
import os
import pandas as pd
from datetime import datetime

# 添加项目根路径
sys.path.append('/Users/nxm/PycharmProjects/dataDig')

from src.config.settings import load_settings
from src.app_logging.logger import setup_logger
from src.db.mysql_client import MySQLClient
from src.strategy.strategies.combined_strategies.red_three_soldiers_strategy import (
    RedThreeSoldiersStrategy, 
    RedThreeSoldiersConfig
)
from src.strategy.services.strategy_service import StrategyService


def test_main_board_detection():
    """测试主板股票识别"""
    print("\n" + "="*60)
    print("测试1: 主板股票识别")
    print("="*60)
    
    config = RedThreeSoldiersConfig(initial_cash=100000.0)
    strategy = RedThreeSoldiersStrategy(config)
    
    test_cases = [
        ("000001.SZ", True, "深圳主板"),
        ("000858.SZ", True, "深圳主板"),
        ("002001.SZ", False, "深圳中小板"),
        ("300001.SZ", False, "创业板"),
        ("600036.SH", True, "上海主板"),
        ("601318.SH", True, "上海主板"),
        ("603000.SH", True, "上海主板"),
        ("605000.SH", True, "上海主板"),
        ("688001.SH", False, "科创板"),
    ]
    
    all_passed = True
    for symbol, expected, description in test_cases:
        result = strategy.is_main_board_stock(symbol)
        status = "✅" if result == expected else "❌"
        print(f"{status} {symbol:10} -> {result:5} (期望: {expected:5}) - {description}")
        if result != expected:
            all_passed = False
    
    print(f"\n主板股票识别测试: {'全部通过' if all_passed else '存在失败'}")
    return all_passed


def test_red_three_soldiers_pattern():
    """测试红三兵形态识别"""
    print("\n" + "="*60)
    print("测试2: 红三兵形态识别")
    print("="*60)
    
    config = RedThreeSoldiersConfig(initial_cash=100000.0)
    logger = setup_logger('INFO', 'logs', 'test_red_three_soldiers.log')
    strategy = RedThreeSoldiersStrategy(config, logger)
    
    # 测试用例1：标准红三兵
    print("\n测试用例1: 标准红三兵形态")
    bars1 = [
        pd.Series({
            'trade_date': '20240101',
            'open': 10.00, 'close': 10.50, 
            'high': 10.60, 'low': 9.90
        }),
        pd.Series({
            'trade_date': '20240102', 
            'open': 10.20, 'close': 10.80,
            'high': 10.90, 'low': 10.10
        }),
        pd.Series({
            'trade_date': '20240103',
            'open': 10.50, 'close': 11.10,
            'high': 11.20, 'low': 10.40
        })
    ]
    
    result1 = strategy.check_red_three_soldiers_pattern(bars1)
    print(f"结果: {result1} (期望: True)")
    print("详细信息:")
    for i, bar in enumerate(bars1, 1):
        print(f"  Day{i}: 开盘={bar['open']:5.2f}, 收盘={bar['close']:5.2f}, 涨幅={((bar['close']-bar['open'])/bar['open']*100):4.1f}%")
    
    # 测试用例2：非阳线（失败案例）
    print("\n测试用例2: 包含阴线（应该失败）")
    bars2 = [
        pd.Series({
            'trade_date': '20240101',
            'open': 10.00, 'close': 10.50, 
            'high': 10.60, 'low': 9.90
        }),
        pd.Series({
            'trade_date': '20240102',
            'open': 10.20, 'close': 10.00,  # 阴线
            'high': 10.30, 'low': 9.90
        }),
        pd.Series({
            'trade_date': '20240103',
            'open': 10.10, 'close': 10.40,
            'high': 10.50, 'low': 10.00
        })
    ]
    
    result2 = strategy.check_red_three_soldiers_pattern(bars2)
    print(f"结果: {result2} (期望: False)")
    
    # 测试用例3：收盘价未上涨（失败案例）
    print("\n测试用例3: 收盘价未阶梯上涨（应该失败）")
    bars3 = [
        pd.Series({
            'trade_date': '20240101',
            'open': 10.00, 'close': 10.50,
            'high': 10.60, 'low': 9.90
        }),
        pd.Series({
            'trade_date': '20240102',
            'open': 10.20, 'close': 10.40,  # 收盘价下降
            'high': 10.50, 'low': 10.10
        }),
        pd.Series({
            'trade_date': '20240103',
            'open': 10.30, 'close': 10.45,
            'high': 10.55, 'low': 10.20
        })
    ]
    
    result3 = strategy.check_red_three_soldiers_pattern(bars3)
    print(f"结果: {result3} (期望: False)")
    
    all_passed = result1 == True and result2 == False and result3 == False
    print(f"\n红三兵形态识别测试: {'全部通过' if all_passed else '存在失败'}")
    return all_passed


def run_red_three_soldiers_backtest():
    """运行红三兵策略回测"""
    print("\n" + "="*60)
    print("测试3: 红三兵策略完整回测")
    print("="*60)
    
    try:
        # 1. 初始化配置和日志
        settings = load_settings()
        logger = setup_logger('INFO', 'logs', 'red_three_soldiers_backtest.log')
        
        print("[初始化] 设置和日志初始化完成")
        
        # 2. 数据库连接
        mysql_client = MySQLClient(
            host=settings.database.host,
            port=settings.database.port,
            user=settings.database.user,
            password=settings.database.password,
            db_name=settings.database.name,
        )
        
        mysql_client.create_database_if_not_exists()
        SessionFactory = mysql_client.session_factory()
        session = SessionFactory()
        
        print("[初始化] 数据库连接建立成功")
        
        # 3. 创建策略服务
        strategy_service = StrategyService(session, logger)
        
        # 4. 策略配置
        strategy_config = {
            'initial_cash': 100000.0,
            'max_stocks': 5,  # 最多同时持有10只股票
            'position_per_stock': 0.2  # 每只股票10%仓位
        }
        
        # 5. 获取主板股票列表（限制数量用于测试）
        all_symbols = strategy_service.backtest_engine.get_available_symbols()
        
        # 过滤出主板股票
        config = RedThreeSoldiersConfig(initial_cash=100000.0)
        temp_strategy = RedThreeSoldiersStrategy(config)
        main_board_symbols = [s for s in all_symbols if temp_strategy.is_main_board_stock(s)]
        
        # 选择前50只主板股票进行测试
        test_symbols = main_board_symbols[:50]
        
        print(f"[回测配置] 全部股票数量: {len(all_symbols)}")
        print(f"[回测配置] 主板股票数量: {len(main_board_symbols)}")
        print(f"[回测配置] 测试股票数量: {len(test_symbols)}")
        print(f"[回测配置] 回测时间范围: 20240101 ~ 20250924")
        print(f"[回测配置] 初始资金: {strategy_config['initial_cash']:,.0f}")
        
        # 6. 运行回测
        print("\n[开始回测] 红三兵策略回测...")
        
        result = strategy_service.run_single_strategy_backtest(
            strategy_class=RedThreeSoldiersStrategy,
            strategy_config=strategy_config,
            symbols=test_symbols,
            start_date="20240101",
            end_date="20250924",
            commission_rate=0.001
        )
        
        # 7. 输出结果
        print("\n" + "="*50)
        print("红三兵策略回测结果")
        print("="*50)
        
        if result and result.summary:
            summary = result.summary
            print(f"回测期间: {summary.start_date} ~ {summary.end_date}")
            print(f"交易日数: {summary.trading_days}")
            print(f"初始资金: {summary.initial_cash:,.0f}")
            print(f"最终价值: {summary.final_value:,.0f}")
            print(f"总收益率: {summary.total_return:.2%}")
            print(f"年化收益率: {summary.annualized_return:.2%}")
            print(f"最大回撤: {summary.max_drawdown:.2%}")
            print(f"波动率: {summary.volatility:.2%}")
            print(f"夏普比率: {summary.sharpe_ratio:.2f}")
            print(f"交易次数: {summary.total_trades}")
            print(f"胜率: {summary.win_rate:.2%}")
            print(f"平均持仓天数: {summary.avg_holding_days:.1f}")
            
            # 8. 导出结果
            strategy_service.export_backtest_result(result, "results")
            
            print(f"\n[导出完成] 回测结果已导出到 results/ 目录")
            
        else:
            print("❌ 回测失败，未获得有效结果")
            return False
        
        session.close()
        print("\n[回测完成] 红三兵策略回测执行完毕")
        return True
        
    except Exception as e:
        print(f"❌ 回测过程中发生错误: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """主测试函数"""
    print("红三兵策略测试")
    print("="*60)
    print(f"测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 运行测试
    test_results = []
    
    # 测试1: 主板股票识别
    test_results.append(test_main_board_detection())
    
    # 测试2: 红三兵形态识别
    test_results.append(test_red_three_soldiers_pattern())
    
    # 测试3: 完整回测
    test_results.append(run_red_three_soldiers_backtest())
    
    # 总结
    print("\n" + "="*60)
    print("测试总结")
    print("="*60)
    print(f"主板股票识别: {'通过' if test_results[0] else '失败'}")
    print(f"红三兵形态识别: {'通过' if test_results[1] else '失败'}")
    print(f"完整策略回测: {'通过' if test_results[2] else '失败'}")
    
    all_passed = all(test_results)
    print(f"\n总体结果: {'全部测试通过 ✅' if all_passed else '存在测试失败 ❌'}")
    
    return all_passed


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
