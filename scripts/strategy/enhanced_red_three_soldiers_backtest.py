#!/usr/bin/env python3
"""
增强版红三兵策略回测脚本

新增条件：
1. 前三天的成交量必须持续变大
2. 每日的收盘价-开盘价的值必须达到最高价-最低价的50%

回测范围：沪深主板所有股票
"""

import sys
import os
from datetime import datetime

# 添加项目根路径
sys.path.append('/Users/nxm/PycharmProjects/dataDig')

from src.config.settings import load_settings
from src.app_logging.logger import setup_logger
from src.db.mysql_client import MySQLClient
from src.strategy.services.strategy_service import StrategyService
from src.strategy.strategies.buy_strategies.red_three_soldiers_strategy import (
    RedThreeSoldiersStrategy, 
    RedThreeSoldiersConfig
)
from src.strategy.strategies.sell_strategies.drop_stop_loss_strategy import DropStopLossStrategy, DropStopLossConfig


def get_main_board_symbols(strategy_service: StrategyService) -> list:
    """
    获取沪深主板股票列表
    
    Args:
        strategy_service: 策略服务
        
    Returns:
        主板股票代码列表
    """
    # 获取所有可用股票
    all_symbols = strategy_service.backtest_engine.get_available_symbols()
    
    if not all_symbols:
        return []
    
    # 创建临时策略实例用于筛选主板股票
    temp_config = RedThreeSoldiersConfig()
    temp_strategy = RedThreeSoldiersStrategy(temp_config)
    
    # 筛选主板股票
    main_board_symbols = []
    for symbol in all_symbols:
        if temp_strategy.is_main_board_stock(symbol):
            main_board_symbols.append(symbol)
    
    return main_board_symbols


def run_enhanced_red_three_soldiers_backtest():
    """运行增强版红三兵策略回测"""
    
    print("增强版红三兵策略回测 (分离策略版本)")
    print("=" * 80)
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    print("🔥 买入策略 - 红三兵增强版:")
    print("1. ✅ 连续三天均为阳线")
    print("2. ✅ 开盘价呈阶梯式包容")
    print("3. ✅ 收盘价呈阶梯式上涨")
    print("4. 🆕 成交量超过前5天最高值50%")
    print("5. 🆕 每日实体比例≥50%")
    print("6. 🆕 每日涨幅≥1%")
    print("7. ✅ 仅限沪深主板股票")
    print()
    print("🛡️ 卖出策略 - 下跌止损:")
    print("1. 🆕 总体跌幅>3%：强制止损")
    print("2. 🆕 当日跌幅>2%：触发止损")
    print("3. ✅ 当日上涨：继续持有")
    print("=" * 80)
    
    try:
        # 1. 初始化配置和日志
        settings = load_settings()
        logger = setup_logger('INFO', '/Users/nxm/PycharmProjects/dataDig/logs', 'enhanced_red_three_soldiers_backtest.log')
        logger.info("[增强版红三兵回测] 开始运行增强版红三兵策略回测 (分离策略版本)")
        logger.info("[增强版红三兵回测] 买入策略: 红三兵增强版")
        logger.info("[增强版红三兵回测] 卖出策略: 下跌止损策略")
        
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
        
        logger.info("[增强版红三兵回测] 数据库连接建立成功")
        
        # 3. 创建策略服务
        strategy_service = StrategyService(session, logger)
        
        # 4. 获取主板股票列表
        main_board_symbols = get_main_board_symbols(strategy_service)
        
        if not main_board_symbols:
            print("❌ 未找到主板股票数据")
            logger.error("[增强版红三兵回测] 未找到主板股票数据")
            return False
        
        print(f"📊 数据统计:")
        print(f"   - 主板股票数量: {len(main_board_symbols)}只")
        print(f"   - 回测时间范围: 20240101 ~ 20250927")
        print(f"   - 预计回测时长: 约{len(main_board_symbols) * 2 // 60}分钟")
        print()
        
        print("\n🚀 开始执行增强版红三兵策略回测...")
        
        # 5. 策略配置
        buy_strategy_config = {
            'initial_cash': 200000.0,    # 100万初始资金
            'max_stocks': 10,             # 最多同时持有50只股票
            'position_per_stock': 0.1    # 每只股票分配2%资金
        }
        
        sell_strategy_config = {
            'initial_cash': 200000.0,          # 保持一致的初始资金设置
            'daily_stop_loss_threshold': 0.02,  # 当日下跌2%止损
            'total_loss_threshold': 0.03         # 总体跌幅3%强制止损
        }
        
        logger.info(f"[增强版红三兵回测] 主板股票数量: {len(main_board_symbols)}")
        logger.info(f"[增强版红三兵回测] 初始资金: {buy_strategy_config['initial_cash']:,.0f}")
        
        # 6. 运行回测 (使用分离的买入和卖出策略)
        result = strategy_service.run_single_strategy_backtest(
            buy_strategy_class=RedThreeSoldiersStrategy,
            buy_strategy_config=buy_strategy_config,
            sell_strategy_class=DropStopLossStrategy,
            sell_strategy_config=sell_strategy_config,
            symbols=main_board_symbols,  # 使用所有主板股票
            start_date="20240101",
            end_date="20250927",
            commission_rate=0.002  # 0.2% 手续费
        )
        
        # 7. 输出详细结果
        print("\n" + "="*80)
        print("🎯 增强版红三兵策略回测结果 (分离策略版本)")
        print("🔥 买入策略: 红三兵增强版 | 🛡️ 卖出策略: 下跌止损")
        print("="*80)
        
        if result and result.summary:
            summary = result.summary
            
            # 基本信息
            print(f"📈 基本信息:")
            print(f"   策略名称: {summary.strategy_name}")
            print(f"   回测期间: {summary.start_date} ~ {summary.end_date}")
            print(f"   交易日数: {summary.trading_days}天")
            print(f"   股票池: 主板股票 {len(main_board_symbols)}只")
            print()
            
            # 收益指标
            print(f"💰 收益指标:")
            print(f"   初始资金: {summary.initial_cash:,.0f}元")
            print(f"   最终价值: {summary.final_value:,.0f}元")
            print(f"   绝对收益: {summary.final_value - summary.initial_cash:,.0f}元")
            print(f"   总收益率: {summary.total_return:.2%}")
            print(f"   年化收益率: {summary.annualized_return:.2%}")
            print()
            
            # 风险指标  
            print(f"⚠️  风险指标:")
            print(f"   最大回撤: {summary.max_drawdown:.2%}")
            print(f"   波动率: {summary.volatility:.2%}")
            print(f"   夏普比率: {summary.sharpe_ratio:.2f}")
            print()
            
            # 交易指标
            print(f"📊 交易指标:")
            print(f"   总交易次数: {summary.total_trades}笔")
            print(f"   胜率: {summary.win_rate:.2%}")
            print(f"   平均持仓天数: {summary.avg_holding_days:.1f}天")
            print()
            
            # 分析交易记录
            trades_df = result.get_trades_df()
            if not trades_df.empty:
                buy_trades = trades_df[trades_df['action'] == 'buy']
                sell_trades = trades_df[trades_df['action'] == 'sell']
                unique_symbols = trades_df['symbol'].nunique()
                
                print(f"🔍 交易分析:")
                print(f"   买入交易: {len(buy_trades)}笔")
                print(f"   卖出交易: {len(sell_trades)}笔")
                print(f"   涉及股票: {unique_symbols}只")
                
                if len(buy_trades) > 0:
                    avg_buy_price = buy_trades['price'].mean()
                    print(f"   平均买入价: {avg_buy_price:.2f}元")
                
                if len(sell_trades) > 0:
                    avg_sell_price = sell_trades['price'].mean()
                    print(f"   平均卖出价: {avg_sell_price:.2f}元")
                
                # 显示最活跃的股票
                if len(trades_df) > 0:
                    symbol_counts = trades_df['symbol'].value_counts().head(10)
                    print(f"\n   交易最频繁的10只股票:")
                    for i, (symbol, count) in enumerate(symbol_counts.items(), 1):
                        print(f"     {i:2d}. {symbol}: {count}笔")
                print()
            
            # 8. 导出结果
            print("💾 导出回测结果...")
            exported_files = strategy_service.export_backtest_result(
                result, 
                "/Users/nxm/PycharmProjects/dataDig/results"
            )
            
            if exported_files:
                print("   已导出文件:")
                for file_type, file_path in exported_files.items():
                    file_name = os.path.basename(file_path)
                    print(f"     - {file_type}: {file_name}")
                logger.info(f"[增强版红三兵回测] 回测结果已导出")
            
        else:
            print("❌ 回测失败，未获得有效结果")
            logger.error("[增强版红三兵回测] 回测失败")
            return False
        
        session.close()
        
        print("\n" + "="*80)
        print("✅ 增强版红三兵策略回测完成")
        print(f"完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80)
        
        return True
        
    except Exception as e:
        print(f"❌ 回测过程中发生错误: {str(e)}")
        logger.error(f"[增强版红三兵回测] 回测过程中发生错误: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """主函数"""
    success = run_enhanced_red_three_soldiers_backtest()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
