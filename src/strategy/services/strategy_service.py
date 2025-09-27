from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Type
import pandas as pd
from sqlalchemy.orm import Session

from src.strategy.models.base_strategy import BaseStrategy, StrategyConfig
from src.strategy.models.backtest_result import BacktestResult
from src.strategy.engines.backtest_engine import BacktestEngine


class StrategyService:
    """策略服务层，提供策略回测的高级接口"""
    
    def __init__(self, session: Session, logger=None):
        self.session = session
        self.logger = logger
        self.backtest_engine = BacktestEngine(session, logger)
        
        if self.logger:
            self.logger.info("[策略服务初始化] 策略服务已初始化")
    
    def run_single_strategy_backtest(
        self,
        strategy_class: Type[BaseStrategy] = None,
        strategy_config: Dict[str, Any] = None,
        buy_strategy_class: Type[BaseStrategy] = None,
        buy_strategy_config: Dict[str, Any] = None,
        sell_strategy_class: Type[BaseStrategy] = None,
        sell_strategy_config: Dict[str, Any] = None,
        symbols: List[str] = None,
        start_date: str = "20240101",
        end_date: str = None,
        commission_rate: float = 0.002
    ) -> BacktestResult:
        """
        运行策略回测（支持买入卖出策略分离）
        
        Args:
            strategy_class: 策略类（兼容旧接口，用于组合策略）
            strategy_config: 策略配置（兼容旧接口）
            buy_strategy_class: 买入策略类
            buy_strategy_config: 买入策略配置
            sell_strategy_class: 卖出策略类
            sell_strategy_config: 卖出策略配置
            symbols: 股票代码列表，如果为空则使用默认股票池
            start_date: 开始日期 YYYYMMDD，默认2024年开始
            end_date: 结束日期 YYYYMMDD，默认到今天
            commission_rate: 手续费率，默认0.2%
            
        Returns:
            回测结果
            
        Notes:
            - 新接口：提供buy_strategy_class/sell_strategy_class进行策略分离
            - 兼容接口：提供strategy_class作为组合策略（同时用于买入卖出）
            - 两种方式至少要提供一种
        """
        # 参数验证和策略创建
        buy_strategy = None
        sell_strategy = None
        
        # 处理兼容接口（旧接口）
        if strategy_class and strategy_config:
            if self.logger:
                self.logger.info(f"[单策略回测] 使用组合策略={strategy_class.__name__}")
            config = StrategyConfig(**strategy_config)
            combined_strategy = strategy_class(config, logger=self.logger)
            # 组合策略同时用作买入和卖出策略
            buy_strategy = combined_strategy
            sell_strategy = combined_strategy
        
        # 处理新接口（分离策略）
        if buy_strategy_class and buy_strategy_config:
            if self.logger:
                self.logger.info(f"[单策略回测] 创建买入策略={buy_strategy_class.__name__}")
            buy_config = StrategyConfig(**buy_strategy_config)
            buy_strategy = buy_strategy_class(buy_config, logger=self.logger)
        
        if sell_strategy_class and sell_strategy_config:
            if self.logger:
                self.logger.info(f"[单策略回测] 创建卖出策略={sell_strategy_class.__name__}")
            sell_config = StrategyConfig(**sell_strategy_config)
            sell_strategy = sell_strategy_class(sell_config, logger=self.logger)
        
        # 验证至少有一个策略
        if not buy_strategy and not sell_strategy:
            raise ValueError("必须提供买入策略或卖出策略中的至少一个")
        
        # 1. 处理默认参数
        if end_date is None:
            end_date = datetime.now().strftime("%Y%m%d")
        
        if symbols is None or len(symbols) == 0:
            symbols = self._get_default_symbols()
            if self.logger:
                self.logger.info(f"[股票池] 使用默认股票池，数量={len(symbols)}")
        
        # 3. 执行回测
        result = self.backtest_engine.run_backtest(
            buy_strategy=buy_strategy,
            sell_strategy=sell_strategy,
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            commission_rate=commission_rate
        )
        
        # 确定策略名称用于日志
        strategy_names = []
        if buy_strategy:
            strategy_names.append(f"买入:{buy_strategy.name}")
        if sell_strategy and sell_strategy != buy_strategy:
            strategy_names.append(f"卖出:{sell_strategy.name}")
        strategy_desc = " + ".join(strategy_names)
        
        if self.logger:
            self.logger.info(f"[单策略回测完成] 策略={strategy_desc}，"
                           f"总收益率={result.summary.total_return:.2%}，"
                           f"年化收益率={result.summary.annualized_return:.2%}，"
                           f"最大回撤={result.summary.max_drawdown:.2%}")
        
        return result
    
    def run_strategy_comparison(
        self,
        strategies: List[Dict[str, Any]],
        symbols: List[str] = None,
        start_date: str = "20200101",
        end_date: str = None,
        commission_rate: float = 0.0003
    ) -> Dict[str, BacktestResult]:
        """
        运行多个策略的对比回测
        
        Args:
            strategies: 策略列表，每个元素包含 'class', 'config', 'name' 字段
            symbols: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期  
            commission_rate: 手续费率
            
        Returns:
            策略名称到回测结果的映射
        """
        if self.logger:
            self.logger.info(f"[策略对比] 开始对比{len(strategies)}个策略")
        
        results = {}
        
        for strategy_info in strategies:
            strategy_name = strategy_info.get('name', strategy_info['class'].__name__)
            
            try:
                if self.logger:
                    self.logger.info(f"[策略对比] 正在回测策略: {strategy_name}")
                
                result = self.run_single_strategy_backtest(
                    strategy_class=strategy_info['class'],
                    strategy_config=strategy_info['config'],
                    symbols=symbols,
                    start_date=start_date,
                    end_date=end_date,
                    commission_rate=commission_rate
                )
                
                results[strategy_name] = result
                
                if self.logger:
                    self.logger.info(f"[策略对比] 策略{strategy_name}回测完成，"
                                   f"收益率={result.summary.total_return:.2%}")
                
            except Exception as e:
                if self.logger:
                    self.logger.error(f"[策略对比] 策略{strategy_name}回测失败: {str(e)}")
                continue
        
        if self.logger:
            self.logger.info(f"[策略对比完成] 成功回测{len(results)}个策略")
        
        return results
    
    def get_performance_comparison(
        self,
        results: Dict[str, BacktestResult]
    ) -> pd.DataFrame:
        """
        获取策略性能对比表
        
        Args:
            results: 策略回测结果字典
            
        Returns:
            性能对比DataFrame
        """
        if not results:
            return pd.DataFrame()
        
        comparison_data = []
        
        for strategy_name, result in results.items():
            summary = result.summary
            comparison_data.append({
                '策略名称': strategy_name,
                '总收益率': f"{summary.total_return:.2%}",
                '年化收益率': f"{summary.annualized_return:.2%}",
                '最大回撤': f"{summary.max_drawdown:.2%}",
                '夏普比率': f"{summary.sharpe_ratio:.2f}",
                '波动率': f"{summary.volatility:.2%}",
                '交易次数': summary.total_trades,
                '胜率': f"{summary.win_rate:.2%}",
                '初始资金': f"{summary.initial_cash:,.0f}",
                '最终价值': f"{summary.final_value:,.0f}",
                '交易日数': summary.trading_days
            })
        
        df = pd.DataFrame(comparison_data)
        
        if self.logger:
            self.logger.info(f"[性能对比] 生成{len(df)}个策略的性能对比表")
        
        return df
    
    def _get_default_symbols(self, max_symbols: int = 50) -> List[str]:
        """
        获取默认的股票池（选择一些主要的股票）
        
        Args:
            max_symbols: 最大股票数量
            
        Returns:
            股票代码列表
        """
        # 可以从数据库获取，这里先使用一些知名股票作为示例
        all_symbols = self.backtest_engine.get_available_symbols(limit=max_symbols)
        
        if not all_symbols:
            # 如果数据库为空，返回一些默认的股票代码
            default_symbols = [
                "000001.SZ",  # 平安银行
                "000002.SZ",  # 万科A
                "000858.SZ",  # 五粮液
                "600036.SH",  # 招商银行
                "600519.SH",  # 贵州茅台
                "600000.SH",  # 浦发银行
            ]
            return default_symbols[:max_symbols]
        
        return all_symbols
    
    def create_simple_ma_strategy_config(
        self,
        short_window: int = 5,
        long_window: int = 20,
        initial_cash: float = 100000.0,
        max_position_pct: float = 0.95
    ) -> Dict[str, Any]:
        """
        创建简单移动平均策略的配置
        
        Args:
            short_window: 短期均线窗口
            long_window: 长期均线窗口
            initial_cash: 初始资金
            max_position_pct: 最大仓位百分比
            
        Returns:
            策略配置字典
        """
        return {
            'short_window': short_window,
            'long_window': long_window,
            'initial_cash': initial_cash,
            'max_position_pct': max_position_pct
        }
    
    def print_backtest_summary(self, result: BacktestResult) -> None:
        """
        打印回测结果摘要
        
        Args:
            result: 回测结果
        """
        summary = result.summary
        
        print(f"\\n========== {summary.strategy_name} 回测结果 ==========")
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
        print("="*50)
        
        if self.logger:
            self.logger.info(f"[结果摘要] {summary.strategy_name} 回测结果已输出")
    
    def export_backtest_result(
        self, 
        result: BacktestResult, 
        output_dir: str = "/Users/nxm/PycharmProjects/dataDig/results"
    ) -> Dict[str, str]:
        """
        导出回测结果到文件
        
        Args:
            result: 回测结果
            output_dir: 输出目录
            
        Returns:
            导出的文件路径字典
        """
        import os
        
        # 创建输出目录
        os.makedirs(output_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        strategy_name = result.summary.strategy_name
        
        exported_files = {}
        
        try:
            # 导出交易记录
            trades_df = result.get_trades_df()
            if not trades_df.empty:
                trades_file = os.path.join(output_dir, f"{strategy_name}_trades_{timestamp}.csv")
                trades_df.to_csv(trades_file, index=False, encoding='utf-8-sig')
                exported_files['trades'] = trades_file
            
            # 导出每日收益
            daily_df = result.get_daily_returns_df()
            if not daily_df.empty:
                daily_file = os.path.join(output_dir, f"{strategy_name}_daily_{timestamp}.csv")
                daily_df.to_csv(daily_file, index=False, encoding='utf-8-sig')
                exported_files['daily_returns'] = daily_file
            
            # 导出摘要信息
            summary_file = os.path.join(output_dir, f"{strategy_name}_summary_{timestamp}.txt")
            with open(summary_file, 'w', encoding='utf-8') as f:
                summary = result.summary
                f.write(f"{summary.strategy_name} 回测结果摘要\\n")
                f.write("="*50 + "\\n")
                f.write(f"回测期间: {summary.start_date} ~ {summary.end_date}\\n")
                f.write(f"交易日数: {summary.trading_days}\\n")
                f.write(f"初始资金: {summary.initial_cash:,.0f}\\n")
                f.write(f"最终价值: {summary.final_value:,.0f}\\n")
                f.write(f"总收益率: {summary.total_return:.2%}\\n")
                f.write(f"年化收益率: {summary.annualized_return:.2%}\\n")
                f.write(f"最大回撤: {summary.max_drawdown:.2%}\\n")
                f.write(f"波动率: {summary.volatility:.2%}\\n")
                f.write(f"夏普比率: {summary.sharpe_ratio:.2f}\\n")
                f.write(f"交易次数: {summary.total_trades}\\n")
                f.write(f"胜率: {summary.win_rate:.2%}\\n")
            
            exported_files['summary'] = summary_file
            
            if self.logger:
                self.logger.info(f"[导出结果] 回测结果已导出到 {output_dir}")
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"[导出失败] 导出回测结果时发生错误: {str(e)}")
        
        return exported_files
