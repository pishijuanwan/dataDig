from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import select

from src.models.daily_price import DailyPrice, StockBasic
from src.strategy.models.base_strategy import BaseStrategy, StrategyConfig
from src.strategy.models.backtest_result import (
    BacktestResult, BacktestSummary, Trade, DailyReturn
)


class BacktestEngine:
    """策略回测引擎"""
    
    def __init__(self, session: Session, logger=None):
        self.session = session
        self.logger = logger
        
        if self.logger:
            self.logger.info("[回测引擎初始化] 回测引擎已初始化")
    
    def run_backtest(
        self,
        strategy: BaseStrategy,
        symbols: List[str],
        start_date: str,
        end_date: str,
        commission_rate: float = 0.0003
    ) -> BacktestResult:
        """
        运行策略回测
        
        Args:
            strategy: 策略实例
            symbols: 股票代码列表
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD  
            commission_rate: 手续费率，默认0.03%
            
        Returns:
            回测结果
        """
        if self.logger:
            self.logger.info(f"[开始回测] 策略={strategy.name}，股票数量={len(symbols)}，"
                           f"时间范围={start_date}~{end_date}")
        
        # 1. 初始化策略
        strategy.initialize()
        
        # 2. 加载历史数据
        price_data = self._load_price_data(symbols, start_date, end_date)
        if price_data.empty:
            if self.logger:
                self.logger.warning("[回测数据] 未找到符合条件的价格数据")
            return self._create_empty_result(strategy, start_date, end_date)
        
        # 3. 执行回测逻辑
        result = self._execute_backtest(
            strategy, price_data, start_date, end_date, commission_rate
        )
        
        if self.logger:
            self.logger.info(f"[回测完成] 策略={strategy.name}，总收益率={result.summary.total_return:.2%}，"
                           f"最大回撤={result.summary.max_drawdown:.2%}，交易次数={result.summary.total_trades}")
        
        return result
    
    def _load_price_data(self, symbols: List[str], start_date: str, end_date: str) -> pd.DataFrame:
        """
        加载价格数据
        
        Args:
            symbols: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            价格数据DataFrame
        """
        if self.logger:
            self.logger.info(f"[加载数据] 开始加载价格数据，股票={len(symbols)}只，时间范围={start_date}~{end_date}")
        
        # 查询价格数据
        stmt = select(DailyPrice).where(
            DailyPrice.ts_code.in_(symbols),
            DailyPrice.trade_date >= start_date,
            DailyPrice.trade_date <= end_date
        ).order_by(DailyPrice.trade_date, DailyPrice.ts_code)
        
        result = self.session.execute(stmt).scalars().all()
        
        if not result:
            return pd.DataFrame()
        
        # 转换为DataFrame
        data = []
        for record in result:
            data.append({
                'ts_code': record.ts_code,
                'trade_date': record.trade_date,
                'open': record.open,
                'high': record.high,
                'low': record.low,
                'close': record.close,
                'pre_close': record.pre_close,
                'change': record.change,
                'pct_chg': record.pct_chg,
                'vol': record.vol,
                'amount': record.amount
            })
        
        df = pd.DataFrame(data)
        
        if self.logger:
            self.logger.info(f"[加载数据] 成功加载{len(df)}条价格记录")
        
        return df
    
    def _execute_backtest(
        self,
        strategy: BaseStrategy,
        price_data: pd.DataFrame,
        start_date: str,
        end_date: str,
        commission_rate: float
    ) -> BacktestResult:
        """
        执行回测逻辑
        """
        trades = []
        daily_returns = []
        
        # 按交易日分组
        grouped = price_data.groupby('trade_date')
        trade_dates = sorted(grouped.groups.keys())
        
        initial_cash = strategy.cash
        previous_total_value = initial_cash
        
        if self.logger:
            self.logger.info(f"[执行回测] 共{len(trade_dates)}个交易日需要处理")
        
        for i, trade_date in enumerate(trade_dates):
            daily_data = grouped.get_group(trade_date)
            
            # 构建当日价格字典
            current_prices = {}
            for _, row in daily_data.iterrows():
                current_prices[row['ts_code']] = row['close']
            
            # 处理每只股票的交易信号
            for _, row in daily_data.iterrows():
                symbol = row['ts_code']
                
                # 调用策略获取交易信号
                signal = strategy.on_bar(symbol, row)
                
                # 处理买入信号
                if signal == "buy" and strategy.cash > 1000:  # 至少1000元才能买入
                    quantity = strategy.get_position_size(symbol, row['close'])
                    if quantity > 0:
                        amount = quantity * row['close']
                        commission = amount * commission_rate
                        total_cost = amount + commission
                        
                        if strategy.cash >= total_cost:
                            # 执行买入
                            strategy.update_position(symbol, quantity, row['close'], 'buy')
                            strategy.cash -= commission  # 扣除手续费
                            
                            trade = Trade(
                                symbol=symbol,
                                trade_date=trade_date,
                                action='buy',
                                quantity=quantity,
                                price=row['close'],
                                amount=amount,
                                commission=commission
                            )
                            trades.append(trade)
                            
                            if self.logger:
                                self.logger.info(f"[执行交易] {trade_date} 买入 {symbol} {quantity}股，"
                                               f"价格={row['close']:.2f}，手续费={commission:.2f}")
                
                # 处理卖出信号
                elif signal == "sell" and symbol in strategy.positions:
                    position = strategy.positions[symbol]
                    if position.quantity > 0:
                        quantity = position.quantity
                        amount = quantity * row['close']
                        commission = amount * commission_rate
                        net_amount = amount - commission
                        
                        # 执行卖出
                        strategy.update_position(symbol, quantity, row['close'], 'sell')
                        strategy.cash -= commission  # 扣除手续费
                        
                        trade = Trade(
                            symbol=symbol,
                            trade_date=trade_date,
                            action='sell',
                            quantity=quantity,
                            price=row['close'],
                            amount=amount,
                            commission=commission
                        )
                        trades.append(trade)
                        
                        if self.logger:
                            self.logger.info(f"[执行交易] {trade_date} 卖出 {symbol} {quantity}股，"
                                           f"价格={row['close']:.2f}，手续费={commission:.2f}")
            
            # 计算当日总资产价值
            total_value = strategy.get_current_value(current_prices)
            stock_value = total_value - strategy.cash
            
            # 计算收益率
            daily_return = (total_value - previous_total_value) / previous_total_value if previous_total_value > 0 else 0
            cumulative_return = (total_value - initial_cash) / initial_cash if initial_cash > 0 else 0
            
            daily_ret = DailyReturn(
                trade_date=trade_date,
                total_value=total_value,
                cash=strategy.cash,
                stock_value=stock_value,
                daily_return=daily_return,
                cumulative_return=cumulative_return,
                positions=len(strategy.positions)
            )
            daily_returns.append(daily_ret)
            
            previous_total_value = total_value
            
            # 定期记录进度
            if (i + 1) % 50 == 0 or i == len(trade_dates) - 1:
                if self.logger:
                    self.logger.info(f"[回测进度] 已处理{i + 1}/{len(trade_dates)}个交易日，"
                                   f"当前总价值={total_value:.2f}，累计收益率={cumulative_return:.2%}")
        
        # 创建回测结果
        summary = BacktestSummary(
            strategy_name=strategy.name,
            start_date=start_date,
            end_date=end_date,
            initial_cash=initial_cash,
            final_value=total_value,
            total_return=cumulative_return,
            annualized_return=0.0,  # 后续计算
            max_drawdown=0.0,  # 后续计算
            volatility=0.0,  # 后续计算
            sharpe_ratio=0.0,  # 后续计算
            total_trades=len(trades),
            win_rate=0.0,  # 后续计算
            avg_holding_days=0.0,  # 后续计算
            trading_days=len(trade_dates)
        )
        
        result = BacktestResult(
            summary=summary,
            trades=trades,
            daily_returns=daily_returns
        )
        
        # 计算详细指标
        result.calculate_metrics()
        
        return result
    
    def _create_empty_result(self, strategy: BaseStrategy, start_date: str, end_date: str) -> BacktestResult:
        """创建空的回测结果"""
        summary = BacktestSummary(
            strategy_name=strategy.name,
            start_date=start_date,
            end_date=end_date,
            initial_cash=strategy.cash,
            final_value=strategy.cash,
            total_return=0.0,
            annualized_return=0.0,
            max_drawdown=0.0,
            volatility=0.0,
            sharpe_ratio=0.0,
            total_trades=0,
            win_rate=0.0,
            avg_holding_days=0.0,
            trading_days=0
        )
        
        return BacktestResult(summary=summary)
    
    def get_available_symbols(self, limit: int = None) -> List[str]:
        """
        获取可用的股票代码列表
        
        Args:
            limit: 限制返回数量
            
        Returns:
            股票代码列表
        """
        stmt = select(StockBasic.ts_code).where(
            StockBasic.list_status == 'L'  # 只选择正常上市的股票
        )
        
        if limit:
            stmt = stmt.limit(limit)
        
        result = self.session.execute(stmt).scalars().all()
        
        if self.logger:
            self.logger.info(f"[获取股票池] 共找到{len(result)}只可用股票")
        
        return list(result)
