from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import select

from src.models.daily_price import DailyPrice, StockBasic
from src.strategy.models.base_strategy import BaseStrategy, StrategyConfig, PositionInfo
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
        buy_strategy: Optional[BaseStrategy] = None,
        sell_strategy: Optional[BaseStrategy] = None,
        symbols: List[str] = None,
        start_date: str = "20240101",
        end_date: str = None,
        commission_rate: float = 0.0003
    ) -> BacktestResult:
        """
        运行策略回测（支持买入卖出策略分离）
        
        Args:
            buy_strategy: 买入策略实例，负责生成买入信号
            sell_strategy: 卖出策略实例，负责生成卖出信号
            symbols: 股票代码列表
            start_date: 开始日期 YYYYMMDD，默认2024年开始
            end_date: 结束日期 YYYYMMDD，默认到今天
            commission_rate: 手续费率，默认0.03%
            
        Returns:
            回测结果
            
        Notes:
            - buy_strategy和sell_strategy至少要提供一个
            - 对于组合策略，可以将同一个策略同时传给buy_strategy和sell_strategy
            - 如果只有buy_strategy，则不会有卖出操作
            - 如果只有sell_strategy，则不会有买入操作
        """
        # 参数验证
        if not buy_strategy and not sell_strategy:
            raise ValueError("至少需要提供买入策略或卖出策略中的一个")
        
        if symbols is None:
            symbols = []
        
        if end_date is None:
            end_date = datetime.now().strftime("%Y%m%d")
        
        # 确定策略名称用于日志
        strategy_names = []
        if buy_strategy:
            strategy_names.append(f"买入:{buy_strategy.name}")
        if sell_strategy and sell_strategy != buy_strategy:
            strategy_names.append(f"卖出:{sell_strategy.name}")
        strategy_desc = " + ".join(strategy_names)
        
        if self.logger:
            self.logger.info(f"[开始回测] 策略={strategy_desc}，股票数量={len(symbols)}，"
                           f"时间范围={start_date}~{end_date}")
        
        # 1. 初始化策略
        if buy_strategy:
            buy_strategy.initialize()
            if self.logger:
                self.logger.info(f"[策略初始化] 买入策略 {buy_strategy.name} 初始化完成")
        
        if sell_strategy and sell_strategy != buy_strategy:
            sell_strategy.initialize()
            if self.logger:
                self.logger.info(f"[策略初始化] 卖出策略 {sell_strategy.name} 初始化完成")
        
        # 2. 加载历史数据
        price_data = self._load_price_data(symbols, start_date, end_date)
        if price_data.empty:
            if self.logger:
                self.logger.warning("[回测数据] 未找到符合条件的价格数据")
            # 使用买入策略作为主策略创建空结果
            main_strategy = buy_strategy or sell_strategy
            return self._create_empty_result(main_strategy, start_date, end_date)
        
        # 3. 执行回测逻辑
        result = self._execute_backtest(
            buy_strategy, sell_strategy, price_data, start_date, end_date, commission_rate
        )
        
        if self.logger:
            self.logger.info(f"[回测完成] 策略={strategy_desc}，总收益率={result.summary.total_return:.2%}，"
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
        buy_strategy: Optional[BaseStrategy],
        sell_strategy: Optional[BaseStrategy],
        price_data: pd.DataFrame,
        start_date: str,
        end_date: str,
        commission_rate: float
    ) -> BacktestResult:
        """
        执行回测逻辑（支持分离的买入卖出策略）
        """
        trades = []
        daily_returns = []
        
        # 按交易日分组
        grouped = price_data.groupby('trade_date')
        trade_dates = sorted(grouped.groups.keys())
        
        # 使用买入策略作为主策略管理资金和持仓，如果没有买入策略则使用卖出策略
        main_strategy = buy_strategy or sell_strategy
        initial_cash = main_strategy.cash
        previous_total_value = initial_cash
        
        # 统一的资金和持仓管理
        current_cash = initial_cash
        positions = {}  # 统一管理持仓 {symbol: PositionInfo}
        
        if self.logger:
            self.logger.info(f"[执行回测] 共{len(trade_dates)}个交易日需要处理，"
                           f"初始资金={initial_cash:,.0f}，主策略={main_strategy.name}")
        
        for i, trade_date in enumerate(trade_dates):
            daily_data = grouped.get_group(trade_date)
            
            # 构建当日价格字典
            current_prices = {}
            for _, row in daily_data.iterrows():
                current_prices[row['ts_code']] = row['close']
            
            # 处理每只股票的交易信号
            for _, row in daily_data.iterrows():
                symbol = row['ts_code']
                
                # 处理买入信号（如果有买入策略）
                if buy_strategy and current_cash > 1000:  # 至少1000元才能买入
                    if buy_strategy.should_buy(symbol, row):
                        # 使用买入策略计算仓位大小
                        buy_strategy.cash = current_cash  # 临时更新现金状态
                        quantity = buy_strategy.get_position_size(symbol, row['close'])
                        if quantity > 0:
                            amount = quantity * row['close']
                            commission = amount * commission_rate
                            total_cost = amount + commission
                            
                            if current_cash >= total_cost:
                                # 执行买入 - 更新统一持仓管理
                                if symbol not in positions:
                                    positions[symbol] = PositionInfo()
                                    positions[symbol].symbol = symbol
                                
                                pos = positions[symbol]
                                total_cost_shares = pos.quantity * pos.avg_price + quantity * row['close']
                                total_quantity = pos.quantity + quantity
                                if total_quantity > 0:
                                    pos.avg_price = total_cost_shares / total_quantity
                                pos.quantity = total_quantity
                                current_cash -= total_cost
                                
                                # 🔧 修复：同步更新买入策略的内部状态
                                buy_strategy.cash = current_cash
                                buy_strategy.update_position(symbol, quantity, row['close'], 'buy')
                                
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
                
                # 处理卖出信号（如果有卖出策略且有持仓）
                if sell_strategy and symbol in positions:
                    position = positions[symbol]
                    if position.quantity > 0 and sell_strategy.should_sell(symbol, row):
                        quantity = position.quantity
                        amount = quantity * row['close']
                        commission = amount * commission_rate
                        net_amount = amount - commission
                        
                        # 执行卖出 - 更新统一持仓管理
                        sell_value = quantity * row['close']
                        sell_cost = quantity * position.avg_price
                        position.realized_pnl += sell_value - sell_cost
                        position.quantity = 0
                        current_cash += net_amount
                        
                        # 🔧 修复：同步更新卖出策略的内部状态
                        sell_strategy.cash = current_cash
                        sell_strategy.update_position(symbol, quantity, row['close'], 'sell')
                        
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
                        
                        # 如果全部卖出，清空持仓
                        del positions[symbol]
                        
                        if self.logger:
                            profit = sell_value - sell_cost
                            self.logger.info(f"[执行交易] {trade_date} 卖出 {symbol} {quantity}股，"
                                           f"价格={row['close']:.2f}，盈亏={profit:.2f}，手续费={commission:.2f}")
            
            # 计算当日总资产价值
            stock_value = 0.0
            for symbol, pos in positions.items():
                if symbol in current_prices:
                    stock_value += pos.quantity * current_prices[symbol]
            
            total_value = current_cash + stock_value
            
            # 计算收益率
            daily_return = (total_value - previous_total_value) / previous_total_value if previous_total_value > 0 else 0
            cumulative_return = (total_value - initial_cash) / initial_cash if initial_cash > 0 else 0
            
            daily_ret = DailyReturn(
                trade_date=trade_date,
                total_value=total_value,
                cash=current_cash,
                stock_value=stock_value,
                daily_return=daily_return,
                cumulative_return=cumulative_return,
                positions=len(positions)
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
            strategy_name=main_strategy.name,
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
