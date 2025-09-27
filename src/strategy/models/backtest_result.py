from datetime import datetime
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
import pandas as pd


@dataclass
class Trade:
    """单笔交易记录"""
    symbol: str  # 股票代码
    trade_date: str  # 交易日期
    action: str  # 买入/卖出 (buy/sell)
    quantity: int  # 交易数量
    price: float  # 交易价格
    amount: float  # 交易金额
    commission: float = 0.0  # 手续费
    
    def __post_init__(self):
        if self.amount == 0:
            self.amount = self.quantity * self.price


@dataclass
class DailyReturn:
    """每日收益记录"""
    trade_date: str  # 交易日期
    total_value: float  # 总资产价值
    cash: float  # 现金
    stock_value: float  # 股票市值
    daily_return: float  # 当日收益率
    cumulative_return: float  # 累计收益率
    positions: int  # 持仓股票数量


@dataclass
class BacktestSummary:
    """回测结果汇总"""
    strategy_name: str  # 策略名称
    start_date: str  # 回测开始日期
    end_date: str  # 回测结束日期
    initial_cash: float  # 初始资金
    final_value: float  # 最终价值
    total_return: float  # 总收益率
    annualized_return: float  # 年化收益率
    max_drawdown: float  # 最大回撤
    volatility: float  # 波动率
    sharpe_ratio: float  # 夏普比率
    total_trades: int  # 总交易次数
    win_rate: float  # 胜率
    avg_holding_days: float  # 平均持仓天数
    trading_days: int  # 交易日天数


@dataclass 
class BacktestResult:
    """完整的回测结果"""
    summary: BacktestSummary  # 回测汇总
    trades: List[Trade] = field(default_factory=list)  # 交易记录
    daily_returns: List[DailyReturn] = field(default_factory=list)  # 每日收益
    created_at: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            "summary": {
                "strategy_name": self.summary.strategy_name,
                "start_date": self.summary.start_date,
                "end_date": self.summary.end_date,
                "initial_cash": self.summary.initial_cash,
                "final_value": self.summary.final_value,
                "total_return": self.summary.total_return,
                "annualized_return": self.summary.annualized_return,
                "max_drawdown": self.summary.max_drawdown,
                "volatility": self.summary.volatility,
                "sharpe_ratio": self.summary.sharpe_ratio,
                "total_trades": self.summary.total_trades,
                "win_rate": self.summary.win_rate,
                "avg_holding_days": self.summary.avg_holding_days,
                "trading_days": self.summary.trading_days
            },
            "trades_count": len(self.trades),
            "daily_returns_count": len(self.daily_returns),
            "created_at": self.created_at.isoformat()
        }
    
    def get_trades_df(self) -> pd.DataFrame:
        """获取交易记录DataFrame"""
        if not self.trades:
            return pd.DataFrame()
        
        trades_data = []
        for trade in self.trades:
            trades_data.append({
                "symbol": trade.symbol,
                "trade_date": trade.trade_date,
                "action": trade.action,
                "quantity": trade.quantity,
                "price": trade.price,
                "amount": trade.amount,
                "commission": trade.commission
            })
        
        return pd.DataFrame(trades_data)
    
    def get_daily_returns_df(self) -> pd.DataFrame:
        """获取每日收益DataFrame"""
        if not self.daily_returns:
            return pd.DataFrame()
        
        returns_data = []
        for daily in self.daily_returns:
            returns_data.append({
                "trade_date": daily.trade_date,
                "total_value": daily.total_value,
                "cash": daily.cash,
                "stock_value": daily.stock_value,
                "daily_return": daily.daily_return,
                "cumulative_return": daily.cumulative_return,
                "positions": daily.positions
            })
        
        return pd.DataFrame(returns_data)
    
    def calculate_metrics(self, risk_free_rate: float = 0.03) -> None:
        """
        计算策略评价指标
        
        Args:
            risk_free_rate: 无风险利率，默认3%
        """
        if not self.daily_returns:
            return
        
        # 转换为DataFrame便于计算
        df = self.get_daily_returns_df()
        if df.empty:
            return
        
        # 计算各项指标
        returns = df['daily_return'].dropna()
        
        # 总收益率
        self.summary.total_return = (self.summary.final_value / self.summary.initial_cash) - 1
        
        # 年化收益率
        if self.summary.trading_days > 0:
            years = self.summary.trading_days / 252  # 假设一年252个交易日
            self.summary.annualized_return = (1 + self.summary.total_return) ** (1 / years) - 1
        
        # 最大回撤
        cumulative_returns = (1 + returns).cumprod()
        rolling_max = cumulative_returns.expanding().max()
        drawdowns = (cumulative_returns - rolling_max) / rolling_max
        self.summary.max_drawdown = drawdowns.min()
        
        # 波动率
        if len(returns) > 1:
            self.summary.volatility = returns.std() * (252 ** 0.5)  # 年化波动率
        
        # 夏普比率
        if self.summary.volatility > 0:
            excess_return = self.summary.annualized_return - risk_free_rate
            self.summary.sharpe_ratio = excess_return / self.summary.volatility
        
        # 交易相关统计
        if self.trades:
            # 胜率计算
            profitable_trades = 0
            total_paired_trades = 0
            
            # 按股票分组计算盈亏
            trade_pairs = {}
            for trade in self.trades:
                if trade.symbol not in trade_pairs:
                    trade_pairs[trade.symbol] = []
                trade_pairs[trade.symbol].append(trade)
            
            for symbol, symbol_trades in trade_pairs.items():
                # 按时间排序
                symbol_trades.sort(key=lambda x: x.trade_date)
                
                # 计算买卖配对的盈亏
                position = 0
                cost_basis = 0
                
                for trade in symbol_trades:
                    if trade.action == 'buy':
                        if position == 0:
                            cost_basis = trade.price
                        position += trade.quantity
                    elif trade.action == 'sell':
                        if position > 0:
                            profit = (trade.price - cost_basis) * min(trade.quantity, position)
                            if profit > 0:
                                profitable_trades += 1
                            total_paired_trades += 1
                            position -= trade.quantity
            
            if total_paired_trades > 0:
                self.summary.win_rate = profitable_trades / total_paired_trades
            
            self.summary.total_trades = len(self.trades)
