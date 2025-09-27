from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import pandas as pd
from datetime import datetime


class TradingSignal:
    """交易信号"""
    BUY = "buy"
    SELL = "sell"
    HOLD = "hold"


class StrategyConfig:
    """策略配置基类"""
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


class PositionInfo:
    """持仓信息"""
    def __init__(self):
        self.symbol: str = ""  # 股票代码
        self.quantity: int = 0  # 持仓数量
        self.avg_price: float = 0.0  # 平均成本
        self.current_price: float = 0.0  # 当前价格
        self.unrealized_pnl: float = 0.0  # 浮动盈亏
        self.realized_pnl: float = 0.0  # 已实现盈亏


class BaseStrategy(ABC):
    """策略基类，所有策略都必须继承此类"""
    
    def __init__(self, config: StrategyConfig, logger=None):
        self.config = config
        self.logger = logger
        self.name = self.__class__.__name__
        self.positions: Dict[str, PositionInfo] = {}  # 当前持仓
        self.cash = getattr(config, 'initial_cash', 100000.0)  # 初始资金
        self.total_value = self.cash  # 总价值
        
        if self.logger:
            self.logger.info(f"[策略初始化] 策略名称={self.name}，初始资金={self.cash}")
    
    @abstractmethod
    def initialize(self) -> None:
        """
        策略初始化，用于设置策略参数、指标等
        在回测开始前调用一次
        """
        pass
    
    @abstractmethod  
    def on_bar(self, symbol: str, bar_data: pd.Series) -> str:
        """
        每个K线数据到来时的处理逻辑
        
        Args:
            symbol: 股票代码
            bar_data: K线数据（包含 open, high, low, close, vol, amount 等字段）
            
        Returns:
            交易信号: TradingSignal.BUY, TradingSignal.SELL, TradingSignal.HOLD
        """
        pass
    
    def should_buy(self, symbol: str, bar_data: pd.Series) -> bool:
        """
        判断是否应该买入
        子类可以重写此方法
        """
        signal = self.on_bar(symbol, bar_data)
        return signal == TradingSignal.BUY
    
    def should_sell(self, symbol: str, bar_data: pd.Series) -> bool:
        """
        判断是否应该卖出
        子类可以重写此方法
        """
        signal = self.on_bar(symbol, bar_data)
        return signal == TradingSignal.SELL
    
    def get_position_size(self, symbol: str, price: float) -> int:
        """
        计算买入数量，默认使用全部可用资金
        子类可以重写此方法实现仓位管理策略
        
        Args:
            symbol: 股票代码
            price: 当前价格
            
        Returns:
            买入数量（股数）
        """
        if hasattr(self.config, 'max_position_pct'):
            max_value = self.cash * self.config.max_position_pct
        else:
            max_value = self.cash * 0.95  # 默认95%仓位
            
        quantity = int(max_value / price / 100) * 100  # 按100股整数倍买入
        
        if self.logger:
            self.logger.info(f"[仓位计算] 股票={symbol}，价格={price}，可用资金={self.cash}，计算数量={quantity}")
        
        return quantity
    
    def update_position(self, symbol: str, quantity: int, price: float, 
                       action: str) -> None:
        """
        更新持仓信息
        
        Args:
            symbol: 股票代码
            quantity: 交易数量
            price: 交易价格
            action: 交易动作 (buy/sell)
        """
        if symbol not in self.positions:
            self.positions[symbol] = PositionInfo()
            self.positions[symbol].symbol = symbol
        
        pos = self.positions[symbol]
        
        if action == "buy":
            # 买入
            total_cost = pos.quantity * pos.avg_price + quantity * price
            total_quantity = pos.quantity + quantity
            if total_quantity > 0:
                pos.avg_price = total_cost / total_quantity
            pos.quantity = total_quantity
            self.cash -= quantity * price
            
            if self.logger:
                self.logger.info(f"[持仓更新] 买入 {symbol} {quantity}股，价格={price}，"
                               f"平均成本={pos.avg_price:.2f}，总持仓={pos.quantity}")
        
        elif action == "sell":
            # 卖出
            if quantity <= pos.quantity:
                sell_value = quantity * price
                sell_cost = quantity * pos.avg_price
                pos.realized_pnl += sell_value - sell_cost
                pos.quantity -= quantity
                self.cash += sell_value
                
                if self.logger:
                    profit = sell_value - sell_cost
                    self.logger.info(f"[持仓更新] 卖出 {symbol} {quantity}股，价格={price}，"
                                   f"盈亏={profit:.2f}，剩余持仓={pos.quantity}")
                
                # 如果全部卖出，清空持仓
                if pos.quantity == 0:
                    del self.positions[symbol]
    
    def get_current_value(self, current_prices: Dict[str, float]) -> float:
        """
        计算当前总资产价值
        
        Args:
            current_prices: 当前股票价格字典
            
        Returns:
            总资产价值
        """
        stock_value = 0.0
        for symbol, pos in self.positions.items():
            if symbol in current_prices:
                stock_value += pos.quantity * current_prices[symbol]
        
        return self.cash + stock_value
    
    def get_strategy_info(self) -> Dict[str, Any]:
        """
        获取策略基本信息
        """
        return {
            "name": self.name,
            "initial_cash": getattr(self.config, 'initial_cash', 100000.0),
            "current_cash": self.cash,
            "positions": len(self.positions),
            "config": self.config.__dict__ if hasattr(self.config, '__dict__') else {}
        }
