import pandas as pd
from typing import Dict
from collections import defaultdict

from src.strategy.models.base_strategy import BaseStrategy, StrategyConfig, TradingSignal


class SimpleMAStrategyConfig(StrategyConfig):
    """简单移动平均策略配置"""
    def __init__(self, short_window: int = 5, long_window: int = 20, **kwargs):
        super().__init__(**kwargs)
        self.short_window = short_window  # 短期均线窗口
        self.long_window = long_window    # 长期均线窗口


class SimpleMAStrategy(BaseStrategy):
    """
    简单移动平均策略
    
    策略规则：
    1. 当短期移动平均线上穿长期移动平均线时买入
    2. 当短期移动平均线下穿长期移动平均线时卖出
    3. 每只股票最多持有一个仓位
    """
    
    def __init__(self, config: SimpleMAStrategyConfig, logger=None):
        super().__init__(config, logger)
        self.price_history: Dict[str, list] = defaultdict(list)  # 存储价格历史
        self.ma_short: Dict[str, float] = {}  # 短期均线
        self.ma_long: Dict[str, float] = {}   # 长期均线
        self.last_signal: Dict[str, str] = {}  # 上次信号，用于判断穿越
        
        if self.logger:
            self.logger.info(f"[策略初始化] SimpleMA策略初始化，短期窗口={config.short_window}，"
                           f"长期窗口={config.long_window}")
    
    def initialize(self) -> None:
        """策略初始化"""
        if self.logger:
            self.logger.info("[策略初始化] SimpleMA策略初始化完成")
    
    def on_bar(self, symbol: str, bar_data: pd.Series) -> str:
        """
        处理每个K线数据
        
        Args:
            symbol: 股票代码
            bar_data: K线数据
            
        Returns:
            交易信号
        """
        current_price = bar_data['close']
        
        # 更新价格历史
        self.price_history[symbol].append(current_price)
        
        # 保持价格历史长度不超过长期窗口的2倍（节省内存）
        max_length = self.config.long_window * 2
        if len(self.price_history[symbol]) > max_length:
            self.price_history[symbol] = self.price_history[symbol][-max_length:]
        
        prices = self.price_history[symbol]
        
        # 计算移动平均线
        if len(prices) >= self.config.short_window:
            self.ma_short[symbol] = sum(prices[-self.config.short_window:]) / self.config.short_window
        
        if len(prices) >= self.config.long_window:
            self.ma_long[symbol] = sum(prices[-self.config.long_window:]) / self.config.long_window
        
        # 如果数据不足，返回持有信号
        if len(prices) < self.config.long_window:
            return TradingSignal.HOLD
        
        # 获取当前均线值
        ma_short = self.ma_short[symbol]
        ma_long = self.ma_long[symbol]
        
        # 判断交易信号
        signal = TradingSignal.HOLD
        
        # 金叉：短期均线上穿长期均线，买入信号
        if ma_short > ma_long:
            # 检查是否刚刚发生金叉
            if symbol not in self.last_signal or self.last_signal[symbol] != 'bullish':
                # 确保没有持仓才买入
                if symbol not in self.positions or self.positions[symbol].quantity == 0:
                    signal = TradingSignal.BUY
                    
                    if self.logger:
                        self.logger.info(f"[交易信号] {bar_data['trade_date']} {symbol} 金叉买入信号，"
                                       f"短期均线={ma_short:.2f}，长期均线={ma_long:.2f}，价格={current_price:.2f}")
            
            self.last_signal[symbol] = 'bullish'
        
        # 死叉：短期均线下穿长期均线，卖出信号  
        elif ma_short < ma_long:
            # 检查是否刚刚发生死叉
            if symbol not in self.last_signal or self.last_signal[symbol] != 'bearish':
                # 确保有持仓才卖出
                if symbol in self.positions and self.positions[symbol].quantity > 0:
                    signal = TradingSignal.SELL
                    
                    if self.logger:
                        self.logger.info(f"[交易信号] {bar_data['trade_date']} {symbol} 死叉卖出信号，"
                                       f"短期均线={ma_short:.2f}，长期均线={ma_long:.2f}，价格={current_price:.2f}")
            
            self.last_signal[symbol] = 'bearish'
        
        return signal
    
    def get_position_size(self, symbol: str, price: float) -> int:
        """
        计算买入数量
        对于移动平均策略，使用等权重分配
        """
        # 假设最多同时持有5只股票，每只股票分配20%资金
        max_stocks = getattr(self.config, 'max_stocks', 5)
        position_pct = getattr(self.config, 'position_per_stock', 1.0 / max_stocks)
        
        max_value = self.cash * position_pct
        quantity = int(max_value / price / 100) * 100  # 按100股整数倍买入
        
        if self.logger:
            self.logger.info(f"[仓位计算] {symbol} 分配资金比例={position_pct:.1%}，"
                           f"可用金额={max_value:.0f}，计算数量={quantity}")
        
        return quantity
