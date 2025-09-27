import pandas as pd
from typing import Dict, List
from collections import defaultdict
from datetime import datetime, timedelta

from src.strategy.models.base_strategy import BaseStrategy, StrategyConfig, TradingSignal


class RedThreeSoldiersConfig(StrategyConfig):
    """红三兵策略配置"""
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class RedThreeSoldiersStrategy(BaseStrategy):
    """
    红三兵策略
    
    策略规则：
    1. 买入条件：
       - 连续三天均为阳线（收盘价 > 开盘价）
       - 开盘价呈阶梯式包容（后一天开盘价在前一天实体内部）  
       - 收盘价呈阶梯式上涨（后一天收盘价 > 前一天收盘价）
       - 必须是沪深主板股票
       - 在第三天收盘价买入
    
    2. 卖出条件：
       - 第四天收盘直接卖出，无论盈亏
    """
    
    def __init__(self, config: RedThreeSoldiersConfig, logger=None):
        super().__init__(config, logger)
        self.price_history: Dict[str, List[pd.Series]] = defaultdict(list)  # 存储价格历史
        self.buy_dates: Dict[str, str] = {}  # 记录买入日期，用于控制卖出时机
        
        if self.logger:
            self.logger.info("[策略初始化] 红三兵策略初始化完成")
    
    def initialize(self) -> None:
        """策略初始化"""
        if self.logger:
            self.logger.info("[策略初始化] 红三兵策略初始化完成")
    
    def is_main_board_stock(self, symbol: str) -> bool:
        """
        判断是否为沪深主板股票
        
        Args:
            symbol: 股票代码，如 '000001.SZ'
            
        Returns:
            是否为主板股票
        """
        if not symbol or len(symbol) < 9:
            return False
            
        code, exchange = symbol.split('.')
        
        # 深圳主板：000xxx
        if exchange == 'SZ' and code.startswith('000'):
            return True
            
        # 上海主板：600xxx, 601xxx, 603xxx, 605xxx
        if exchange == 'SH' and (code.startswith('600') or 
                                code.startswith('601') or 
                                code.startswith('603') or 
                                code.startswith('605')):
            return True
            
        return False
    
    def check_red_three_soldiers_pattern(self, bars: List[pd.Series]) -> bool:
        """
        检查红三兵形态
        
        Args:
            bars: 连续三天的K线数据（按时间顺序）
            
        Returns:
            是否满足红三兵条件
        """
        if len(bars) != 3:
            return False
            
        day1, day2, day3 = bars
        
        # 1. 连续三天均为阳线（收盘价 > 开盘价）
        is_day1_up = day1['close'] > day1['open']
        is_day2_up = day2['close'] > day2['open'] 
        is_day3_up = day3['close'] > day3['open']
        
        if self.logger:
            self.logger.debug(f"[红三兵检查] 阳线检查: Day1={is_day1_up}, Day2={is_day2_up}, Day3={is_day3_up}")
        
        # 2. 开盘价呈阶梯式包容（后一天开盘价在前一天实体内部）
        is_open_include = (day2['open'] > day1['open'] and day2['open'] <= day1['close']) and \
                         (day3['open'] > day2['open'] and day3['open'] <= day2['close'])
        
        if self.logger:
            self.logger.debug(f"[红三兵检查] 开盘价包容: {is_open_include}")
            self.logger.debug(f"[红三兵检查] Day1: open={day1['open']:.2f}, close={day1['close']:.2f}")
            self.logger.debug(f"[红三兵检查] Day2: open={day2['open']:.2f}, close={day2['close']:.2f}")
            self.logger.debug(f"[红三兵检查] Day3: open={day3['open']:.2f}, close={day3['close']:.2f}")
        
        # 3. 收盘价呈阶梯式上涨（后一天收盘价 > 前一天收盘价）
        is_close_rise = (day2['close'] > day1['close']) and (day3['close'] > day2['close'])
        
        if self.logger:
            self.logger.debug(f"[红三兵检查] 收盘价上涨: {is_close_rise}")
        
        # 基础条件：以上3个条件必须同时满足
        base_condition = is_day1_up and is_day2_up and is_day3_up and is_open_include and is_close_rise
        
        if self.logger and base_condition:
            self.logger.info(f"[红三兵形态] 发现红三兵形态！三日收盘价: {day1['close']:.2f} -> {day2['close']:.2f} -> {day3['close']:.2f}")
        
        return base_condition
    
    def on_bar(self, symbol: str, bar_data: pd.Series) -> str:
        """
        处理每个K线数据
        
        Args:
            symbol: 股票代码
            bar_data: K线数据
            
        Returns:
            交易信号
        """
        # 检查是否为主板股票
        if not self.is_main_board_stock(symbol):
            return TradingSignal.HOLD
        
        # 存储历史数据，只保留最近4天（3天形态检查 + 1天卖出判断）
        self.price_history[symbol].append(bar_data.copy())
        if len(self.price_history[symbol]) > 4:
            self.price_history[symbol].pop(0)
        
        current_date = bar_data['trade_date']
        
        # 卖出逻辑：检查是否需要卖出
        if symbol in self.positions and symbol in self.buy_dates:
            buy_date = self.buy_dates[symbol]
            
            # 计算买入日期的下一个交易日（第四天）
            # 这里简单处理，实际应该基于交易日历
            if current_date != buy_date:  # 不是买入当天，就卖出
                if self.logger:
                    self.logger.info(f"[卖出信号] {symbol} 买入日期={buy_date}，当前日期={current_date}，执行卖出")
                
                # 从买入记录中移除
                del self.buy_dates[symbol]
                return TradingSignal.SELL
        
        # 买入逻辑：检查红三兵形态
        if len(self.price_history[symbol]) >= 3:
            # 取最近三天数据
            recent_bars = self.price_history[symbol][-3:]
            
            # 检查是否已持有该股票
            if symbol not in self.positions:
                # 检查红三兵形态
                if self.check_red_three_soldiers_pattern(recent_bars):
                    if self.logger:
                        self.logger.info(f"[买入信号] {symbol} 满足红三兵形态，日期={current_date}，价格={bar_data['close']:.2f}")
                    
                    # 记录买入日期
                    self.buy_dates[symbol] = current_date
                    return TradingSignal.BUY
        
        return TradingSignal.HOLD
    
    def get_position_size(self, symbol: str, price: float) -> int:
        """
        计算买入数量
        红三兵策略采用等权重分配，假设最多同时持有10只股票
        """
        max_stocks = getattr(self.config, 'max_stocks', 10)
        position_pct = getattr(self.config, 'position_per_stock', 1.0 / max_stocks)
        
        max_value = self.cash * position_pct
        quantity = int(max_value / price / 100) * 100  # 按100股整数倍买入
        
        if self.logger:
            self.logger.info(f"[仓位计算] {symbol} 分配资金比例={position_pct:.1%}，"
                           f"可用金额={max_value:.0f}，计算数量={quantity}")
        
        return quantity
    
    def get_strategy_info(self) -> Dict:
        """获取策略信息"""
        info = super().get_strategy_info()
        info.update({
            "strategy_type": "红三兵策略",
            "description": "基于连续三日阳线形态的短期交易策略",
            "holding_period": "1天（第四天卖出）",
            "target_market": "沪深主板"
        })
        return info
