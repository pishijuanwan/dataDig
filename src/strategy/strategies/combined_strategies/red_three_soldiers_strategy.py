import pandas as pd
from typing import Dict, List
from collections import defaultdict
from datetime import datetime, timedelta

from src.strategy.models.base_strategy import BaseStrategy, StrategyConfig
from src.strategy.models.strategy_types import StrategyCapability
from src.strategy.strategies.sell_strategies.drop_stop_loss_strategy import DropStopLossStrategy


class RedThreeSoldiersConfig(StrategyConfig):
    """红三兵策略配置"""
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class RedThreeSoldiersStrategy(BaseStrategy):
    """
    红三兵策略（组合策略）
    
    策略规则：
    1. 买入条件：
       - 连续三天均为阳线（收盘价 > 开盘价）
       - 开盘价呈阶梯式包容（后一天开盘价在前一天实体内部）  
       - 收盘价呈阶梯式上涨（后一天收盘价 > 前一天收盘价）
       - 必须是沪深主板股票
       - 在第三天收盘价买入
    
    2. 卖出条件：
       - 相对买入价跌幅超过3%：强制止损卖出（优先级最高）
       - 当日上涨：继续持有不卖出
       - 当日下跌超过2%：执行止损卖出
    """
    
    def __init__(self, config: RedThreeSoldiersConfig, logger=None):
        super().__init__(config, logger)
        self.price_history: Dict[str, List[pd.Series]] = defaultdict(list)  # 存储价格历史
        self.buy_dates: Dict[str, str] = {}  # 记录买入日期，用于控制卖出时机
        self.buy_prices: Dict[str, float] = {}  # 记录买入价格，用于卖出策略计算收益
        
        # 初始化卖出策略
        self.sell_strategy = DropStopLossStrategy(logger)
        
        if self.logger:
            self.logger.info("[策略初始化] 红三兵组合策略初始化")
            self.logger.info("[策略初始化] 已集成下跌止损卖出策略")
    
    def initialize(self) -> None:
        """策略初始化"""
        # 设置为组合策略，既可买入也可卖出
        self.capability = StrategyCapability.combined()
        
        if self.logger:
            self.logger.info(f"[策略初始化] 红三兵组合策略初始化完成，策略类型={self.capability.get_strategy_type()}")
    
    def _update_price_history(self, symbol: str, bar_data: pd.Series) -> None:
        """
        更新价格历史数据
        
        Args:
            symbol: 股票代码
            bar_data: K线数据
        """
        # 存储历史数据，只保留最近4天（3天形态检查 + 1天卖出判断）
        self.price_history[symbol].append(bar_data.copy())
        if len(self.price_history[symbol]) > 4:
            self.price_history[symbol].pop(0)
    
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
        检查红三兵形态（增强版）
        
        Args:
            bars: 连续三天的K线数据（按时间顺序）
            
        Returns:
            是否满足红三兵条件
        """
        if len(bars) != 3:
            return False
            
        day1, day2, day3 = bars
        
        # 1. 连续三天均为阳线（收盘价 > 开盘价）
        # 处理价格为None的情况
        is_day1_up = (day1.get('close', 0) or 0) > (day1.get('open', 0) or 0)
        is_day2_up = (day2.get('close', 0) or 0) > (day2.get('open', 0) or 0) 
        is_day3_up = (day3.get('close', 0) or 0) > (day3.get('open', 0) or 0)
        
        # 2. 开盘价呈阶梯式包容（后一天开盘价在前一天实体内部）
        day1_open = day1.get('open', 0) or 0
        day1_close = day1.get('close', 0) or 0
        day2_open = day2.get('open', 0) or 0
        day2_close = day2.get('close', 0) or 0
        day3_open = day3.get('open', 0) or 0
        day3_close = day3.get('close', 0) or 0
        
        is_open_include = (day1_open < day2_open <= day1_close) and \
                          (day2_open < day3_open <= day2_close)
        
        # 3. 收盘价呈阶梯式上涨（后一天收盘价 > 前一天收盘价）
        is_close_rise = (day2_close > day1_close) and (day3_close > day2_close)
        
        # 4. 新增条件：成交量持续递增
        # 处理成交量可能为None或0的情况
        vol1 = day1.get('vol', 0) or 0
        vol2 = day2.get('vol', 0) or 0  
        vol3 = day3.get('vol', 0) or 0
        
        is_volume_increase = (0 < vol1 < vol2 < vol3)
        is_volume_increase = True
        
        # 5. 新增条件：每日实体比例达到50%（实体长度/影线总长度 >= 0.5）
        def get_body_ratio(bar):
            """计算实体占总振幅的比例"""
            # 处理价格字段为None的情况
            open_price = bar.get('open', 0) or 0
            close_price = bar.get('close', 0) or 0
            high_price = bar.get('high', 0) or 0
            low_price = bar.get('low', 0) or 0
            
            if any(price <= 0 for price in [open_price, close_price, high_price, low_price]):
                return 0
                
            body_length = abs(close_price - open_price)  # 实体长度
            total_range = high_price - low_price         # 总振幅
            if total_range == 0:
                return 0
            return body_length / total_range
        
        day1_body_ratio = get_body_ratio(day1)
        day2_body_ratio = get_body_ratio(day2)
        day3_body_ratio = get_body_ratio(day3)
        
        min_body_ratio = 0.0  # 20%要求
        is_body_ratio_valid = (day1_body_ratio >= min_body_ratio and 
                              day2_body_ratio >= min_body_ratio and 
                              day3_body_ratio >= min_body_ratio)
        
        # 基础条件：所有5个条件必须同时满足
        base_condition = (is_day1_up and is_day2_up and is_day3_up and 
                         is_open_include and is_close_rise and 
                         is_volume_increase and is_body_ratio_valid)
        
        if self.logger and base_condition:
            self.logger.info(f"[红三兵形态] 发现增强版红三兵形态！")
            self.logger.info(f"[红三兵形态] 三日收盘价: {day1_close:.2f} -> {day2_close:.2f} -> {day3_close:.2f}")
            self.logger.info(f"[红三兵形态] 三日成交量: {vol1:.0f} -> {vol2:.0f} -> {vol3:.0f}")
            self.logger.info(f"[红三兵形态] 三日实体比例: {day1_body_ratio:.1%}, {day2_body_ratio:.1%}, {day3_body_ratio:.1%}")
        
        return base_condition
    
    def generate_buy_signal(self, symbol: str, bar_data: pd.Series) -> bool:
        """
        生成买入信号：检查红三兵形态
        
        Args:
            symbol: 股票代码
            bar_data: K线数据
            
        Returns:
            是否应该买入
        """
        # 检查是否为主板股票
        if not self.is_main_board_stock(symbol):
            return False
        
        # 更新价格历史
        self._update_price_history(symbol, bar_data)
        
        # 检查红三兵形态
        if len(self.price_history[symbol]) >= 3:
            # 取最近三天数据
            recent_bars = self.price_history[symbol][-3:]
            
            # 检查是否已持有该股票
            if symbol not in self.positions:
                # 检查红三兵形态
                if self.check_red_three_soldiers_pattern(recent_bars):
                    current_date = bar_data['trade_date']
                    
                    if self.logger:
                        self.logger.info(f"[买入信号] {symbol} 满足红三兵形态，日期={current_date}，价格={bar_data['close']:.2f}")
                    
                    # 记录买入日期和买入价格
                    self.buy_dates[symbol] = current_date
                    self.buy_prices[symbol] = bar_data['close']
                    return True
        
        return False
    
    def generate_sell_signal(self, symbol: str, bar_data: pd.Series) -> bool:
        """
        生成卖出信号：使用下跌止损策略
        
        Args:
            symbol: 股票代码
            bar_data: K线数据
            
        Returns:
            是否应该卖出
        """
        # 更新价格历史（确保数据同步）
        self._update_price_history(symbol, bar_data)
        
        current_date = bar_data['trade_date']
        
        # 卖出逻辑：使用下跌止损策略
        if symbol in self.positions and symbol in self.buy_dates:
            buy_date = self.buy_dates[symbol]
            buy_price = self.buy_prices.get(symbol, None)
            
            # 不在买入当天卖出，从第二天开始检查卖出条件
            if current_date != buy_date:
                if self.logger:
                    self.logger.info(f"[卖出检查] {symbol} 买入日期={buy_date}，当前日期={current_date}，开始检查止损条件")
                
                # 使用下跌止损策略判断是否卖出
                should_sell = self.sell_strategy.should_sell(symbol, bar_data, buy_price)
                
                if should_sell:
                    if self.logger:
                        self.logger.info(f"[卖出信号] {symbol} 触发下跌止损策略，执行卖出")
                    
                    # 从买入记录中移除
                    del self.buy_dates[symbol]
                    if symbol in self.buy_prices:
                        del self.buy_prices[symbol]
                    return True
        
        return False
    
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
            "strategy_description": "红三兵策略（增强版）",
            "description": "基于连续三日阳线形态的短期交易策略",
            "enhanced_conditions": [
                "连续三天阳线",
                "开盘价阶梯式包容",
                "收盘价阶梯式上涨", 
                "成交量持续递增",
                "实体比例≥50%"
            ],
            "sell_conditions": [
                "总体跌幅>3%：强制止损（优先级最高）",
                "当日上涨：继续持有",
                "当日下跌>2%：触发止损"
            ],
            "holding_period": "灵活（下跌2%止损）",
            "target_market": "沪深主板"
        })
        return info
