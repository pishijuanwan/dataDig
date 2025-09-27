"""
下跌止损卖出策略

该策略根据股票当天表现和相对买入价表现决定是否卖出：
- 如果相对买入价跌幅超过3%，强制卖出（优先级最高）
- 如果当天上涨，则继续持有不卖出
- 如果当天下跌超过2%，则执行止损卖出
"""

import pandas as pd
from typing import Dict
from src.strategy.models.base_strategy import BaseStrategy
from src.strategy.models.strategy_types import StrategyCapability


class DropStopLossStrategy:
    """
    下跌止损卖出策略
    
    策略逻辑：
    1. 优先条件：如果相对买入价跌幅超过3%，强制卖出
    2. 如果当天股价上涨（收盘价 >= 开盘价），则不卖出
    3. 如果当天股价下跌超过2%（(开盘价-收盘价)/开盘价 > 0.02），则卖出
    """
    
    def __init__(self, logger=None):
        self.logger = logger
        self.daily_stop_loss_threshold = 0.02  # 当日下跌2%止损线
        self.total_loss_threshold = 0.03       # 相对买入价3%强制止损线
        
        if self.logger:
            self.logger.info("[下跌止损策略] 下跌止损卖出策略初始化完成")
            self.logger.info(f"[下跌止损策略] 当日止损阈值: {self.daily_stop_loss_threshold:.1%}")
            self.logger.info(f"[下跌止损策略] 总体止损阈值: {self.total_loss_threshold:.1%}")
            self.logger.info("[下跌止损策略] 总体止损优先级高于当日止损")
    
    def should_sell(self, symbol: str, bar_data: pd.Series, buy_price: float = None) -> bool:
        """
        判断是否应该卖出
        
        Args:
            symbol: 股票代码
            bar_data: 当天K线数据
            buy_price: 买入价格（可选，用于计算总收益）
            
        Returns:
            是否应该卖出
        """
        open_price = bar_data.get('open', 0) or 0
        close_price = bar_data.get('close', 0) or 0
        current_date = bar_data.get('trade_date', 'Unknown')
        
        # 检查价格数据有效性
        if open_price <= 0 or close_price <= 0:
            if self.logger:
                self.logger.warning(f"[下跌止损策略] {symbol} 价格数据无效，开盘价={open_price}, 收盘价={close_price}")
            return False
        
        # 计算当日涨跌幅和总收益
        daily_return = (close_price - open_price) / open_price
        total_return = None
        if buy_price and buy_price > 0:
            total_return = (close_price - buy_price) / buy_price
        
        if self.logger:
            self.logger.info(f"[下跌止损策略] {symbol} 当日表现分析，日期={current_date}")
            self.logger.info(f"[下跌止损策略] {symbol} 开盘价={open_price:.2f}, 收盘价={close_price:.2f}")
            self.logger.info(f"[下跌止损策略] {symbol} 当日涨跌幅={daily_return:.2%}")
            if buy_price:
                self.logger.info(f"[下跌止损策略] {symbol} 买入价={buy_price:.2f}, 总收益={total_return:.2%}")
        
        # 优先条件：检查相对买入价的总体跌幅
        if buy_price and buy_price > 0 and total_return is not None:
            if total_return <= -self.total_loss_threshold:
                # 相对买入价跌幅超过3%，强制卖出
                if self.logger:
                    self.logger.info(f"[下跌止损策略] {symbol} 相对买入价跌幅{abs(total_return):.2%}，超过总体止损线{self.total_loss_threshold:.1%}")
                    self.logger.info(f"[下跌止损策略] {symbol} 触发强制止损卖出信号（优先级最高）")
                return True
        
        # 次要条件：检查当日表现
        if daily_return >= 0:
            # 当天上涨或持平，继续持有
            if self.logger:
                self.logger.info(f"[下跌止损策略] {symbol} 当日上涨{daily_return:.2%}，继续持有")
            return False
        
        elif abs(daily_return) > self.daily_stop_loss_threshold:
            # 当天下跌超过当日止损线，执行卖出
            if self.logger:
                self.logger.info(f"[下跌止损策略] {symbol} 当日下跌{abs(daily_return):.2%}，超过当日止损线{self.daily_stop_loss_threshold:.1%}")
                self.logger.info(f"[下跌止损策略] {symbol} 触发当日止损卖出信号")
            return True
        
        else:
            # 当天下跌但未超过止损线，继续持有
            if self.logger:
                self.logger.info(f"[下跌止损策略] {symbol} 当日下跌{abs(daily_return):.2%}，未达当日止损线{self.daily_stop_loss_threshold:.1%}，继续持有")
            return False
    
    def get_strategy_info(self) -> Dict:
        """获取策略信息"""
        return {
            "strategy_name": "下跌止损卖出策略",
            "strategy_type": "sell_only",
            "description": "基于当日涨跌幅和总体跌幅的双重止损卖出策略",
            "conditions": [
                f"总体跌幅>{self.total_loss_threshold:.1%}：强制卖出（优先级最高）",
                "当日上涨：继续持有",
                f"当日下跌>{self.daily_stop_loss_threshold:.1%}：执行止损"
            ],
            "daily_stop_loss_threshold": f"{self.daily_stop_loss_threshold:.1%}",
            "total_loss_threshold": f"{self.total_loss_threshold:.1%}",
            "priority": "总体跌幅止损 > 当日止损"
        }
