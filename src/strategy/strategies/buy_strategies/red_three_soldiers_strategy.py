import pandas as pd
from typing import Dict, List
from collections import defaultdict
from datetime import datetime, timedelta

from src.strategy.models.base_strategy import BaseStrategy, StrategyConfig
from src.strategy.models.strategy_types import StrategyCapability


class RedThreeSoldiersConfig(StrategyConfig):
    """红三兵策略配置"""
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class RedThreeSoldiersStrategy(BaseStrategy):
    """
    红三兵买入策略（纯买入策略）
    
    策略规则：
    买入条件：
       - 连续三天均为阳线（收盘价 > 开盘价）
       - 开盘价呈阶梯式包容（后一天开盘价在前一天实体内部）  
       - 收盘价呈阶梯式上涨（后一天收盘价 > 前一天收盘价）
       - 前三天每日成交量都超过前5天最高成交量的50%
       - 每日实体比例≥50%
       - 每日涨幅≥1%（新增条件）
       - 必须是沪深主板股票
       - 在第三天收盘价买入
    
    注意：此策略仅负责买入信号生成，卖出策略需要单独配置
    """
    
    def __init__(self, config: RedThreeSoldiersConfig, logger=None):
        super().__init__(config, logger)
        self.price_history: Dict[str, List[pd.Series]] = defaultdict(list)  # 存储价格历史
        
        if self.logger:
            self.logger.info("[策略初始化] 红三兵买入策略初始化")
    
    def initialize(self) -> None:
        """策略初始化"""
        # 设置为纯买入策略
        self.capability = StrategyCapability.buy_only()
        
        if self.logger:
            self.logger.info(f"[策略初始化] 红三兵买入策略初始化完成，策略类型={self.capability.get_strategy_type()}")
    
    def _update_price_history(self, symbol: str, bar_data: pd.Series) -> None:
        """
        更新价格历史数据
        
        Args:
            symbol: 股票代码
            bar_data: K线数据
        """
        # 存储历史数据，保留最近8天（前5天基准 + 红三兵3天）
        self.price_history[symbol].append(bar_data.copy())
        if len(self.price_history[symbol]) > 8:
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
    
    def check_volume_condition(self, baseline_bars: List[pd.Series], recent_bars: List[pd.Series], current_date: str = None) -> bool:
        """
        检查成交量条件：前三天每天的成交量都必须比前5天中最高成交量大50%
        
        Args:
            baseline_bars: 前5天的K线数据（基准期）
            recent_bars: 红三兵3天的K线数据
            current_date: 当前交易日期
            
        Returns:
            是否满足成交量条件
        """
        if len(baseline_bars) != 5 or len(recent_bars) != 3:
            return False
        
        # 计算前5天的最高成交量
        baseline_volumes = []
        for bar in baseline_bars:
            vol = bar.get('vol', 0) or 0
            if vol > 0:
                baseline_volumes.append(vol)
        
        if not baseline_volumes:
            if self.logger:
                self.logger.warning(f"[成交量检查] 前5天成交量数据无效，跳过成交量检查")
            return False
        
        max_baseline_volume = max(baseline_volumes)
        volume_threshold = max_baseline_volume * 2 # 要求大100%
        
        # 检查红三兵3天的成交量是否都超过基准最高值的1.5倍
        day1_vol = recent_bars[0].get('vol', 0) or 0
        day2_vol = recent_bars[1].get('vol', 0) or 0
        day3_vol = recent_bars[2].get('vol', 0) or 0
        
        is_volume_valid = (day1_vol > volume_threshold and 
                          day2_vol > volume_threshold and 
                          day3_vol > volume_threshold)
        
        return is_volume_valid
    
    def _log_volume_analysis(self, baseline_bars: List[pd.Series], recent_bars: List[pd.Series], current_date: str = None) -> None:
        """
        输出成交量分析详情（仅在股票被选中时调用）
        
        Args:
            baseline_bars: 前5天的K线数据（基准期）
            recent_bars: 红三兵3天的K线数据
            current_date: 当前交易日期
        """
        if not self.logger:
            return
        
        # 计算前5天的最高成交量
        baseline_volumes = []
        for bar in baseline_bars:
            vol = bar.get('vol', 0) or 0
            if vol > 0:
                baseline_volumes.append(vol)
        
        if not baseline_volumes:
            return
        
        max_baseline_volume = max(baseline_volumes)
        volume_threshold = max_baseline_volume * 2
        
        # 红三兵三天成交量
        day1_vol = recent_bars[0].get('vol', 0) or 0
        day2_vol = recent_bars[1].get('vol', 0) or 0
        day3_vol = recent_bars[2].get('vol', 0) or 0
        
        date_info = f"，交易日期={current_date}" if current_date else ""
        self.logger.info(f"[成交量分析] 满足成交量条件（大100%）！{date_info}")
        self.logger.info(f"[成交量分析] 前5天最高成交量: {max_baseline_volume:.0f}")
        self.logger.info(f"[成交量分析] 成交量阈值(2倍): {volume_threshold:.0f}")
        self.logger.info(f"[成交量分析] 红三兵三日成交量: {day1_vol:.0f}, {day2_vol:.0f}, {day3_vol:.0f}")
        self.logger.info(f"[成交量分析] 实际倍数: {day1_vol/max_baseline_volume:.2f}x, {day2_vol/max_baseline_volume:.2f}x, {day3_vol/max_baseline_volume:.2f}x")
    
    def check_red_three_soldiers_pattern(self, bars: List[pd.Series], current_date: str = None) -> bool:
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
        
        # 4. 新增条件：每日实体比例达到50%（实体长度/影线总长度 >= 0.5）
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
        
        # 6. 新增条件：每日涨幅都要超过1%
        def get_daily_return(bar):
            """计算当日涨跌幅"""
            open_price = bar.get('open', 0) or 0
            close_price = bar.get('close', 0) or 0
            if open_price <= 0:
                return 0
            return (close_price - open_price) / open_price
        
        day1_return = get_daily_return(day1)
        day2_return = get_daily_return(day2)
        day3_return = get_daily_return(day3)
        
        min_daily_return = 0.01  # 1%涨幅要求
        is_daily_return_valid = (day1_return >= min_daily_return and 
                               day2_return >= min_daily_return and 
                               day3_return >= min_daily_return)
        
        # 基础条件：所有5个条件必须同时满足（成交量条件已在外部检查）
        base_condition = (is_day1_up and is_day2_up and is_day3_up and 
                         is_open_include and is_close_rise and 
                         is_body_ratio_valid and is_daily_return_valid)
        
        if self.logger and base_condition:
            date_info = f"，交易日期={current_date}" if current_date else ""
            self.logger.info(f"[形态分析] 发现增强版红三兵形态！{date_info}")
            self.logger.info(f"[形态分析] 三日收盘价: {day1_close:.2f} -> {day2_close:.2f} -> {day3_close:.2f}{date_info}")
            self.logger.info(f"[形态分析] 三日实体比例: {day1_body_ratio:.1%}, {day2_body_ratio:.1%}, {day3_body_ratio:.1%}{date_info}")
            self.logger.info(f"[形态分析] 三日涨跌幅: {day1_return:.2%}, {day2_return:.2%}, {day3_return:.2%}{date_info}")
        
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
        
        # 检查红三兵形态和成交量条件
        if len(self.price_history[symbol]) >= 8:
            # 取最近八天数据：前5天 + 红三兵3天
            all_history = self.price_history[symbol]
            baseline_bars = all_history[-8:-3]  # 前5天（基准期）
            recent_bars = all_history[-3:]      # 红三兵3天
            
            # 检查是否已持有该股票
            if symbol not in self.positions:
                current_date = bar_data['trade_date']
                
                # 先检查成交量条件
                if self.check_volume_condition(baseline_bars, recent_bars, current_date):
                    # 再检查红三兵形态
                    if self.check_red_three_soldiers_pattern(recent_bars, current_date):
                        
                        if self.logger:
                            # 股票被选中，输出完整选择逻辑
                            self.logger.info(f"[✅ 选中买入] {symbol} 通过所有筛选条件，交易日期={current_date}，买入价格={bar_data['close']:.2f}")
                            
                            # 输出成交量分析详情
                            self._log_volume_analysis(baseline_bars, recent_bars, current_date)
                            
                            # 输出红三兵形态详情（已在check_red_three_soldiers_pattern中输出）
                        
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
            "strategy_description": "红三兵买入策略（增强版）",
            "description": "基于连续三日阳线形态的纯买入策略",
            "strategy_type": "buy_only",
            "buy_conditions": [
                "连续三天阳线",
                "开盘价阶梯式包容",
                "收盘价阶梯式上涨", 
                "成交量超过前5天最高值100%",
                "实体比例≥50%",
                "每日涨幅≥1%"
            ],
            "target_market": "沪深主板",
            "notes": "此策略仅负责买入信号生成，卖出策略需要单独配置"
        })
        return info
