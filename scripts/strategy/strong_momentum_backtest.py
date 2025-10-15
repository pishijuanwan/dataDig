#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
强势非涨停策略回测系统

策略逻辑：
1. 选股条件：
   - 个股过去5个交易日涨幅超过20%
   - 过去5个交易日没有一天涨幅超过9.5%
   - 限制沪深主板股票（排除创业板300和科创板688）
   - 排除ST股票（包括ST、*ST、SST等特别处理股票）
   - 排除已持仓股票（避免重复买入）
   - 排除无交易股票（开盘价=收盘价且最高价=最低价的一字板）
   - 排除涨幅过大股票（当日收盘价不超过20个交易日前收盘价的160%）
   - 单日筛选股票数量不超过10只（超过则丢弃该日所有数据）

2. 交易规则：
   - 初始资金：10万
   - 每次交易手续费：0.1%（买卖各0.1%）
   - 最多同时持有5只股票，资金按公式分配：当前现金/(5-持仓股票数量)
   - 当天股票多于仓位容量时随机选择
   - 卖出条件：跌幅超过5%止损，持有5天收益为负卖出，或持有到第11天卖出

用法示例:
python strong_momentum_backtest.py
"""

import sys
import os
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple
import pandas as pd
import numpy as np

# 添加项目根路径到Python路径
sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))

from src.config.settings import load_settings
from src.db.mysql_client import MySQLClient
from src.app_logging.logger import get_logger


class Position:
    """持仓信息类"""
    def __init__(self, ts_code: str, name: str, buy_date: str, buy_price: float, 
                 shares: int, buy_amount: float):
        self.ts_code = ts_code
        self.name = name
        self.buy_date = buy_date
        self.buy_price = buy_price
        self.shares = shares
        self.buy_amount = buy_amount
        self.hold_days = 0
        
    def update_hold_days(self):
        """更新持有天数"""
        self.hold_days += 1
        
    def get_current_value(self, current_price: float) -> float:
        """获取当前市值"""
        return self.shares * current_price
        
    def get_return_pct(self, current_price: float) -> float:
        """获取收益率"""
        return (current_price - self.buy_price) / self.buy_price * 100


class StrongMomentumBacktester:
    """强势非涨停策略回测器"""
    
    def __init__(self, session, logger, initial_capital: float = 100000):
        self.session = session
        self.logger = logger
        self.initial_capital = initial_capital
        self.current_cash = initial_capital
        self.positions = {}  # ts_code -> Position
        self.transaction_cost = 0.001  # 0.1% 手续费
        self.max_position_ratio = 0.20  # 20% 最大仓位
        self.stop_loss_pct = -5.0   # -5% 止损
        self.max_hold_days = 10  # 最多持有10天（第11天卖出）
        
        # 交易记录
        self.trades = []
        self.daily_values = []
        
        if self.logger:
            self.logger.info(f"[策略回测初始化] 初始资金: {initial_capital:,.2f}元, 最大单股仓位: {self.max_position_ratio*100}%")
    
    def is_main_board_stock(self, ts_code: str) -> bool:
        """判断是否为沪深主板股票（排除创业板和科创板）"""
        if ts_code.endswith('.SH'):
            # 上海：600、601、603为主板，688为科创板（排除）
            return ts_code.startswith('600') or ts_code.startswith('601') or ts_code.startswith('603')
        elif ts_code.endswith('.SZ'):
            # 深圳：000为主板，002为中小板（算主板），300为创业板（排除）
            return ts_code.startswith('000') or ts_code.startswith('002')
        else:
            return False
    
    def get_trading_dates_2024_to_now(self) -> List[str]:
        """获取2024年至今的所有交易日期（从数据库）"""
        from src.models.daily_price import DailyPrice
        from sqlalchemy import select
        
        if self.logger:
            self.logger.info("[获取交易日期] 开始从数据库获取2024年至今的交易日期")
        
        stmt = select(DailyPrice.trade_date.distinct()).where(
            DailyPrice.trade_date >= '20240101'
        ).order_by(DailyPrice.trade_date)
        
        result = self.session.execute(stmt).scalars().all()
        
        if self.logger:
            self.logger.info(f"[获取交易日期] 共找到{len(result)}个交易日")
        
        return list(result)
    
    def get_stock_5day_performance(self, ts_code: str, end_date: str) -> Dict[str, Any]:
        """获取股票过去5日的详细表现（从数据库）"""
        from src.models.daily_price import DailyPrice
        from sqlalchemy import select, and_
        
        # 获取包含end_date在内的最近6天数据（需要计算5日涨幅，需要第6天作为基准）
        stmt = select(
            DailyPrice.trade_date,
            DailyPrice.close,
            DailyPrice.pct_chg,
            DailyPrice.vol
        ).where(
            and_(
                DailyPrice.ts_code == ts_code,
                DailyPrice.trade_date <= end_date
            )
        ).order_by(DailyPrice.trade_date.desc()).limit(6)
        
        result = self.session.execute(stmt).fetchall()
        
        if len(result) < 6:  # 需要至少6天数据
            return None
        
        # 按日期正序排列（result是倒序的）
        data = list(reversed(result))
        
        # 计算5日涨幅：第6天（基准）到最后一天
        start_price = data[0][1]  # 第6天的收盘价（基准日）
        end_price = data[5][1]    # 最后一天的收盘价（end_date）
        
        if not start_price or start_price <= 0:
            return None
        
        total_return = (end_price - start_price) / start_price * 100
        
        # 检查最近5天的每日涨跌幅（排除基准日）
        daily_changes = []
        has_limit_up = False
        
        for i in range(1, 6):  # 最近5天
            pct_chg = data[i][2]
            if pct_chg is not None:
                daily_changes.append(pct_chg)
                # 判断是否有涨幅超过9.5%的交易日
                if pct_chg > 9.5:
                    has_limit_up = True
        
        return {
            'total_return_5d': total_return,
            'daily_changes': daily_changes,
            'has_limit_up': has_limit_up,
            'trading_dates': [row[0] for row in data[1:6]],  # 最近5天的交易日期
            'end_price': end_price,
            'avg_volume': sum(row[3] for row in data[1:6] if row[3]) / 5 if any(row[3] for row in data[1:6]) else 0
        }
    
    def get_daily_stock_list(self, trade_date: str) -> List[tuple]:
        """获取指定日期的股票列表（从数据库）"""
        from src.models.daily_price import DailyPrice, StockBasic
        from sqlalchemy import select, and_
        
        stmt = select(
            DailyPrice.ts_code,
            StockBasic.name,
            DailyPrice.open,
            DailyPrice.high,
            DailyPrice.low,
            DailyPrice.close,
            DailyPrice.vol
        ).select_from(
            DailyPrice.__table__.join(
                StockBasic.__table__, DailyPrice.ts_code == StockBasic.ts_code
            )
        ).where(
            and_(
                DailyPrice.trade_date == trade_date,
                StockBasic.list_status == 'L',  # 只选择正常上市的股票
                DailyPrice.close.isnot(None),   # 确保有收盘价
                DailyPrice.vol > 0               # 确保有成交量
            )
        )
        
        result = self.session.execute(stmt).fetchall()
        return result
    
    def get_stock_price(self, ts_code: str, trade_date: str) -> float:
        """获取指定股票在指定日期的收盘价"""
        from src.models.daily_price import DailyPrice
        from sqlalchemy import select, and_
        
        stmt = select(DailyPrice.close).where(
            and_(
                DailyPrice.ts_code == ts_code,
                DailyPrice.trade_date == trade_date
            )
        )
        
        result = self.session.execute(stmt).scalar()
        return result if result else 0.0
    
    def get_stock_price_20days_ago(self, ts_code: str, end_date: str) -> float:
        """获取股票20个交易日前的收盘价"""
        from src.models.daily_price import DailyPrice
        from sqlalchemy import select, and_
        
        # 获取包含end_date在内的最近21天数据（需要第21天作为20个交易日前）
        stmt = select(DailyPrice.close).where(
            and_(
                DailyPrice.ts_code == ts_code,
                DailyPrice.trade_date <= end_date
            )
        ).order_by(DailyPrice.trade_date.desc()).limit(21)
        
        result = self.session.execute(stmt).fetchall()
        
        if len(result) < 21:  # 需要至少21天数据
            return 0.0
        
        # 返回第21天（20个交易日前）的收盘价
        return result[20][0] if result[20][0] else 0.0
    
    def screen_stocks(self, trade_date: str, 
                     min_5day_return: float = 20.0,
                     max_daily_limit: float = 9.5) -> List[Dict]:
        """筛选符合条件的股票"""
        
        # 获取当日股票列表
        stock_list = self.get_daily_stock_list(trade_date)
        
        if not stock_list:
            return []
        
        # 筛选符合条件的股票
        qualified_stocks = []
        
        for ts_code, name, open_price, high, low, close, vol in stock_list:
            # 1. 首先检查是否为沪深主板股票
            if not self.is_main_board_stock(ts_code):
                continue
            
            # 2. 排除ST股票（包括ST、*ST、SST等）
            if name and ('ST' in name or 'st' in name):
                if self.logger:
                    self.logger.debug(f"[选股过滤] {trade_date}: 排除ST股票 {name}({ts_code})")
                continue
            
            # 3. 排除已持仓的股票
            if ts_code in self.positions:
                if self.logger:
                    self.logger.debug(f"[选股过滤] {trade_date}: 排除已持仓股票 {name}({ts_code})")
                continue
            
            # 4. 排除无交易股票（开盘价=收盘价 且 最高价=最低价）
            if open_price == close and high == low:
                if self.logger:
                    self.logger.debug(f"[选股过滤] {trade_date}: 排除无交易股票(一字板) {name}({ts_code})")
                continue
            
            # 5. 检查20个交易日涨幅限制（收盘价不能超过20天前收盘价的160%）
            price_20days_ago = self.get_stock_price_20days_ago(ts_code, trade_date)
            if price_20days_ago > 0 and close > price_20days_ago * 1.6:
                if self.logger:
                    self.logger.debug(f"[选股过滤] {trade_date}: 排除涨幅过大股票 {name}({ts_code}), "
                                    f"当前{close:.2f}元 vs 20天前{price_20days_ago:.2f}元")
                continue
            
            # 6. 获取过去5日表现
            performance_5d = self.get_stock_5day_performance(ts_code, trade_date)
            
            if not performance_5d:
                continue
            
            # 7. 检查是否符合涨幅和涨停条件
            if (performance_5d['total_return_5d'] >= min_5day_return and 
                not performance_5d['has_limit_up']):
                
                qualified_stocks.append({
                    'ts_code': ts_code,
                    'name': name,
                    'price': close,
                    'return_5d': performance_5d['total_return_5d'],
                    'has_limit_up': performance_5d['has_limit_up'],
                    'daily_changes': performance_5d['daily_changes'],
                    'avg_volume': performance_5d['avg_volume']
                })
        
        # 8. 检查当日股票数量是否超过10只
        if len(qualified_stocks) > 10:
            if self.logger:
                self.logger.info(f"[选股筛选] {trade_date}: 找到{len(qualified_stocks)}个机会，超过10只限制，丢弃该日所有股票")
            return []  # 丢弃这一天的所有数据
        elif len(qualified_stocks) > 0:
            if self.logger:
                self.logger.info(f"[选股筛选] {trade_date}: 找到{len(qualified_stocks)}个符合条件的强势非涨停股票")
        
        return qualified_stocks
    
    def calculate_position_sizes(self, available_cash: float, candidate_stocks: List[Dict]) -> List[Dict]:
        """计算仓位大小"""
        if not candidate_stocks:
            return []
        
        # 计算当前总资产（现金 + 持仓市值）
        total_assets = available_cash
        for position in self.positions.values():
            current_price = self.get_stock_price(position.ts_code, 
                                               max(self.daily_values[-1]['date'] if self.daily_values else '20240101'))
            if current_price > 0:
                total_assets += position.get_current_value(current_price)
        
        # 计算每个股票的最大可买金额
        max_position_value = total_assets * self.max_position_ratio
        
        # 计算当前持仓数量
        current_positions = len(self.positions)
        
        # 计算理论最大持仓数量（5个，因为每个20%）
        max_total_positions = 5
        
        # 计算可以新买入的股票数量
        available_position_slots = max_total_positions - current_positions
        
        if available_position_slots <= 0:
            if self.logger:
                self.logger.info(f"[仓位计算] 当前已满仓({current_positions}只)，无法买入新股票")
            return []
        
        # 如果候选股票数量超过可用仓位，随机选择
        if len(candidate_stocks) > available_position_slots:
            if self.logger:
                self.logger.info(f"[仓位计算] 候选股票{len(candidate_stocks)}只超过可用仓位{available_position_slots}只，随机选择")
            candidate_stocks = random.sample(candidate_stocks, available_position_slots)
        
        # 计算每个新股票应分配的资金：当前现金/(5-持仓股票数量)
        remaining_position_slots = max_total_positions - current_positions
        cash_per_new_stock = available_cash / remaining_position_slots
        
        if self.logger:
            self.logger.info(f"[仓位计算] 当前持仓{current_positions}只，剩余{remaining_position_slots}个仓位，"
                           f"每个新股票分配资金: {cash_per_new_stock:,.2f}元")
        
        # 直接使用分配的资金，不再受20%最大仓位限制约束
        # 因为资金分配公式本身就保证了仓位平衡
        max_cash_per_stock = cash_per_new_stock
        
        buy_orders = []
        for stock in candidate_stocks:
            # 考虑交易费用后的实际可用金额
            available_for_stock = max_cash_per_stock / (1 + self.transaction_cost)
            
            # 计算可买股数（100股为一手）
            shares = int(available_for_stock / stock['price'] / 100) * 100
            
            if shares >= 100:  # 至少一手
                buy_amount_before_cost = shares * stock['price']
                transaction_cost = buy_amount_before_cost * self.transaction_cost
                total_cost = buy_amount_before_cost + transaction_cost
                
                if total_cost <= available_cash:
                    buy_orders.append({
                        'ts_code': stock['ts_code'],
                        'name': stock['name'],
                        'price': stock['price'],
                        'shares': shares,
                        'amount': buy_amount_before_cost,
                        'cost': transaction_cost,
                        'total_cost': total_cost
                    })
        
        if self.logger and buy_orders:
            self.logger.info(f"[仓位计算] 计划买入{len(buy_orders)}只股票，总成本{sum(order['total_cost'] for order in buy_orders):,.2f}元")
        
        return buy_orders
    
    def execute_buy_orders(self, buy_orders: List[Dict], trade_date: str):
        """执行买入订单"""
        for order in buy_orders:
            if self.current_cash >= order['total_cost']:
                # 扣除资金
                self.current_cash -= order['total_cost']
                
                # 建立持仓
                position = Position(
                    ts_code=order['ts_code'],
                    name=order['name'],
                    buy_date=trade_date,
                    buy_price=order['price'],
                    shares=order['shares'],
                    buy_amount=order['amount']
                )
                
                self.positions[order['ts_code']] = position
                
                # 记录交易
                self.trades.append({
                    'date': trade_date,
                    'type': '买入',
                    'ts_code': order['ts_code'],
                    'name': order['name'],
                    'price': order['price'],
                    'shares': order['shares'],
                    'amount': order['amount'],
                    'cost': order['cost'],
                    'total_cost': order['total_cost'],
                    'cash_after': self.current_cash
                })
                
                if self.logger:
                    self.logger.info(f"[买入执行] {trade_date} 买入 {order['name']}({order['ts_code']}) "
                                   f"{order['shares']}股，价格{order['price']:.2f}元，"
                                   f"总成本{order['total_cost']:,.2f}元")
    
    def check_sell_conditions(self, trade_date: str) -> List[str]:
        """检查卖出条件"""
        to_sell = []
        
        for ts_code, position in self.positions.items():
            current_price = self.get_stock_price(ts_code, trade_date)
            
            if current_price <= 0:
                continue
            
            # 更新持有天数
            position.update_hold_days()
            
            # 计算收益率
            return_pct = position.get_return_pct(current_price)
            
            # 判断卖出条件
            should_sell = False
            sell_reason = ""
            
            # 1. 止损条件：跌幅超过5%
            if return_pct <= self.stop_loss_pct:
                should_sell = True
                sell_reason = f"止损(跌幅{return_pct:.2f}%)"
            
            # 2. 持有5天收益为负的条件
            elif position.hold_days >= 5 and return_pct < 0:
                should_sell = True
                sell_reason = f"持有5天亏损卖出(收益{return_pct:.2f}%)"
            
            # 3. 持有天数条件：持有到第11天卖出（持有天数为10天后卖出）
            elif position.hold_days >= self.max_hold_days:
                should_sell = True
                sell_reason = f"到期卖出(持有{position.hold_days}天)"
            
            if should_sell:
                to_sell.append(ts_code)
                if self.logger:
                    self.logger.info(f"[卖出条件] {trade_date} {position.name}({ts_code}) 触发卖出: {sell_reason}")
        
        return to_sell
    
    def execute_sell_orders(self, sell_list: List[str], trade_date: str):
        """执行卖出订单"""
        for ts_code in sell_list:
            if ts_code not in self.positions:
                continue
            
            position = self.positions[ts_code]
            current_price = self.get_stock_price(ts_code, trade_date)
            
            if current_price <= 0:
                continue
            
            # 计算卖出金额
            sell_amount_before_cost = position.shares * current_price
            transaction_cost = sell_amount_before_cost * self.transaction_cost
            net_amount = sell_amount_before_cost - transaction_cost
            
            # 计算收益
            profit = net_amount - position.buy_amount
            profit_pct = (current_price - position.buy_price) / position.buy_price * 100
            
            # 增加现金
            self.current_cash += net_amount
            
            # 记录交易
            self.trades.append({
                'date': trade_date,
                'type': '卖出',
                'ts_code': ts_code,
                'name': position.name,
                'buy_date': position.buy_date,
                'buy_price': position.buy_price,
                'sell_price': current_price,
                'shares': position.shares,
                'buy_amount': position.buy_amount,
                'sell_amount': sell_amount_before_cost,
                'cost': transaction_cost,
                'net_amount': net_amount,
                'profit': profit,
                'profit_pct': profit_pct,
                'hold_days': position.hold_days,
                'cash_after': self.current_cash
            })
            
            if self.logger:
                self.logger.info(f"[卖出执行] {trade_date} 卖出 {position.name}({ts_code}) "
                               f"{position.shares}股，价格{current_price:.2f}元，"
                               f"收益{profit:+.2f}元({profit_pct:+.2f}%)，持有{position.hold_days}天")
            
            # 删除持仓
            del self.positions[ts_code]
    
    def calculate_daily_value(self, trade_date: str):
        """计算当日账户总价值"""
        total_value = self.current_cash
        position_value = 0.0
        
        for position in self.positions.values():
            current_price = self.get_stock_price(position.ts_code, trade_date)
            if current_price > 0:
                position_value += position.get_current_value(current_price)
        
        total_value += position_value
        
        daily_return = (total_value - self.initial_capital) / self.initial_capital * 100
        
        self.daily_values.append({
            'date': trade_date,
            'cash': self.current_cash,
            'position_value': position_value,
            'total_value': total_value,
            'daily_return': daily_return,
            'position_count': len(self.positions)
        })
        
        if self.logger:
            self.logger.info(f"[账户价值] {trade_date} 现金:{self.current_cash:,.2f}元, "
                           f"持仓市值:{position_value:,.2f}元, "
                           f"总资产:{total_value:,.2f}元, "
                           f"收益率:{daily_return:+.2f}%, 持仓数:{len(self.positions)}只")
    
    def run_backtest(self):
        """运行回测"""
        if self.logger:
            self.logger.info("[回测开始] 开始执行强势非涨停策略回测")
        
        # 获取交易日期
        trade_dates = self.get_trading_dates_2024_to_now()
        
        if len(trade_dates) < 10:
            if self.logger:
                self.logger.error("[回测错误] 交易日期数据不足")
            return None
        
        print(f"\n📅 回测时间范围: {trade_dates[0]} 到 {trade_dates[-1]}")
        print(f"📊 总交易日数: {len(trade_dates)}天")
        print(f"💰 初始资金: {self.initial_capital:,.2f}元")
        
        # 开始回测循环
        for i, trade_date in enumerate(trade_dates):
            if i < 5:  # 前5天用于计算历史数据，不进行交易
                continue
            
            if (i - 4) % 50 == 0 or i == len(trade_dates) - 1:
                if self.logger:
                    self.logger.info(f"[回测进度] 已处理{i-4}/{len(trade_dates)-5}个交易日")
            
            # 1. 检查卖出条件并执行卖出
            sell_list = self.check_sell_conditions(trade_date)
            if sell_list:
                self.execute_sell_orders(sell_list, trade_date)
            
            # 2. 筛选股票
            candidate_stocks = self.screen_stocks(trade_date)
            
            # 3. 计算买入订单
            if candidate_stocks and self.current_cash > 1000:  # 至少有1000元现金才考虑买入
                buy_orders = self.calculate_position_sizes(self.current_cash, candidate_stocks)
                if buy_orders:
                    self.execute_buy_orders(buy_orders, trade_date)
            
            # 4. 计算当日账户价值
            self.calculate_daily_value(trade_date)
        
        if self.logger:
            self.logger.info("[回测完成] 强势非涨停策略回测执行完成")
        
        return self.analyze_results()
    
    def analyze_results(self) -> Dict[str, Any]:
        """分析回测结果"""
        if not self.daily_values:
            return {}
        
        # 基本统计
        final_value = self.daily_values[-1]['total_value']
        total_return = (final_value - self.initial_capital) / self.initial_capital * 100
        
        # 交易统计
        buy_trades = [t for t in self.trades if t['type'] == '买入']
        sell_trades = [t for t in self.trades if t['type'] == '卖出']
        
        # 收益统计
        profits = [t['profit'] for t in sell_trades if 'profit' in t]
        profit_pcts = [t['profit_pct'] for t in sell_trades if 'profit_pct' in t]
        
        # 计算胜率
        winning_trades = [p for p in profits if p > 0]
        losing_trades = [p for p in profits if p < 0]
        
        # 计算最大回撤
        peak_value = self.initial_capital
        max_drawdown = 0.0
        for daily in self.daily_values:
            if daily['total_value'] > peak_value:
                peak_value = daily['total_value']
            else:
                drawdown = (peak_value - daily['total_value']) / peak_value * 100
                max_drawdown = max(max_drawdown, drawdown)
        
        # 计算年化收益率（假设一年250个交易日）
        trading_days = len(self.daily_values)
        if trading_days > 0:
            years = trading_days / 250.0
            annual_return = (pow(final_value / self.initial_capital, 1/years) - 1) * 100 if years > 0 else 0
        else:
            annual_return = 0
        
        results = {
            'backtest_summary': {
                'start_date': self.daily_values[0]['date'],
                'end_date': self.daily_values[-1]['date'],
                'trading_days': trading_days,
                'initial_capital': self.initial_capital,
                'final_value': final_value,
                'total_return': total_return,
                'annual_return': annual_return,
                'max_drawdown': max_drawdown
            },
            'trade_summary': {
                'total_buy_trades': len(buy_trades),
                'total_sell_trades': len(sell_trades),
                'completed_trades': len(sell_trades),
                'current_positions': len(self.positions)
            },
            'performance_summary': {
                'total_profit': sum(profits) if profits else 0,
                'average_profit': sum(profits) / len(profits) if profits else 0,
                'average_profit_pct': sum(profit_pcts) / len(profit_pcts) if profit_pcts else 0,
                'winning_trades': len(winning_trades),
                'losing_trades': len(losing_trades),
                'win_rate': len(winning_trades) / len(profits) * 100 if profits else 0,
                'best_trade': max(profits) if profits else 0,
                'worst_trade': min(profits) if profits else 0,
                'best_trade_pct': max(profit_pcts) if profit_pcts else 0,
                'worst_trade_pct': min(profit_pcts) if profit_pcts else 0
            },
            'daily_values': self.daily_values,
            'trades': self.trades,
            'current_positions': [
                {
                    'ts_code': pos.ts_code,
                    'name': pos.name,
                    'buy_date': pos.buy_date,
                    'buy_price': pos.buy_price,
                    'shares': pos.shares,
                    'buy_amount': pos.buy_amount,
                    'hold_days': pos.hold_days
                }
                for pos in self.positions.values()
            ]
        }
        
        return results


def export_backtest_results(results: Dict, output_dir: str = "/Users/nxm/PycharmProjects/dataDig/results"):
    """导出回测结果"""
    import os
    
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 导出交易记录
    if results['trades']:
        trades_df = pd.DataFrame(results['trades'])
        trades_file = os.path.join(output_dir, f"强势策略回测_交易记录_{timestamp}.csv")
        trades_df.to_csv(trades_file, index=False, encoding='utf-8-sig')
        print(f"📁 交易记录已导出: {trades_file}")
    
    # 导出每日价值
    if results['daily_values']:
        daily_df = pd.DataFrame(results['daily_values'])
        daily_file = os.path.join(output_dir, f"强势策略回测_每日价值_{timestamp}.csv")
        daily_df.to_csv(daily_file, index=False, encoding='utf-8-sig')
        print(f"📁 每日价值已导出: {daily_file}")
    
    # 导出回测报告
    summary_file = os.path.join(output_dir, f"强势策略回测_报告_{timestamp}.txt")
    with open(summary_file, 'w', encoding='utf-8') as f:
        summary = results['backtest_summary']
        trade_summary = results['trade_summary']
        perf_summary = results['performance_summary']
        
        f.write("强势非涨停策略回测报告\n")
        f.write("="*50 + "\n")
        f.write(f"策略描述: 过去5日涨幅>20%且单日涨幅≤9.5%的沪深主板股票(排除ST股)\n")
        f.write(f"回测时间: {summary['start_date']} 到 {summary['end_date']}\n")
        f.write(f"交易日数: {summary['trading_days']}天\n")
        f.write(f"初始资金: {summary['initial_capital']:,.2f}元\n")
        f.write(f"期末资金: {summary['final_value']:,.2f}元\n")
        f.write(f"总收益率: {summary['total_return']:+.2f}%\n")
        f.write(f"年化收益率: {summary['annual_return']:+.2f}%\n")
        f.write(f"最大回撤: {summary['max_drawdown']:.2f}%\n")
        f.write("\n")
        
        f.write("交易统计:\n")
        f.write(f"  买入次数: {trade_summary['total_buy_trades']}次\n")
        f.write(f"  卖出次数: {trade_summary['total_sell_trades']}次\n")
        f.write(f"  完成交易: {trade_summary['completed_trades']}笔\n")
        f.write(f"  当前持仓: {trade_summary['current_positions']}只\n")
        f.write("\n")
        
        f.write("收益统计:\n")
        f.write(f"  总盈亏: {perf_summary['total_profit']:+,.2f}元\n")
        f.write(f"  平均盈亏: {perf_summary['average_profit']:+,.2f}元\n")
        f.write(f"  平均收益率: {perf_summary['average_profit_pct']:+.2f}%\n")
        f.write(f"  胜率: {perf_summary['win_rate']:.2f}%\n")
        f.write(f"  盈利次数: {perf_summary['winning_trades']}次\n")
        f.write(f"  亏损次数: {perf_summary['losing_trades']}次\n")
        f.write(f"  最佳交易: {perf_summary['best_trade']:+,.2f}元 ({perf_summary['best_trade_pct']:+.2f}%)\n")
        f.write(f"  最差交易: {perf_summary['worst_trade']:+,.2f}元 ({perf_summary['worst_trade_pct']:+.2f}%)\n")
    
    print(f"📁 回测报告已导出: {summary_file}")


def main():
    """主函数"""
    logger = get_logger(__name__)
    logger.info("[强势策略回测开始] 强势非涨停策略回测程序启动")
    
    # 设置随机种子以保证可重现性
    random.seed(42)
    np.random.seed(42)
    
    # 初始化服务
    settings = load_settings()
    mysql_client = MySQLClient(
        host=settings.database.host,
        port=settings.database.port,
        user=settings.database.user,
        password=settings.database.password,
        db_name=settings.database.name
    )
    
    print("\n" + "="*60)
    print("              强势非涨停策略回测系统")
    print("="*60)
    print("\n📊 策略规则:")
    print("  选股条件:")
    print("    • 过去5个交易日涨幅超过20%")
    print("    • 过去5个交易日没有一天涨幅超过9.5%")
    print("    • 限制沪深主板股票（排除创业板300和科创板688）")
    print("    • 排除ST股票（包括ST、*ST、SST等特别处理股票）")
    print("    • 排除已持仓股票（避免重复买入）")
    print("    • 排除无交易股票（一字板等无法买入的股票）")
    print("    • 排除涨幅过大股票（20日内涨幅不超过160%）")
    print("    • 单日筛选股票数量不超过10只")
    print("  ")
    print("  交易规则:")
    print("    • 初始资金: 10万元")
    print("    • 交易费用: 买卖各0.1%")
    print("    • 持仓限制: 最多同时持有5只股票")
    print("    • 仓位分配: 当前现金/(5-持仓股票数量)")
    print("    • 卖出条件: 跌破买入价5%止损，持有5天收益为负卖出，或持有到第11天卖出")
    print("    • 选股限制: 当日候选股票过多时随机选择")
    print("\n⚡ 开始执行回测...")
    
    try:
        with mysql_client.get_session() as session:
            backtester = StrongMomentumBacktester(session, logger, initial_capital=100000)
            
            # 运行回测
            results = backtester.run_backtest()
            
            if not results:
                print("\n❌ 回测失败")
                return
            
            # 显示回测结果
            summary = results['backtest_summary']
            trade_summary = results['trade_summary']
            perf_summary = results['performance_summary']
            
            print("\n" + "="*60)
            print("              回测结果汇总")
            print("="*60)
            
            print(f"\n📅 回测时间: {summary['start_date']} 到 {summary['end_date']}")
            print(f"📊 交易日数: {summary['trading_days']}天")
            print(f"💰 初始资金: {summary['initial_capital']:,.2f}元")
            print(f"💰 期末资金: {summary['final_value']:,.2f}元")
            print(f"📈 总收益率: {summary['total_return']:+.2f}%")
            print(f"📈 年化收益率: {summary['annual_return']:+.2f}%")
            print(f"📉 最大回撤: {summary['max_drawdown']:.2f}%")
            
            print(f"\n🔄 交易统计:")
            print(f"  买入次数: {trade_summary['total_buy_trades']}次")
            print(f"  卖出次数: {trade_summary['total_sell_trades']}次")
            print(f"  完成交易: {trade_summary['completed_trades']}笔")
            print(f"  当前持仓: {trade_summary['current_positions']}只")
            
            print(f"\n💰 收益统计:")
            print(f"  总盈亏: {perf_summary['total_profit']:+,.2f}元")
            print(f"  平均盈亏: {perf_summary['average_profit']:+,.2f}元")
            print(f"  平均收益率: {perf_summary['average_profit_pct']:+.2f}%")
            print(f"  胜率: {perf_summary['win_rate']:.1f}%")
            print(f"  盈利次数: {perf_summary['winning_trades']}次")
            print(f"  亏损次数: {perf_summary['losing_trades']}次")
            print(f"  最佳交易: {perf_summary['best_trade']:+,.2f}元 ({perf_summary['best_trade_pct']:+.2f}%)")
            print(f"  最差交易: {perf_summary['worst_trade']:+,.2f}元 ({perf_summary['worst_trade_pct']:+.2f}%)")
            
            # 策略效果评价
            print(f"\n🎯 策略效果评价:")
            if summary['total_return'] > 0:
                if summary['annual_return'] > 15:
                    print("✅ 策略表现优秀: 年化收益率超过15%")
                elif summary['annual_return'] > 5:
                    print("⚠️  策略表现良好: 年化收益率在5-15%之间")
                else:
                    print("⚠️  策略表现一般: 年化收益率较低")
            else:
                print("❌ 策略表现不佳: 总收益为负")
            
            if perf_summary['win_rate'] > 60:
                print("✅ 胜率表现优秀: 超过60%")
            elif perf_summary['win_rate'] > 50:
                print("⚠️  胜率表现一般: 50-60%之间")
            else:
                print("❌ 胜率偏低: 低于50%")
            
            if summary['max_drawdown'] < 15:
                print("✅ 风险控制良好: 最大回撤小于15%")
            elif summary['max_drawdown'] < 25:
                print("⚠️  风险控制一般: 最大回撤在15-25%之间")
            else:
                print("❌ 风险控制较差: 最大回撤超过25%")
            
            # 显示当前持仓
            if results['current_positions']:
                print(f"\n📋 当前持仓 ({len(results['current_positions'])}只):")
                for pos in results['current_positions']:
                    print(f"  {pos['name']}({pos['ts_code']}) - 买入日期:{pos['buy_date']}, "
                          f"买入价:{pos['buy_price']:.2f}元, 股数:{pos['shares']}股, "
                          f"持有{pos['hold_days']}天")
            
            # 导出结果
            print(f"\n📁 正在导出回测结果...")
            export_backtest_results(results)
            
            print("\n" + "="*60)
            print("              强势策略回测完成")
            print("="*60)
            print("\n💡 投资建议:")
            print("  1. 注意控制单笔投资金额，分散风险")
            print("  2. 严格执行5%止损纪律，保护本金")
            print("  3. 关注市场整体走势，避免在熊市中使用")
            print("  4. 持有5天后若收益为负及时止损，避免进一步亏损")
            print("  5. 可以结合其他技术指标优化买卖时机")
            print("  6. 定期回测和优化策略参数")
            
        logger.info("[强势策略回测完成] 回测程序执行完成")
        
    except Exception as e:
        logger.error(f"[强势策略回测错误] 执行过程中发生错误: {str(e)}")
        print(f"\n❌ 执行过程中发生错误: {str(e)}")
        print("📋 请检查日志文件获取详细错误信息")
        import traceback
        print("\n详细错误信息:")
        traceback.print_exc()


if __name__ == "__main__":
    main()
