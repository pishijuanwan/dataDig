#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
强势非涨停策略批量分析

策略逻辑：
1. 个股过去5个交易日涨幅超过20%
2. 过去5个交易日没有一天涨幅超过9.5%
3. 限制沪深主板股票（排除创业板300和科创板688）
4. 单日筛选股票数量不超过10只（超过则丢弃该日所有数据）
5. 分析这些股票5日后、10日后的表现

理论基础：
- 强势上涨但单日涨幅不超过9.5%的股票可能还有上涨空间
- 避免追高波动股票，降低风险
- 寻找稳步上涨的理性强势股

用法示例:
python strong_momentum_strategy.py
"""

import sys
import os
from datetime import datetime, timedelta
from typing import List, Dict, Any
import pandas as pd

# 添加项目根路径到Python路径
sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))

from src.config.settings import load_settings
from src.db.mysql_client import MySQLClient
from src.app_logging.logger import get_logger


class StrongMomentumAnalyzer:
    """强势非涨停策略分析器"""
    
    def __init__(self, session, logger):
        self.session = session
        self.logger = logger
        
        if self.logger:
            self.logger.info("[强势策略分析器初始化] 强势非涨停策略分析器已初始化")
    
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
        
        # 确保有足够后续数据用于分析（排除最近15天）
        if len(result) > 15:
            analysis_dates = result[:-15]  # 排除最近15天，确保有后续数据
        else:
            analysis_dates = result[:-5] if len(result) > 5 else result
        
        if self.logger:
            self.logger.info(f"[获取交易日期] 共找到{len(result)}个交易日，可分析{len(analysis_dates)}个交易日")
        
        return list(analysis_dates)
    
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
    
    def get_future_performance_batch(self, stocks_dates: List[tuple], days: int = 5) -> Dict[tuple, float]:
        """批量获取股票未来表现（从数据库）"""
        from src.models.daily_price import DailyPrice
        from sqlalchemy import select, and_
        
        if self.logger:
            self.logger.info(f"[批量获取未来表现] 开始计算{len(stocks_dates)}个股票-日期组合的{days}日后表现")
        
        future_performance = {}
        
        # 按股票分组处理
        stocks_by_code = {}
        for ts_code, trade_date in stocks_dates:
            if ts_code not in stocks_by_code:
                stocks_by_code[ts_code] = []
            stocks_by_code[ts_code].append(trade_date)
        
        for ts_code, dates_list in stocks_by_code.items():
            # 获取这只股票的所有价格数据
            max_date = max(dates_list)
            
            stmt = select(
                DailyPrice.trade_date,
                DailyPrice.close
            ).where(
                and_(
                    DailyPrice.ts_code == ts_code,
                    DailyPrice.trade_date >= min(dates_list)
                )
            ).order_by(DailyPrice.trade_date)
            
            price_data = self.session.execute(stmt).fetchall()
            
            # 转换为字典和列表
            price_dict = {date: price for date, price in price_data}
            price_dates = [date for date, _ in price_data]
            
            # 为每个日期计算未来表现
            for start_date in dates_list:
                try:
                    if start_date not in price_dates:
                        continue
                    
                    start_idx = price_dates.index(start_date)
                    target_idx = start_idx + days
                    
                    if target_idx < len(price_dates):
                        start_price = price_dict[start_date]
                        target_date = price_dates[target_idx]
                        target_price = price_dict[target_date]
                        
                        if start_price and start_price > 0:
                            performance = (target_price - start_price) / start_price * 100
                            future_performance[(ts_code, start_date)] = performance
                
                except (ValueError, IndexError, KeyError):
                    continue
        
        if self.logger:
            self.logger.info(f"[批量获取未来表现] 计算完成，成功计算{len(future_performance)}个未来表现")
        
        return future_performance
    
    def analyze_strong_momentum_opportunities(self, 
                                            min_5day_return: float = 20.0,
                                            max_daily_limit: float = 9.5):
        """分析2024年至今所有强势非涨停机会"""
        
        if self.logger:
            self.logger.info("[开始强势策略分析] 开始分析2024年至今强势非涨停机会")
        
        # 1. 获取所有交易日期
        trade_dates = self.get_trading_dates_2024_to_now()
        
        if not trade_dates:
            if self.logger:
                self.logger.error("[强势策略分析] 未找到交易日期数据")
            return None
        
        # 只分析有足够历史数据的日期（至少第6个交易日开始）
        analysis_dates = trade_dates[5:] if len(trade_dates) > 5 else []
        
        if not analysis_dates:
            if self.logger:
                self.logger.error("[强势策略分析] 交易日期不足")
            return None
        
        print(f"\\n📅 分析时间范围: {analysis_dates[0]} 到 {analysis_dates[-1]}")
        print(f"📊 总分析日数: {len(analysis_dates)}天")
        
        # 2. 逐日筛选符合条件的股票
        all_opportunities = []
        processed_days = 0
        daily_results = {}  # 存储每日的筛选结果
        
        for trade_date in analysis_dates:
            processed_days += 1
            
            if processed_days % 50 == 0 or processed_days == len(analysis_dates):
                if self.logger:
                    self.logger.info(f"[筛选进度] 已处理{processed_days}/{len(analysis_dates)}个交易日")
            
            # 获取当日股票列表
            stock_list = self.get_daily_stock_list(trade_date)
            
            if not stock_list:
                continue
            
            # 筛选符合条件的股票（先存储到临时列表）
            daily_opportunities = []
            
            for ts_code, name, close, vol in stock_list:
                # 1. 首先检查是否为沪深主板股票
                if not self.is_main_board_stock(ts_code):
                    continue
                
                # 2. 获取过去5日表现
                performance_5d = self.get_stock_5day_performance(ts_code, trade_date)
                
                if not performance_5d:
                    continue
                
                # 3. 检查是否符合涨幅和涨停条件
                if (performance_5d['total_return_5d'] >= min_5day_return and 
                    not performance_5d['has_limit_up']):
                    
                    opportunity = {
                        'trade_date': trade_date,
                        'ts_code': ts_code,
                        'name': name,
                        'close': close,
                        'return_5d': performance_5d['total_return_5d'],
                        'has_limit_up': performance_5d['has_limit_up'],
                        'daily_changes': performance_5d['daily_changes'],
                        'avg_volume': performance_5d['avg_volume']
                    }
                    
                    daily_opportunities.append(opportunity)
            
            # 检查当日股票数量是否超过10只
            if len(daily_opportunities) > 10:
                if self.logger:
                    self.logger.info(f"[数量过滤] {trade_date}: 找到{len(daily_opportunities)}个机会，超过10只限制，丢弃该日数据")
                continue  # 丢弃这一天的所有数据
            elif len(daily_opportunities) > 0:
                # 当日股票数量在限制范围内，添加到总结果
                all_opportunities.extend(daily_opportunities)
                if self.logger:
                    self.logger.info(f"[筛选结果] {trade_date}: 找到{len(daily_opportunities)}个强势非涨停机会")
        
        if not all_opportunities:
            print("\\n❌ 未找到符合条件的强势非涨停机会")
            return None
        
        print(f"\\n✅ 筛选完成: 找到 {len(all_opportunities)} 个符合条件的强势非涨停机会")
        
        # 3. 批量计算未来表现
        print("\\n📈 正在计算5日后和10日后表现...")
        
        stocks_dates_for_future = [(opp['ts_code'], opp['trade_date']) for opp in all_opportunities]
        
        # 5日后表现
        future_5d = self.get_future_performance_batch(stocks_dates_for_future, 5)
        
        # 10日后表现
        future_10d = self.get_future_performance_batch(stocks_dates_for_future, 10)
        
        # 4. 整合结果
        results = []
        for opp in all_opportunities:
            key = (opp['ts_code'], opp['trade_date'])
            
            result = opp.copy()
            result['return_after_5d'] = future_5d.get(key, None)
            result['return_after_10d'] = future_10d.get(key, None)
            
            results.append(result)
        
        # 5. 计算统计指标
        stats = self.calculate_strategy_statistics(results)
        
        if self.logger:
            self.logger.info("[强势策略分析完成] 2024年至今强势非涨停机会分析完成")
        
        return {
            'opportunities': results,
            'statistics': stats,
            'total_opportunities': len(results),
            'date_range': (analysis_dates[0], analysis_dates[-1]),
            'trading_days_analyzed': len(analysis_dates)
        }
    
    def calculate_strategy_statistics(self, results: List[Dict]) -> Dict[str, Any]:
        """计算策略统计指标"""
        
        if not results:
            return {}
        
        # 提取有效的收益率数据
        returns_5d = [r['return_after_5d'] for r in results if r['return_after_5d'] is not None]
        returns_10d = [r['return_after_10d'] for r in results if r['return_after_10d'] is not None]
        
        # 提取5日涨幅数据
        initial_returns = [r['return_5d'] for r in results if r['return_5d'] is not None]
        
        stats = {
            'total_opportunities': len(results),
            'valid_5d_count': len(returns_5d),
            'valid_10d_count': len(returns_10d)
        }
        
        # 初始5日涨幅统计
        if initial_returns:
            stats['initial_5d_stats'] = {
                'mean_return': sum(initial_returns) / len(initial_returns),
                'median_return': sorted(initial_returns)[len(initial_returns) // 2],
                'max_return': max(initial_returns),
                'min_return': min(initial_returns)
            }
        
        # 5日后表现统计
        if returns_5d:
            stats['5d_stats'] = {
                'mean_return': sum(returns_5d) / len(returns_5d),
                'median_return': sorted(returns_5d)[len(returns_5d) // 2],
                'max_return': max(returns_5d),
                'min_return': min(returns_5d),
                'positive_count': sum(1 for r in returns_5d if r > 0),
                'negative_count': sum(1 for r in returns_5d if r < 0),
                'win_rate': sum(1 for r in returns_5d if r > 0) / len(returns_5d) * 100
            }
        
        # 10日后表现统计
        if returns_10d:
            stats['10d_stats'] = {
                'mean_return': sum(returns_10d) / len(returns_10d),
                'median_return': sorted(returns_10d)[len(returns_10d) // 2],
                'max_return': max(returns_10d),
                'min_return': min(returns_10d),
                'positive_count': sum(1 for r in returns_10d if r > 0),
                'negative_count': sum(1 for r in returns_10d if r < 0),
                'win_rate': sum(1 for r in returns_10d if r > 0) / len(returns_10d) * 100
            }
        
        return stats


def export_momentum_results(results_data: Dict, output_dir: str = "/Users/nxm/PycharmProjects/dataDig/results"):
    """导出强势策略分析结果"""
    import os
    
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 导出详细数据
    if results_data['opportunities']:
        df = pd.DataFrame(results_data['opportunities'])
        detail_file = os.path.join(output_dir, f"强势非涨停策略2024年批量分析_{timestamp}.csv")
        df.to_csv(detail_file, index=False, encoding='utf-8-sig')
        print(f"📁 详细数据已导出: {detail_file}")
    
    # 导出统计摘要
    summary_file = os.path.join(output_dir, f"强势非涨停策略2024年统计摘要_{timestamp}.txt")
    with open(summary_file, 'w', encoding='utf-8') as f:
        stats = results_data['statistics']
        f.write("2024年至今强势非涨停策略批量分析统计报告\\n")
        f.write("="*50 + "\\n")
        f.write(f"策略描述: 过去5日涨幅>20%且单日涨幅≤9.5%且限制沪深主板且单日数量≤10只\\n")
        f.write(f"分析时间范围: {results_data['date_range'][0]} 到 {results_data['date_range'][1]}\\n")
        f.write(f"分析交易日数: {results_data['trading_days_analyzed']}天\\n")
        f.write(f"找到投资机会: {results_data['total_opportunities']}个\\n")
        f.write("\\n")
        
        if 'initial_5d_stats' in stats:
            f.write("筛选条件统计（过去5日涨幅）:\\n")
            init = stats['initial_5d_stats']
            f.write(f"  平均涨幅: {init['mean_return']:.2f}%\\n")
            f.write(f"  中位数涨幅: {init['median_return']:.2f}%\\n")
            f.write(f"  最大涨幅: {init['max_return']:.2f}%\\n")
            f.write(f"  最小涨幅: {init['min_return']:.2f}%\\n")
            f.write("\\n")
        
        if '5d_stats' in stats:
            f.write("5日后表现统计:\\n")
            s5 = stats['5d_stats']
            f.write(f"  有效数据: {stats['valid_5d_count']}个\\n")
            f.write(f"  平均收益率: {s5['mean_return']:.2f}%\\n")
            f.write(f"  中位数收益率: {s5['median_return']:.2f}%\\n")
            f.write(f"  最大收益率: {s5['max_return']:.2f}%\\n")
            f.write(f"  最小收益率: {s5['min_return']:.2f}%\\n")
            f.write(f"  胜率: {s5['win_rate']:.2f}%\\n")
            f.write(f"  上涨次数: {s5['positive_count']}次\\n")
            f.write(f"  下跌次数: {s5['negative_count']}次\\n")
            f.write("\\n")
        
        if '10d_stats' in stats:
            f.write("10日后表现统计:\\n")
            s10 = stats['10d_stats']
            f.write(f"  有效数据: {stats['valid_10d_count']}个\\n")
            f.write(f"  平均收益率: {s10['mean_return']:.2f}%\\n")
            f.write(f"  中位数收益率: {s10['median_return']:.2f}%\\n")
            f.write(f"  最大收益率: {s10['max_return']:.2f}%\\n")
            f.write(f"  最小收益率: {s10['min_return']:.2f}%\\n")
            f.write(f"  胜率: {s10['win_rate']:.2f}%\\n")
            f.write(f"  上涨次数: {s10['positive_count']}次\\n")
            f.write(f"  下跌次数: {s10['negative_count']}次\\n")
    
    print(f"📁 统计摘要已导出: {summary_file}")


def main():
    """主函数"""
    logger = get_logger(__name__)
    logger.info("[强势策略程序开始] 2024年强势非涨停策略分析脚本启动")
    
    # 初始化服务
    settings = load_settings()
    mysql_client = MySQLClient(
        host=settings.database.host,
        port=settings.database.port,
        user=settings.database.user,
        password=settings.database.password,
        db_name=settings.database.name
    )
    
    print("\\n" + "="*60)
    print("              2024年强势非涨停策略分析")
    print("="*60)
    print("\\n📊 策略逻辑:")
    print("  1. 个股过去5个交易日涨幅超过20%")
    print("  2. 过去5个交易日没有一天涨幅超过9.5%")
    print("  3. 限制沪深主板股票（排除创业板300和科创板688）")
    print("  4. 单日筛选股票数量不超过10只（超过则丢弃该日所有数据）")
    print("  5. 分析5日后、10日后的表现")
    print("\\n💡 策略理念:")
    print("  - 寻找强势上涨但单日涨幅不超过9.5%的稳健强势股")
    print("  - 避免追高波动股票，降低追高风险")
    print("  - 专注主板优质股票，避开高风险板块")
    print("  - 避免市场过热时期，确保筛选精准度")
    print("  - 捕捉稳步上涨的持续潜力")
    print("\\n⚡ 数据来源: 直接从数据库查询，无需调用API")
    
    try:
        with mysql_client.get_session() as session:
            analyzer = StrongMomentumAnalyzer(session, logger)
            
            # 执行批量分析
            print("\\n🔍 开始批量分析...")
            results = analyzer.analyze_strong_momentum_opportunities(
                min_5day_return=20.0,    # 5日涨幅≥20%
                max_daily_limit=9.5      # 单日涨幅≤9.5%
            )
            
            if not results:
                print("\\n❌ 分析失败或未找到符合条件的机会")
                return
            
            # 显示统计结果
            stats = results['statistics']
            
            print("\\n" + "="*60)
            print("              统计结果汇总")
            print("="*60)
            
            print(f"\\n📅 分析时间范围: {results['date_range'][0]} 到 {results['date_range'][1]}")
            print(f"📊 分析交易日数: {results['trading_days_analyzed']}天")
            print(f"🎯 找到投资机会: {results['total_opportunities']}个")
            
            if 'initial_5d_stats' in stats:
                print("\\n📊 筛选条件统计（过去5日涨幅）:")
                init = stats['initial_5d_stats']
                print(f"  💰 平均涨幅: {init['mean_return']:.2f}%")
                print(f"  📊 中位数涨幅: {init['median_return']:.2f}%")
                print(f"  🔥 最大涨幅: {init['max_return']:.2f}%")
                print(f"  ❄️  最小涨幅: {init['min_return']:.2f}%")
            
            if '5d_stats' in stats:
                print("\\n📈 5日后表现统计:")
                s5 = stats['5d_stats']
                print(f"  💰 平均收益率: {s5['mean_return']:+.2f}%")
                print(f"  📊 中位数收益率: {s5['median_return']:+.2f}%")
                print(f"  🔥 最大收益率: {s5['max_return']:+.2f}%")
                print(f"  ❄️  最小收益率: {s5['min_return']:+.2f}%")
                print(f"  🎯 胜率: {s5['win_rate']:.1f}%")
                print(f"  📈 上涨次数: {s5['positive_count']}次")
                print(f"  📉 下跌次数: {s5['negative_count']}次")
                print(f"  📋 有效数据: {stats['valid_5d_count']}个")
            
            if '10d_stats' in stats:
                print("\\n📈 10日后表现统计:")
                s10 = stats['10d_stats']
                print(f"  💰 平均收益率: {s10['mean_return']:+.2f}%")
                print(f"  📊 中位数收益率: {s10['median_return']:+.2f}%")
                print(f"  🔥 最大收益率: {s10['max_return']:+.2f}%")
                print(f"  ❄️  最小收益率: {s10['min_return']:+.2f}%")
                print(f"  🎯 胜率: {s10['win_rate']:.1f}%")
                print(f"  📈 上涨次数: {s10['positive_count']}次")
                print(f"  📉 下跌次数: {s10['negative_count']}次")
                print(f"  📋 有效数据: {stats['valid_10d_count']}个")
            
            # 策略效果评价
            print("\\n🎯 策略效果评价:")
            
            if '5d_stats' in stats and '10d_stats' in stats:
                avg_5d = stats['5d_stats']['mean_return']
                avg_10d = stats['10d_stats']['mean_return']
                win_5d = stats['5d_stats']['win_rate']
                win_10d = stats['10d_stats']['win_rate']
                
                if avg_5d > 0 and avg_10d > 0:
                    print("✅ 策略整体有效: 短期和中期都有正收益")
                elif avg_5d > 0:
                    print("⚠️  策略短期有效: 5日表现良好，适合短线操作")
                elif avg_10d > 0:
                    print("⚠️  策略中期有效: 10日表现良好，需要耐心持有")
                else:
                    print("❌ 策略效果不佳: 平均收益为负，需要优化条件")
                
                if win_5d > 60 or win_10d > 60:
                    print("✅ 胜率表现优秀: 超过60%的机会获得正收益")
                elif win_5d > 50 or win_10d > 50:
                    print("⚠️  胜率表现一般: 约半数机会获得正收益")
                else:
                    print("❌ 胜率偏低: 多数机会仍为负收益")
                
                print(f"\\n📊 策略表现对比:")
                print(f"   5日 vs 10日平均收益: {avg_5d:+.2f}% vs {avg_10d:+.2f}%")
                print(f"   5日 vs 10日胜率: {win_5d:.1f}% vs {win_10d:.1f}%")
            
            # 导出结果
            print("\\n📁 正在导出分析结果...")
            export_momentum_results(results)
            
            print("\\n" + "="*60)
            print("              强势非涨停策略分析完成")
            print("="*60)
            print("\\n💡 使用建议:")
            print("  1. 关注胜率较高的时间维度进行操作")
            print("  2. 结合成交量和技术形态分析")
            print("  3. 避免在大盘调整时使用此策略")
            print("  4. 设置合理的止损位（建议-15%）")
            print("  5. 关注强势股的基本面支撑")
            
        logger.info("[强势策略程序完成] 2024年强势非涨停策略分析已完成")
        
    except Exception as e:
        logger.error(f"[强势策略程序错误] 执行过程中发生错误: {str(e)}")
        print(f"\\n❌ 执行过程中发生错误: {str(e)}")
        print("📋 请检查日志文件获取详细错误信息")
        import traceback
        print("\\n详细错误信息:")
        traceback.print_exc()


if __name__ == "__main__":
    main()
