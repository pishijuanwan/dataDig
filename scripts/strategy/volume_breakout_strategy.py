#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
放量突破横盘策略批量分析

策略逻辑：
1. 过去20个交易日最高价比最低价涨幅不超过5%（横盘整理）
2. 当天涨幅超过5%但小于9.5%（突破但非涨停）
3. 当天成交量是过去20个交易日平均成交量的3倍以上（放量突破）
4. 分析这些股票3日后、5日后、10日后的表现

理论基础：
- 长期横盘整理后的放量突破往往意味着趋势改变
- 适度涨幅（5%-9.5%）避免追高风险
- 放量确认突破的有效性
- 寻找低位突破的潜力股

用法示例:
python volume_breakout_strategy.py
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


class VolumeBreakoutAnalyzer:
    """放量突破横盘策略分析器"""
    
    def __init__(self, session, logger):
        self.session = session
        self.logger = logger
        
        if self.logger:
            self.logger.info("[放量突破策略分析器初始化] 放量突破横盘策略分析器已初始化")
    
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
    
    def check_consolidation_pattern(self, ts_code: str, end_date: str) -> Dict[str, Any]:
        """检查股票过去20日的横盘整理模式和当日表现"""
        from src.models.daily_price import DailyPrice
        from sqlalchemy import select, and_
        
        if self.logger:
            self.logger.debug(f"[检查横盘模式] 检查股票{ts_code}在{end_date}的横盘模式")
        
        # 获取包含end_date在内的最近21天数据（需要20天历史+当天）
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
        ).order_by(DailyPrice.trade_date.desc()).limit(21)
        
        result = self.session.execute(stmt).fetchall()
        
        if len(result) < 21:  # 需要至少21天数据
            return None
        
        # 按日期正序排列（result是倒序的）
        data = list(reversed(result))
        
        # 前20天的数据（用于计算横盘和平均成交量）
        historical_data = data[:20]
        current_day = data[20]  # 当天数据
        
        # 1. 检查过去20日的价格波动（使用收盘价）
        close_prices = [row[1] for row in historical_data if row[1] is not None]
        if not close_prices or len(close_prices) < 20:
            return None
        
        max_close = max(close_prices)
        min_close = min(close_prices)
        
        if min_close <= 0:
            return None
        
        price_range_pct = (max_close - min_close) / min_close * 100
        
        # 横盘条件：20日内涨幅不超过5%
        if price_range_pct > 5.0:
            return None
        
        # 2. 检查当日涨幅
        current_pct_chg = current_day[2]
        if current_pct_chg is None or current_pct_chg <= 5.0 or current_pct_chg >= 9.5:
            return None
        
        # 3. 检查当日成交量vs过去20日平均成交量
        historical_volumes = [row[3] for row in historical_data if row[3] is not None and row[3] > 0]
        if not historical_volumes:
            return None
        
        avg_volume = sum(historical_volumes) / len(historical_volumes)
        current_volume = current_day[3]
        
        if current_volume is None or current_volume <= 0 or avg_volume <= 0:
            return None
        
        volume_ratio = current_volume / avg_volume
        
        # 放量条件：当日成交量至少是过去20日平均的3倍
        if volume_ratio < 3.0:
            return None
        
        if self.logger:
            self.logger.info(f"[找到符合条件的股票] {ts_code} 在{end_date}: 20日波动{price_range_pct:.2f}%, 当日涨幅{current_pct_chg:.2f}%, 放量{volume_ratio:.2f}倍")
        
        return {
            'price_range_20d': price_range_pct,
            'max_close_20d': max_close,
            'min_close_20d': min_close,
            'current_pct_chg': current_pct_chg,
            'current_close': current_day[1],
            'avg_volume_20d': avg_volume,
            'current_volume': current_volume,
            'volume_ratio': volume_ratio,
            'trade_date': current_day[0]
        }
    
    def get_daily_stock_list(self, trade_date: str) -> List[tuple]:
        """获取指定日期的股票列表（从数据库）"""
        from src.models.daily_price import DailyPrice, StockBasic
        from sqlalchemy import select, and_
        
        stmt = select(
            DailyPrice.ts_code,
            StockBasic.name,
            DailyPrice.close,
            DailyPrice.vol,
            DailyPrice.pct_chg
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
    
    def analyze_volume_breakout_opportunities(self):
        """分析2024年至今所有放量突破横盘机会"""
        
        if self.logger:
            self.logger.info("[开始放量突破策略分析] 开始分析2024年至今放量突破横盘机会")
        
        # 1. 获取所有交易日期
        trade_dates = self.get_trading_dates_2024_to_now()
        
        if not trade_dates:
            if self.logger:
                self.logger.error("[放量突破策略分析] 未找到交易日期数据")
            return None
        
        # 只分析有足够历史数据的日期（至少第21个交易日开始，需要20天历史）
        analysis_dates = trade_dates[20:] if len(trade_dates) > 20 else []
        
        if not analysis_dates:
            if self.logger:
                self.logger.error("[放量突破策略分析] 交易日期不足")
            return None
        
        print(f"\n📅 分析时间范围: {analysis_dates[0]} 到 {analysis_dates[-1]}")
        print(f"📊 总分析日数: {len(analysis_dates)}天")
        
        # 2. 逐日筛选符合条件的股票
        all_opportunities = []
        processed_days = 0
        
        for trade_date in analysis_dates:
            processed_days += 1
            
            if processed_days % 50 == 0 or processed_days == len(analysis_dates):
                if self.logger:
                    self.logger.info(f"[筛选进度] 已处理{processed_days}/{len(analysis_dates)}个交易日")
            
            # 获取当日股票列表
            stock_list = self.get_daily_stock_list(trade_date)
            
            if not stock_list:
                continue
            
            # 筛选符合条件的股票
            for ts_code, name, close, vol, pct_chg in stock_list:
                # 检查是否符合横盘突破条件
                pattern_result = self.check_consolidation_pattern(ts_code, trade_date)
                
                if pattern_result:
                    opportunity = {
                        'trade_date': trade_date,
                        'ts_code': ts_code,
                        'name': name,
                        'close': close,
                        'pct_chg': pattern_result['current_pct_chg'],
                        'price_range_20d': pattern_result['price_range_20d'],
                        'max_close_20d': pattern_result['max_close_20d'],
                        'min_close_20d': pattern_result['min_close_20d'],
                        'volume_ratio': pattern_result['volume_ratio'],
                        'avg_volume_20d': pattern_result['avg_volume_20d'],
                        'current_volume': pattern_result['current_volume']
                    }
                    
                    all_opportunities.append(opportunity)
        
        if not all_opportunities:
            print("\n❌ 未找到符合条件的放量突破横盘机会")
            return None
        
        print(f"\n✅ 筛选完成: 找到 {len(all_opportunities)} 个符合条件的放量突破横盘机会")
        
        # 3. 批量计算未来表现
        print("\n📈 正在计算3日后、5日后和10日后表现...")
        
        stocks_dates_for_future = [(opp['ts_code'], opp['trade_date']) for opp in all_opportunities]
        
        # 3日后表现
        future_3d = self.get_future_performance_batch(stocks_dates_for_future, 3)
        
        # 5日后表现
        future_5d = self.get_future_performance_batch(stocks_dates_for_future, 5)
        
        # 10日后表现
        future_10d = self.get_future_performance_batch(stocks_dates_for_future, 10)
        
        # 4. 整合结果
        results = []
        for opp in all_opportunities:
            key = (opp['ts_code'], opp['trade_date'])
            
            result = opp.copy()
            result['return_after_3d'] = future_3d.get(key, None)
            result['return_after_5d'] = future_5d.get(key, None)
            result['return_after_10d'] = future_10d.get(key, None)
            
            results.append(result)
        
        # 5. 计算统计指标
        stats = self.calculate_strategy_statistics(results)
        
        if self.logger:
            self.logger.info("[放量突破策略分析完成] 2024年至今放量突破横盘机会分析完成")
        
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
        returns_3d = [r['return_after_3d'] for r in results if r['return_after_3d'] is not None]
        returns_5d = [r['return_after_5d'] for r in results if r['return_after_5d'] is not None]
        returns_10d = [r['return_after_10d'] for r in results if r['return_after_10d'] is not None]
        
        # 提取选股条件数据
        price_ranges = [r['price_range_20d'] for r in results if r['price_range_20d'] is not None]
        volume_ratios = [r['volume_ratio'] for r in results if r['volume_ratio'] is not None]
        pct_chgs = [r['pct_chg'] for r in results if r['pct_chg'] is not None]
        
        stats = {
            'total_opportunities': len(results),
            'valid_3d_count': len(returns_3d),
            'valid_5d_count': len(returns_5d),
            'valid_10d_count': len(returns_10d)
        }
        
        # 选股条件统计
        if price_ranges:
            stats['price_range_stats'] = {
                'mean': sum(price_ranges) / len(price_ranges),
                'median': sorted(price_ranges)[len(price_ranges) // 2],
                'max': max(price_ranges),
                'min': min(price_ranges)
            }
        
        if volume_ratios:
            stats['volume_ratio_stats'] = {
                'mean': sum(volume_ratios) / len(volume_ratios),
                'median': sorted(volume_ratios)[len(volume_ratios) // 2],
                'max': max(volume_ratios),
                'min': min(volume_ratios)
            }
        
        if pct_chgs:
            stats['breakout_pct_stats'] = {
                'mean': sum(pct_chgs) / len(pct_chgs),
                'median': sorted(pct_chgs)[len(pct_chgs) // 2],
                'max': max(pct_chgs),
                'min': min(pct_chgs)
            }
        
        # 3日后表现统计
        if returns_3d:
            stats['3d_stats'] = {
                'mean_return': sum(returns_3d) / len(returns_3d),
                'median_return': sorted(returns_3d)[len(returns_3d) // 2],
                'max_return': max(returns_3d),
                'min_return': min(returns_3d),
                'positive_count': sum(1 for r in returns_3d if r > 0),
                'negative_count': sum(1 for r in returns_3d if r < 0),
                'win_rate': sum(1 for r in returns_3d if r > 0) / len(returns_3d) * 100
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


def export_breakout_results(results_data: Dict, output_dir: str = "/Users/nxm/PycharmProjects/dataDig/results"):
    """导出放量突破策略分析结果"""
    import os
    
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 导出详细数据
    if results_data['opportunities']:
        df = pd.DataFrame(results_data['opportunities'])
        detail_file = os.path.join(output_dir, f"放量突破横盘策略2024年批量分析_{timestamp}.csv")
        df.to_csv(detail_file, index=False, encoding='utf-8-sig')
        print(f"📁 详细数据已导出: {detail_file}")
    
    # 导出统计摘要
    summary_file = os.path.join(output_dir, f"放量突破横盘策略2024年统计摘要_{timestamp}.txt")
    with open(summary_file, 'w', encoding='utf-8') as f:
        stats = results_data['statistics']
        f.write("2024年至今放量突破横盘策略批量分析统计报告\n")
        f.write("="*50 + "\n")
        f.write(f"策略描述: 过去20日波动≤5% + 当日涨幅5%-9.5% + 当日放量≥3倍\n")
        f.write(f"分析时间范围: {results_data['date_range'][0]} 到 {results_data['date_range'][1]}\n")
        f.write(f"分析交易日数: {results_data['trading_days_analyzed']}天\n")
        f.write(f"找到投资机会: {results_data['total_opportunities']}个\n")
        f.write("\n")
        
        if 'price_range_stats' in stats:
            f.write("选股条件统计（过去20日价格波动）:\n")
            pr = stats['price_range_stats']
            f.write(f"  平均波动: {pr['mean']:.2f}%\n")
            f.write(f"  中位数波动: {pr['median']:.2f}%\n")
            f.write(f"  最大波动: {pr['max']:.2f}%\n")
            f.write(f"  最小波动: {pr['min']:.2f}%\n")
            f.write("\n")
        
        if 'volume_ratio_stats' in stats:
            f.write("选股条件统计（放量倍数）:\n")
            vr = stats['volume_ratio_stats']
            f.write(f"  平均放量倍数: {vr['mean']:.2f}倍\n")
            f.write(f"  中位数放量倍数: {vr['median']:.2f}倍\n")
            f.write(f"  最大放量倍数: {vr['max']:.2f}倍\n")
            f.write(f"  最小放量倍数: {vr['min']:.2f}倍\n")
            f.write("\n")
        
        if 'breakout_pct_stats' in stats:
            f.write("选股条件统计（突破日涨幅）:\n")
            bp = stats['breakout_pct_stats']
            f.write(f"  平均涨幅: {bp['mean']:.2f}%\n")
            f.write(f"  中位数涨幅: {bp['median']:.2f}%\n")
            f.write(f"  最大涨幅: {bp['max']:.2f}%\n")
            f.write(f"  最小涨幅: {bp['min']:.2f}%\n")
            f.write("\n")
        
        if '3d_stats' in stats:
            f.write("3日后表现统计:\n")
            s3 = stats['3d_stats']
            f.write(f"  有效数据: {stats['valid_3d_count']}个\n")
            f.write(f"  平均收益率: {s3['mean_return']:.2f}%\n")
            f.write(f"  中位数收益率: {s3['median_return']:.2f}%\n")
            f.write(f"  最大收益率: {s3['max_return']:.2f}%\n")
            f.write(f"  最小收益率: {s3['min_return']:.2f}%\n")
            f.write(f"  胜率: {s3['win_rate']:.2f}%\n")
            f.write(f"  上涨次数: {s3['positive_count']}次\n")
            f.write(f"  下跌次数: {s3['negative_count']}次\n")
            f.write("\n")
        
        if '5d_stats' in stats:
            f.write("5日后表现统计:\n")
            s5 = stats['5d_stats']
            f.write(f"  有效数据: {stats['valid_5d_count']}个\n")
            f.write(f"  平均收益率: {s5['mean_return']:.2f}%\n")
            f.write(f"  中位数收益率: {s5['median_return']:.2f}%\n")
            f.write(f"  最大收益率: {s5['max_return']:.2f}%\n")
            f.write(f"  最小收益率: {s5['min_return']:.2f}%\n")
            f.write(f"  胜率: {s5['win_rate']:.2f}%\n")
            f.write(f"  上涨次数: {s5['positive_count']}次\n")
            f.write(f"  下跌次数: {s5['negative_count']}次\n")
            f.write("\n")
        
        if '10d_stats' in stats:
            f.write("10日后表现统计:\n")
            s10 = stats['10d_stats']
            f.write(f"  有效数据: {stats['valid_10d_count']}个\n")
            f.write(f"  平均收益率: {s10['mean_return']:.2f}%\n")
            f.write(f"  中位数收益率: {s10['median_return']:.2f}%\n")
            f.write(f"  最大收益率: {s10['max_return']:.2f}%\n")
            f.write(f"  最小收益率: {s10['min_return']:.2f}%\n")
            f.write(f"  胜率: {s10['win_rate']:.2f}%\n")
            f.write(f"  上涨次数: {s10['positive_count']}次\n")
            f.write(f"  下跌次数: {s10['negative_count']}次\n")
    
    print(f"📁 统计摘要已导出: {summary_file}")


def main():
    """主函数"""
    logger = get_logger(__name__)
    logger.info("[放量突破策略程序开始] 2024年放量突破横盘策略分析脚本启动")
    
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
    print("              2024年放量突破横盘策略分析")
    print("="*60)
    print("\n📊 策略逻辑:")
    print("  1. 过去20个交易日最高价比最低价涨幅不超过5%（横盘整理）")
    print("  2. 当天涨幅超过5%但小于9.5%（突破但非涨停）")
    print("  3. 当天成交量是过去20个交易日平均成交量的3倍以上（放量突破）")
    print("  4. 分析3日后、5日后、10日后的表现")
    print("\n💡 策略理念:")
    print("  - 长期横盘整理后的放量突破往往意味着趋势改变")
    print("  - 适度涨幅（5%-9.5%）避免追高风险")
    print("  - 放量确认突破的有效性，避免假突破")
    print("  - 寻找低位突破的潜力股")
    print("\n⚡ 数据来源: 直接从数据库查询，无需调用API")
    
    try:
        with mysql_client.get_session() as session:
            analyzer = VolumeBreakoutAnalyzer(session, logger)
            
            # 执行批量分析
            print("\n🔍 开始批量分析...")
            results = analyzer.analyze_volume_breakout_opportunities()
            
            if not results:
                print("\n❌ 分析失败或未找到符合条件的机会")
                return
            
            # 显示统计结果
            stats = results['statistics']
            
            print("\n" + "="*60)
            print("              统计结果汇总")
            print("="*60)
            
            print(f"\n📅 分析时间范围: {results['date_range'][0]} 到 {results['date_range'][1]}")
            print(f"📊 分析交易日数: {results['trading_days_analyzed']}天")
            print(f"🎯 找到投资机会: {results['total_opportunities']}个")
            
            if 'price_range_stats' in stats:
                print("\n📊 选股条件统计（过去20日价格波动）:")
                pr = stats['price_range_stats']
                print(f"  💰 平均波动: {pr['mean']:.2f}%")
                print(f"  📊 中位数波动: {pr['median']:.2f}%")
                print(f"  🔥 最大波动: {pr['max']:.2f}%")
                print(f"  ❄️  最小波动: {pr['min']:.2f}%")
            
            if 'volume_ratio_stats' in stats:
                print("\n📊 选股条件统计（放量倍数）:")
                vr = stats['volume_ratio_stats']
                print(f"  💰 平均放量倍数: {vr['mean']:.2f}倍")
                print(f"  📊 中位数放量倍数: {vr['median']:.2f}倍")
                print(f"  🔥 最大放量倍数: {vr['max']:.2f}倍")
                print(f"  ❄️  最小放量倍数: {vr['min']:.2f}倍")
            
            if 'breakout_pct_stats' in stats:
                print("\n📊 选股条件统计（突破日涨幅）:")
                bp = stats['breakout_pct_stats']
                print(f"  💰 平均涨幅: {bp['mean']:.2f}%")
                print(f"  📊 中位数涨幅: {bp['median']:.2f}%")
                print(f"  🔥 最大涨幅: {bp['max']:.2f}%")
                print(f"  ❄️  最小涨幅: {bp['min']:.2f}%")
            
            if '3d_stats' in stats:
                print("\n📈 3日后表现统计:")
                s3 = stats['3d_stats']
                print(f"  💰 平均收益率: {s3['mean_return']:+.2f}%")
                print(f"  📊 中位数收益率: {s3['median_return']:+.2f}%")
                print(f"  🔥 最大收益率: {s3['max_return']:+.2f}%")
                print(f"  ❄️  最小收益率: {s3['min_return']:+.2f}%")
                print(f"  🎯 胜率: {s3['win_rate']:.1f}%")
                print(f"  📈 上涨次数: {s3['positive_count']}次")
                print(f"  📉 下跌次数: {s3['negative_count']}次")
                print(f"  📋 有效数据: {stats['valid_3d_count']}个")
            
            if '5d_stats' in stats:
                print("\n📈 5日后表现统计:")
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
                print("\n📈 10日后表现统计:")
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
            print("\n🎯 策略效果评价:")
            
            if '3d_stats' in stats and '5d_stats' in stats and '10d_stats' in stats:
                avg_3d = stats['3d_stats']['mean_return']
                avg_5d = stats['5d_stats']['mean_return']
                avg_10d = stats['10d_stats']['mean_return']
                win_3d = stats['3d_stats']['win_rate']
                win_5d = stats['5d_stats']['win_rate']
                win_10d = stats['10d_stats']['win_rate']
                
                if avg_3d > 0 and avg_5d > 0 and avg_10d > 0:
                    print("✅ 策略整体有效: 短期、中短期和中期都有正收益")
                elif avg_3d > 0 and avg_5d > 0:
                    print("⚠️  策略短期有效: 3日和5日表现良好，适合短线操作")
                elif avg_10d > 0:
                    print("⚠️  策略中期有效: 10日表现良好，需要耐心持有")
                else:
                    print("❌ 策略效果不佳: 平均收益为负，需要优化条件")
                
                if win_3d > 60 or win_5d > 60 or win_10d > 60:
                    print("✅ 胜率表现优秀: 超过60%的机会获得正收益")
                elif win_3d > 50 or win_5d > 50 or win_10d > 50:
                    print("⚠️  胜率表现一般: 约半数机会获得正收益")
                else:
                    print("❌ 胜率偏低: 多数机会仍为负收益")
                
                print(f"\n📊 策略表现对比:")
                print(f"   3日 vs 5日 vs 10日平均收益: {avg_3d:+.2f}% vs {avg_5d:+.2f}% vs {avg_10d:+.2f}%")
                print(f"   3日 vs 5日 vs 10日胜率: {win_3d:.1f}% vs {win_5d:.1f}% vs {win_10d:.1f}%")
            
            # 导出结果
            print("\n📁 正在导出分析结果...")
            export_breakout_results(results)
            
            print("\n" + "="*60)
            print("              放量突破横盘策略分析完成")
            print("="*60)
            print("\n💡 使用建议:")
            print("  1. 关注胜率较高的时间维度进行操作")
            print("  2. 结合技术形态确认突破有效性")
            print("  3. 观察放量是否伴随利好消息")
            print("  4. 设置合理的止损位（建议-8%）")
            print("  5. 横盘时间越长突破后爆发力越强")
            print("  6. 注意大盘环境，牛市中效果更佳")
            
        logger.info("[放量突破策略程序完成] 2024年放量突破横盘策略分析已完成")
        
    except Exception as e:
        logger.error(f"[放量突破策略程序错误] 执行过程中发生错误: {str(e)}")
        print(f"\n❌ 执行过程中发生错误: {str(e)}")
        print("📋 请检查日志文件获取详细错误信息")
        import traceback
        print("\n详细错误信息:")
        traceback.print_exc()


if __name__ == "__main__":
    main()


