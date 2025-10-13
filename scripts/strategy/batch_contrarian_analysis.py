#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
批量逆向投资策略历史统计分析

分析2024年至今所有符合逆向投资条件的情况，
统计5日后、10日后的表现，给出最值和均值

注意：直接从数据库查询数据，不调用API接口

用法示例:
python batch_contrarian_analysis.py
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


class BatchContrarianAnalyzer:
    """批量逆向投资分析器"""
    
    # 主要指数代码映射
    INDEX_MAPPING = {
        'sh': '000001.SH',    # 上证指数
        'szzs': '399001.SZ',  # 深证成指
        'cyb': '399006.SZ',   # 创业板指
        'kc50': '000688.SH'   # 科创50
    }
    
    # 股票所属板块判断规则
    BOARD_RULES = {
        'sh': lambda code: code.endswith('.SH') and (code.startswith('600') or code.startswith('601') or code.startswith('603')),
        'sz': lambda code: code.endswith('.SZ') and code.startswith('000'),
        'sme': lambda code: code.endswith('.SZ') and code.startswith('002'),  # 中小板
        'cyb': lambda code: code.endswith('.SZ') and code.startswith('300'),  # 创业板
        'kc': lambda code: code.endswith('.SH') and code.startswith('688')   # 科创板
    }
    
    def __init__(self, session, logger):
        self.session = session
        self.logger = logger
        
        if self.logger:
            self.logger.info("[批量分析器初始化] 批量逆向投资分析器已初始化")
    
    def get_stock_board(self, ts_code: str) -> str:
        """判断股票所属板块"""
        for board, rule in self.BOARD_RULES.items():
            if rule(ts_code):
                return board
        return 'unknown'
    
    def get_corresponding_index(self, ts_code: str) -> str:
        """获取股票对应的主要指数代码"""
        board = self.get_stock_board(ts_code)
        
        if board == 'sh':
            return self.INDEX_MAPPING['sh']     # 上证指数
        elif board == 'sz':
            return self.INDEX_MAPPING['szzs']   # 深证成指 
        elif board == 'cyb':
            return self.INDEX_MAPPING['cyb']    # 创业板指
        elif board == 'kc':
            return self.INDEX_MAPPING['kc50']   # 科创50
        elif board == 'sme':
            return self.INDEX_MAPPING['szzs']   # 中小板用深证成指
        else:
            return self.INDEX_MAPPING['sh']     # 默认用上证指数
    
    def get_trading_dates_2024_to_now(self) -> List[str]:
        """获取2024年至今的所有交易日期（从数据库）"""
        from src.models.daily_price import DailyPrice
        from sqlalchemy import select, and_
        
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
    
    def get_daily_stock_data(self, trade_date: str) -> pd.DataFrame:
        """获取指定日期的所有股票数据（从数据库）"""
        from src.models.daily_price import DailyPrice, StockBasic, DailyBasic
        from sqlalchemy import select, and_
        
        # 联合查询获取股票价格和基本面数据
        stmt = select(
            DailyPrice.ts_code,
            StockBasic.name,
            DailyPrice.close,
            DailyPrice.pct_chg,
            DailyPrice.vol,
            DailyBasic.pe,
            DailyBasic.pb,
            DailyBasic.total_mv
        ).select_from(
            DailyPrice.__table__.join(
                StockBasic.__table__, DailyPrice.ts_code == StockBasic.ts_code
            ).outerjoin(
                DailyBasic.__table__, 
                and_(
                    DailyPrice.ts_code == DailyBasic.ts_code,
                    DailyPrice.trade_date == DailyBasic.trade_date
                )
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
        
        if not result:
            return pd.DataFrame()
        
        # 转换为DataFrame
        df = pd.DataFrame(result, columns=[
            'ts_code', 'name', 'close', 'pct_chg', 'vol', 'pe', 'pb', 'total_mv'
        ])
        
        return df
    
    def get_index_performance_batch(self, trade_dates: List[str]) -> Dict[str, Dict[str, float]]:
        """批量获取指数表现数据（从数据库）"""
        from src.models.daily_price import IndexDaily
        from sqlalchemy import select, and_
        
        if self.logger:
            self.logger.info(f"[批量获取指数数据] 开始获取{len(trade_dates)}个交易日的指数数据")
        
        index_codes = list(self.INDEX_MAPPING.values())
        
        stmt = select(
            IndexDaily.trade_date,
            IndexDaily.ts_code,
            IndexDaily.pct_chg
        ).where(
            and_(
                IndexDaily.trade_date.in_(trade_dates),
                IndexDaily.ts_code.in_(index_codes)
            )
        )
        
        result = self.session.execute(stmt).fetchall()
        
        # 组织数据结构：{trade_date: {index_code: pct_chg}}
        index_data = {}
        for trade_date, index_code, pct_chg in result:
            if trade_date not in index_data:
                index_data[trade_date] = {}
            index_data[trade_date][index_code] = float(pct_chg) if pct_chg is not None else 0.0
        
        if self.logger:
            self.logger.info(f"[批量获取指数数据] 获取完成，覆盖{len(index_data)}个交易日")
        
        return index_data
    
    def get_stock_historical_performance_batch(self, stocks_dates: List[tuple], days: int = 20) -> Dict[tuple, float]:
        """批量获取股票历史涨幅（从数据库）"""
        from src.models.daily_price import DailyPrice
        from sqlalchemy import select, and_
        
        if self.logger:
            self.logger.info(f"[批量获取历史涨幅] 开始计算{len(stocks_dates)}个股票-日期组合的{days}日历史涨幅")
        
        historical_performance = {}
        
        # 按股票分组处理，减少数据库查询次数
        stocks_by_code = {}
        for ts_code, trade_date in stocks_dates:
            if ts_code not in stocks_by_code:
                stocks_by_code[ts_code] = []
            stocks_by_code[ts_code].append(trade_date)
        
        for ts_code, dates_list in stocks_by_code.items():
            # 获取这只股票的所有价格数据
            max_date = max(dates_list)
            min_date = min(dates_list)
            # 获取足够的历史数据
            stmt = select(
                DailyPrice.trade_date,
                DailyPrice.close
            ).where(
                and_(
                    DailyPrice.ts_code == ts_code,
                    DailyPrice.trade_date <= max_date,
                    DailyPrice.trade_date >= '20240101'  # 从2024年开始
                )
            ).order_by(DailyPrice.trade_date)
            
            price_data = self.session.execute(stmt).fetchall()
            
            if len(price_data) < days:
                continue
            
            # 转换为字典便于查找
            price_dict = {date: price for date, price in price_data}
            price_dates = [date for date, _ in price_data]
            
            # 为每个日期计算历史涨幅
            for target_date in dates_list:
                try:
                    # 找到目标日期在价格数据中的位置
                    if target_date not in price_dates:
                        continue
                    
                    target_idx = price_dates.index(target_date)
                    
                    if target_idx >= days:
                        current_price = price_dict[target_date]
                        past_date = price_dates[target_idx - days]
                        past_price = price_dict[past_date]
                        
                        if past_price and past_price > 0:
                            performance = (current_price - past_price) / past_price * 100
                            historical_performance[(ts_code, target_date)] = performance
                
                except (ValueError, IndexError, KeyError):
                    continue
        
        if self.logger:
            self.logger.info(f"[批量获取历史涨幅] 计算完成，成功计算{len(historical_performance)}个历史涨幅")
        
        return historical_performance
    
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
    
    def analyze_all_contrarian_opportunities(self, 
                                           min_index_rise: float = 2.0,
                                           max_stock_fall: float = -6.0,
                                           max_historical_rise: float = 20.0,
                                           historical_days: int = 20):
        """分析2024年至今所有的逆向投资机会"""
        
        if self.logger:
            self.logger.info("[开始批量分析] 开始分析2024年至今所有逆向投资机会")
        
        # 1. 获取所有交易日期
        trade_dates = self.get_trading_dates_2024_to_now()
        
        if not trade_dates:
            if self.logger:
                self.logger.error("[批量分析] 未找到交易日期数据")
            return None
        
        print(f"\\n📅 分析时间范围: {trade_dates[0]} 到 {trade_dates[-1]}")
        print(f"📊 总交易日数: {len(trade_dates)}天")
        
        # 2. 批量获取指数数据
        index_data = self.get_index_performance_batch(trade_dates)
        
        # 3. 逐日筛选符合条件的股票
        all_opportunities = []
        processed_days = 0
        
        for trade_date in trade_dates:
            processed_days += 1
            
            if processed_days % 50 == 0 or processed_days == len(trade_dates):
                if self.logger:
                    self.logger.info(f"[筛选进度] 已处理{processed_days}/{len(trade_dates)}个交易日")
            
            # 获取当日股票数据
            stock_data = self.get_daily_stock_data(trade_date)
            
            if stock_data.empty:
                continue
            
            # 获取当日指数数据
            if trade_date not in index_data:
                continue
            
            daily_index_data = index_data[trade_date]
            
            # 筛选符合条件的股票
            for _, row in stock_data.iterrows():
                ts_code = row['ts_code']
                name = row['name']
                pct_chg = row['pct_chg']
                
                # 1. 检查个股跌幅
                if pct_chg is None or pct_chg > max_stock_fall:
                    continue
                
                # 2. 检查对应指数涨幅
                index_code = self.get_corresponding_index(ts_code)
                if index_code not in daily_index_data:
                    continue
                
                index_pct_chg = daily_index_data[index_code]
                if index_pct_chg < min_index_rise:
                    continue
                
                # 暂时记录，稍后批量计算历史涨幅
                opportunity = {
                    'trade_date': trade_date,
                    'ts_code': ts_code,
                    'name': name,
                    'board': self.get_stock_board(ts_code),
                    'pct_chg': pct_chg,
                    'index_code': index_code,
                    'index_pct_chg': index_pct_chg,
                    'close': row['close']
                }
                all_opportunities.append(opportunity)
        
        if not all_opportunities:
            print("\\n❌ 未找到符合条件的逆向投资机会")
            return None
        
        print(f"\\n🎯 初步筛选: 找到 {len(all_opportunities)} 个潜在机会")
        
        # 4. 批量计算历史涨幅
        stocks_dates_for_history = [(opp['ts_code'], opp['trade_date']) for opp in all_opportunities]
        historical_performance = self.get_stock_historical_performance_batch(stocks_dates_for_history, historical_days)
        
        # 5. 根据历史涨幅筛选
        final_opportunities = []
        for opp in all_opportunities:
            key = (opp['ts_code'], opp['trade_date'])
            if key in historical_performance:
                hist_perf = historical_performance[key]
                if hist_perf <= max_historical_rise:
                    opp['historical_performance'] = hist_perf
                    final_opportunities.append(opp)
        
        if not final_opportunities:
            print("\\n❌ 考虑历史涨幅后，未找到符合条件的机会")
            return None
        
        print(f"\\n✅ 最终筛选: 找到 {len(final_opportunities)} 个符合全部条件的逆向投资机会")
        
        # 6. 批量计算未来表现
        print("\\n📈 正在计算5日后和10日后表现...")
        
        stocks_dates_for_future = [(opp['ts_code'], opp['trade_date']) for opp in final_opportunities]
        
        # 5日后表现
        future_5d = self.get_future_performance_batch(stocks_dates_for_future, 5)
        
        # 10日后表现
        future_10d = self.get_future_performance_batch(stocks_dates_for_future, 10)
        
        # 7. 整合结果
        results = []
        for opp in final_opportunities:
            key = (opp['ts_code'], opp['trade_date'])
            
            result = opp.copy()
            result['return_5d'] = future_5d.get(key, None)
            result['return_10d'] = future_10d.get(key, None)
            
            results.append(result)
        
        # 8. 计算统计指标
        stats = self.calculate_strategy_statistics(results)
        
        if self.logger:
            self.logger.info("[批量分析完成] 2024年至今逆向投资机会分析完成")
        
        return {
            'opportunities': results,
            'statistics': stats,
            'total_opportunities': len(results),
            'date_range': (trade_dates[0], trade_dates[-1]),
            'trading_days_analyzed': len(trade_dates)
        }
    
    def calculate_strategy_statistics(self, results: List[Dict]) -> Dict[str, Any]:
        """计算策略统计指标"""
        
        if not results:
            return {}
        
        # 提取有效的收益率数据
        returns_5d = [r['return_5d'] for r in results if r['return_5d'] is not None]
        returns_10d = [r['return_10d'] for r in results if r['return_10d'] is not None]
        
        stats = {
            'total_opportunities': len(results),
            'valid_5d_count': len(returns_5d),
            'valid_10d_count': len(returns_10d)
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


def export_batch_results(results_data: Dict, output_dir: str = "/Users/nxm/PycharmProjects/dataDig/results"):
    """导出批量分析结果"""
    import os
    
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 导出详细数据
    if results_data['opportunities']:
        df = pd.DataFrame(results_data['opportunities'])
        detail_file = os.path.join(output_dir, f"逆向投资策略2024年批量分析_{timestamp}.csv")
        df.to_csv(detail_file, index=False, encoding='utf-8-sig')
        print(f"📁 详细数据已导出: {detail_file}")
    
    # 导出统计摘要
    summary_file = os.path.join(output_dir, f"逆向投资策略2024年统计摘要_{timestamp}.txt")
    with open(summary_file, 'w', encoding='utf-8') as f:
        stats = results_data['statistics']
        f.write("2024年至今逆向投资策略批量分析统计报告\\n")
        f.write("="*50 + "\\n")
        f.write(f"分析时间范围: {results_data['date_range'][0]} 到 {results_data['date_range'][1]}\\n")
        f.write(f"分析交易日数: {results_data['trading_days_analyzed']}天\\n")
        f.write(f"找到投资机会: {results_data['total_opportunities']}个\\n")
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
    logger.info("[批量分析程序开始] 2024年逆向投资策略批量分析脚本启动")
    
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
    print("              2024年逆向投资策略批量分析")
    print("="*60)
    print("\\n📊 分析目标:")
    print("  - 统计2024年至今所有符合逆向投资条件的情况")
    print("  - 分析5日后、10日后的表现")
    print("  - 计算最值、均值等统计指标")
    print("\\n⚡ 数据来源: 直接从数据库查询，无需调用API")
    
    try:
        with mysql_client.get_session() as session:
            analyzer = BatchContrarianAnalyzer(session, logger)
            
            # 执行批量分析
            print("\\n🔍 开始批量分析...")
            results = analyzer.analyze_all_contrarian_opportunities(
                min_index_rise=2.0,      # 指数涨幅≥2%
                max_stock_fall=-6.0,     # 个股跌幅≥6%
                max_historical_rise=20.0, # 近20日涨幅≤20%
                historical_days=20
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
                print(f"  ❄️  최소 수익률: {s10['min_return']:+.2f}%")
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
            export_batch_results(results)
            
            print("\\n" + "="*60)
            print("              批量分析完成")
            print("="*60)
            print("\\n💡 使用建议:")
            print("  1. 重点关注胜率较高的时间维度")
            print("  2. 结合具体股票基本面分析")
            print("  3. 设置合理的止损和仓位控制")
            print("  4. 在市场波动加大时重点关注此策略")
            
        logger.info("[批量分析程序完成] 2024年逆向投资策略批量分析已完成")
        
    except Exception as e:
        logger.error(f"[批量分析程序错误] 执行过程中发生错误: {str(e)}")
        print(f"\\n❌ 执行过程中发生错误: {str(e)}")
        print("📋 请检查日志文件获取详细错误信息")
        import traceback
        print("\\n详细错误信息:")
        traceback.print_exc()


if __name__ == "__main__":
    main()
