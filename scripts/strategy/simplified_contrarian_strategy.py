#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简化版逆向投资策略分析脚本

由于缺少每日指标数据(daily_basic)，本脚本只使用价格数据和指数数据进行分析

筛选条件：
1. 对应盘面涨幅超过2%
2. 个股下跌6%以上
3. 个股在最近20个交易日涨幅低于20%

分析这些被错杀的股票在未来5个和10个交易日的表现

用法示例:
python simplified_contrarian_strategy.py
"""

import sys
import os
from datetime import datetime, timedelta
from typing import List

# 添加项目根路径到Python路径
sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))

from src.config.settings import load_settings
from src.db.mysql_client import MySQLClient
from src.app_logging.logger import get_logger


class SimplifiedContrarianAnalyzer:
    """简化版逆向投资分析器"""
    
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
    
    def get_index_performance(self, index_code: str, trade_date: str):
        """获取指数在指定日期的涨跌幅"""
        from src.models.daily_price import IndexDaily
        from sqlalchemy import select, and_
        
        stmt = select(IndexDaily.pct_chg).where(
            and_(
                IndexDaily.ts_code == index_code,
                IndexDaily.trade_date == trade_date
            )
        )
        
        result = self.session.execute(stmt).scalar()
        return float(result) if result is not None else None
    
    def get_stock_historical_performance(self, ts_code: str, end_date: str, days: int = 20):
        """获取股票历史期间涨幅"""
        from src.models.daily_price import DailyPrice
        from sqlalchemy import select, and_
        
        stmt = select(DailyPrice.trade_date, DailyPrice.close).where(
            and_(
                DailyPrice.ts_code == ts_code,
                DailyPrice.trade_date <= end_date
            )
        ).order_by(DailyPrice.trade_date.desc()).limit(days + 1)
        
        result = self.session.execute(stmt).fetchall()
        
        if len(result) < days:
            return None
        
        latest_price = result[0][1]  # 最新收盘价
        past_price = result[days - 1][1]  # N天前收盘价
        
        if past_price and past_price > 0:
            performance = (latest_price - past_price) / past_price * 100
            return performance
        else:
            return None
    
    def get_stock_prices_by_date(self, trade_date: str):
        """获取指定日期的所有股票价格数据"""
        from src.models.daily_price import DailyPrice, StockBasic
        from sqlalchemy import select, and_
        
        stmt = select(
            DailyPrice.ts_code,
            StockBasic.name,
            DailyPrice.close,
            DailyPrice.pct_chg,
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
    
    def screen_contrarian_stocks(self, 
                                 screening_date: str,
                                 min_index_rise: float = 2.0,
                                 max_stock_fall: float = -6.0,
                                 max_historical_rise: float = 20.0,
                                 historical_days: int = 20):
        """筛选逆向投资机会"""
        
        if self.logger:
            self.logger.info(f"[简化逆向筛选] 开始筛选，日期={screening_date}")
        
        # 获取当日所有股票数据
        stock_data = self.get_stock_prices_by_date(screening_date)
        
        if not stock_data:
            if self.logger:
                self.logger.warning(f"[简化逆向筛选] {screening_date}无股票数据")
            return []
        
        if self.logger:
            self.logger.info(f"[简化逆向筛选] 获取到{len(stock_data)}只股票的数据")
        
        filtered_stocks = []
        
        for ts_code, name, close, pct_chg, vol in stock_data:
            # 1. 检查个股跌幅是否符合条件
            if pct_chg is None or pct_chg > max_stock_fall:
                continue
            
            # 2. 获取对应指数涨幅
            index_code = self.get_corresponding_index(ts_code)
            index_performance = self.get_index_performance(index_code, screening_date)
            
            if index_performance is None or index_performance < min_index_rise:
                continue
            
            # 3. 检查历史涨幅
            historical_performance = self.get_stock_historical_performance(
                ts_code, screening_date, historical_days
            )
            
            if historical_performance is None or historical_performance > max_historical_rise:
                continue
            
            # 符合所有条件
            board = self.get_stock_board(ts_code)
            stock_info = {
                'ts_code': ts_code,
                'name': name,
                'board': board,
                'pct_chg': pct_chg,
                'corresponding_index': index_code,
                'index_pct_chg': index_performance,
                'historical_performance': historical_performance,
                'close': close,
                'vol': vol
            }
            
            filtered_stocks.append(stock_info)
            
            if self.logger:
                self.logger.info(f"[符合条件] {ts_code} {name}：个股跌{pct_chg:.2f}%，"
                              f"对应指数({index_code})涨{index_performance:.2f}%，"
                              f"{historical_days}日涨幅{historical_performance:.2f}%")
        
        if self.logger:
            self.logger.info(f"[简化逆向筛选完成] 筛选出{len(filtered_stocks)}只符合条件的股票")
        
        return filtered_stocks
    
    def analyze_future_performance(self, filtered_stocks, screening_date: str, analysis_days: int = 5):
        """分析未来表现"""
        from src.models.daily_price import DailyPrice
        from sqlalchemy import select, and_
        
        if not filtered_stocks:
            return None
        
        if self.logger:
            self.logger.info(f"[表现分析] 分析{len(filtered_stocks)}只股票{analysis_days}天后的表现")
        
        # 获取目标日期
        target_date = self._get_next_trading_date(screening_date, analysis_days)
        if not target_date:
            if self.logger:
                self.logger.warning(f"[表现分析] 无法找到{screening_date}后第{analysis_days}个交易日")
            return None
        
        # 获取目标日期的价格
        stock_codes = [stock['ts_code'] for stock in filtered_stocks]
        stmt = select(DailyPrice.ts_code, DailyPrice.close).where(
            and_(
                DailyPrice.ts_code.in_(stock_codes),
                DailyPrice.trade_date == target_date
            )
        )
        
        target_prices = dict(self.session.execute(stmt).fetchall())
        
        # 计算表现
        performances = []
        for stock in filtered_stocks:
            ts_code = stock['ts_code']
            if ts_code in target_prices:
                screening_price = stock['close']
                target_price = target_prices[ts_code]
                return_pct = (target_price - screening_price) / screening_price * 100
                
                performance = {
                    'ts_code': ts_code,
                    'name': stock['name'],
                    'screening_price': screening_price,
                    'target_price': target_price,
                    'return_pct': return_pct
                }
                performances.append(performance)
        
        if not performances:
            return None
        
        # 计算统计指标
        returns = [p['return_pct'] for p in performances]
        
        result = {
            'screening_date': screening_date,
            'target_date': target_date,
            'analysis_days': analysis_days,
            'total_stocks': len(performances),
            'avg_return': sum(returns) / len(returns),
            'median_return': sorted(returns)[len(returns) // 2],
            'max_return': max(returns),
            'min_return': min(returns),
            'positive_count': sum(1 for r in returns if r > 0),
            'negative_count': sum(1 for r in returns if r < 0),
            'win_rate': sum(1 for r in returns if r > 0) / len(returns) * 100,
            'performances': sorted(performances, key=lambda x: x['return_pct'], reverse=True)
        }
        
        return result
    
    def _get_next_trading_date(self, start_date: str, days_offset: int):
        """获取指定日期后第N个交易日"""
        from src.models.daily_price import DailyPrice
        from sqlalchemy import select
        
        stmt = select(DailyPrice.trade_date.distinct()).where(
            DailyPrice.trade_date > start_date
        ).order_by(DailyPrice.trade_date).limit(days_offset)
        
        result = self.session.execute(stmt).scalars().all()
        
        if len(result) >= days_offset:
            return result[days_offset - 1]
        else:
            return None


def find_best_screening_date(mysql_client, logger):
    """寻找最适合进行逆向投资筛选的日期"""
    with mysql_client.get_session() as session:
        from src.models.daily_price import DailyPrice, IndexDaily
        from sqlalchemy import select, and_
        
        # 查询最近有数据的交易日期（确保有后续数据用于分析）
        stmt = select(DailyPrice.trade_date.distinct()).order_by(
            DailyPrice.trade_date.desc()
        ).limit(30)
        
        recent_dates = session.execute(stmt).scalars().all()
        
        if len(recent_dates) < 15:
            return None
        
        # 检查这些日期的指数数据，找到指数上涨的日期
        analyzer = SimplifiedContrarianAnalyzer(session, logger)
        
        suitable_dates = []
        
        for date in recent_dates[10:]:  # 跳过最近10天，确保有后续数据
            # 检查主要指数表现
            index_codes = ['000001.SH', '399001.SZ', '399006.SZ']
            index_rises = []
            
            for index_code in index_codes:
                performance = analyzer.get_index_performance(index_code, date)
                if performance is not None and performance >= 1.0:  # 至少涨1%
                    index_rises.append(performance)
            
            if len(index_rises) >= 1:  # 至少有一个指数上涨
                suitable_dates.append((date, max(index_rises)))
        
        # 按指数涨幅排序，选择最佳日期
        suitable_dates.sort(key=lambda x: x[1], reverse=True)
        
        if suitable_dates:
            best_date = suitable_dates[0][0]
            logger.info(f"[最佳日期选择] 选择{best_date}作为筛选日期，当日最大指数涨幅={suitable_dates[0][1]:.2f}%")
            return best_date
        else:
            # 如果没有找到指数上涨的日期，选择一个有数据的日期
            fallback_date = recent_dates[15]
            logger.info(f"[备选日期] 未找到指数大涨日期，选择{fallback_date}作为测试日期")
            return fallback_date


def main():
    """主函数"""
    logger = get_logger(__name__)
    logger.info("[简化逆向策略开始] 简化版逆向投资策略分析脚本启动")
    
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
    print("              简化版逆向投资策略分析")
    print("="*60)
    print("\\n📢 注意：由于缺少每日指标数据，本分析仅使用价格数据和指数数据")
    print("\\n策略理念: 在大盘上涨时寻找被错杀的个股")
    print("\\n筛选条件:")
    print("  1. 对应盘面(指数)涨幅 ≥ 2%")
    print("  2. 个股当日跌幅 ≥ 6%") 
    print("  3. 个股近20日涨幅 ≤ 20%")
    
    try:
        with mysql_client.get_session() as session:
            analyzer = SimplifiedContrarianAnalyzer(session, logger)
            
            # 寻找最佳筛选日期
            screening_date = find_best_screening_date(mysql_client, logger)
            if not screening_date:
                print("\\n❌ 无法找到合适的筛选日期")
                return
            
            print(f"\\n📅 选择筛选日期: {screening_date}")
            
            # 执行筛选
            filtered_stocks = analyzer.screen_contrarian_stocks(
                screening_date=screening_date,
                min_index_rise=2.0,
                max_stock_fall=-6.0,
                max_historical_rise=20.0
            )
            
            if not filtered_stocks:
                print("\\n❌ 未找到符合逆向投资条件的股票")
                print("\\n尝试放宽条件...")
                
                # 放宽条件重试
                filtered_stocks = analyzer.screen_contrarian_stocks(
                    screening_date=screening_date,
                    min_index_rise=1.0,      # 放宽到1%
                    max_stock_fall=-3.0,     # 放宽到-3%
                    max_historical_rise=30.0  # 放宽到30%
                )
                
                if filtered_stocks:
                    print(f"\\n✅ 使用放宽条件找到 {len(filtered_stocks)} 只股票")
                else:
                    print("\\n❌ 即使放宽条件也未找到符合的股票")
                    return
            else:
                print(f"\\n✅ 筛选结果: 找到 {len(filtered_stocks)} 只符合条件的股票")
            
            # 显示筛选结果
            print("\\n========== 筛选股票详情 ==========")
            print(f"{'股票代码':<10} {'股票名称':<12} {'板块':<4} {'个股跌幅%':<8} {'对应指数':<12} {'指数涨幅%':<8} {'20日涨幅%':<8}")
            print("-" * 80)
            
            for stock in filtered_stocks:
                print(f"{stock['ts_code']:<10} {stock['name'][:10]:<12} {stock['board']:<4} "
                      f"{stock['pct_chg']:>7.2f} {stock['corresponding_index']:<12} "
                      f"{stock['index_pct_chg']:>7.2f} {stock['historical_performance']:>7.2f}")
            
            # 分析5天后表现
            print("\\n========== 5个交易日后表现分析 ==========")
            result_5d = analyzer.analyze_future_performance(filtered_stocks, screening_date, 5)
            
            if result_5d:
                print(f"📊 分析期间: {result_5d['screening_date']} -> {result_5d['target_date']}")
                print(f"📈 分析股票数: {result_5d['total_stocks']}只")
                print(f"💰 平均收益率: {result_5d['avg_return']:.2f}%")
                print(f"📊 中位数收益率: {result_5d['median_return']:.2f}%")
                print(f"🎯 胜率: {result_5d['win_rate']:.2f}%")
                print(f"🔥 最大收益率: {result_5d['max_return']:.2f}%")
                print(f"❄️ 最小收益率: {result_5d['min_return']:.2f}%")
                print(f"📈 上涨股票: {result_5d['positive_count']}只")
                print(f"📉 下跌股票: {result_5d['negative_count']}只")
                
                # 显示表现最好的股票
                if result_5d['performances']:
                    print("\\n🏆 表现最好的5只股票:")
                    for i, perf in enumerate(result_5d['performances'][:5]):
                        print(f"  {i+1}. {perf['name']}({perf['ts_code']}): {perf['return_pct']:+.2f}%")
            else:
                print("❌ 无法分析5天后表现：缺少后续价格数据")
            
            # 分析10天后表现
            print("\\n========== 10个交易日后表现分析 ==========")
            result_10d = analyzer.analyze_future_performance(filtered_stocks, screening_date, 10)
            
            if result_10d:
                print(f"📊 分析期间: {result_10d['screening_date']} -> {result_10d['target_date']}")
                print(f"📈 分析股票数: {result_10d['total_stocks']}只")
                print(f"💰 平均收益率: {result_10d['avg_return']:.2f}%")
                print(f"🎯 胜率: {result_10d['win_rate']:.2f}%")
                print(f"🔥 最大收益率: {result_10d['max_return']:.2f}%")
                print(f"❄️ 最小收益率: {result_10d['min_return']:.2f}%")
            else:
                print("❌ 无法分析10天后表现：缺少后续价格数据")
            
            # 策略评价
            if result_5d and result_10d:
                print("\\n========== 策略效果评价 ==========")
                print(f"📊 短期vs中期表现对比:")
                print(f"   5日平均收益率: {result_5d['avg_return']:+.2f}%  |  10日平均收益率: {result_10d['avg_return']:+.2f}%")
                print(f"   5日胜率: {result_5d['win_rate']:.1f}%           |  10日胜率: {result_10d['win_rate']:.1f}%")
                
                if result_5d['avg_return'] > 0 and result_10d['avg_return'] > 0:
                    print("\\n✅ 策略效果良好：短期和中期都获得正收益")
                elif result_5d['avg_return'] > 0:
                    print("\\n⚠️ 策略短期有效：适合短线操作")
                elif result_10d['avg_return'] > 0:
                    print("\\n⚠️ 策略中期有效：需要耐心持有")
                else:
                    print("\\n❌ 策略效果不佳：可能需要调整条件或等待更好时机")
        
        print("\\n" + "="*60)
        print("              简化版逆向投资策略分析完成")
        print("="*60)
        print("\\n💡 建议:")
        print("  1. 导入每日指标数据(daily_basic)以获得更精确的筛选")
        print("  2. 结合个股基本面分析做最终投资决策")
        print("  3. 设置合理的止损和仓位控制")
        
        logger.info("[简化逆向策略完成] 简化版逆向投资策略分析已完成")
        
    except Exception as e:
        logger.error(f"[简化逆向策略错误] 执行过程中发生错误: {str(e)}")
        print(f"\\n❌ 执行过程中发生错误: {str(e)}")
        print("📋 请检查日志文件获取详细错误信息")
        import traceback
        print("\\n详细错误信息:")
        traceback.print_exc()


if __name__ == "__main__":
    main()
