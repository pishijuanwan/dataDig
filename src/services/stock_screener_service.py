from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable
import pandas as pd
import numpy as np
from sqlalchemy.orm import Session
from sqlalchemy import select, and_, or_, func

from src.models.daily_price import StockBasic, DailyPrice, DailyBasic, IndexDaily, IndexBasic
from src.app_logging.logger import get_logger


class ScreeningCondition:
    """筛选条件基类"""
    
    def __init__(self, name: str, description: str = ""):
        self.name = name
        self.description = description
    
    def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        """应用筛选条件，返回符合条件的数据"""
        raise NotImplementedError("子类必须实现apply方法")


class TechnicalCondition(ScreeningCondition):
    """技术指标筛选条件"""
    
    def __init__(self, name: str, condition_func: Callable[[pd.DataFrame], pd.DataFrame], description: str = ""):
        super().__init__(name, description)
        self.condition_func = condition_func
    
    def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        """应用技术指标筛选条件"""
        return self.condition_func(data)


class FundamentalCondition(ScreeningCondition):
    """基本面筛选条件"""
    
    def __init__(self, field: str, operator: str, value: float, name: str = None, description: str = ""):
        if name is None:
            name = f"{field} {operator} {value}"
        super().__init__(name, description)
        self.field = field
        self.operator = operator
        self.value = value
    
    def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        """应用基本面筛选条件"""
        if self.field not in data.columns:
            return pd.DataFrame()
        
        if self.operator == '>':
            return data[data[self.field] > self.value]
        elif self.operator == '<':
            return data[data[self.field] < self.value]
        elif self.operator == '>=':
            return data[data[self.field] >= self.value]
        elif self.operator == '<=':
            return data[data[self.field] <= self.value]
        elif self.operator == '==':
            return data[data[self.field] == self.value]
        elif self.operator == '!=':
            return data[data[self.field] != self.value]
        else:
            raise ValueError(f"不支持的操作符: {self.operator}")


class PerformanceAnalysisResult:
    """表现分析结果"""
    
    def __init__(self):
        self.screening_date: str = ""  # 筛选日期
        self.screening_condition: str = ""  # 筛选条件描述
        self.total_screened: int = 0  # 筛选出的股票总数
        self.analysis_days: int = 0  # 分析天数
        
        # 表现统计
        self.avg_return: float = 0.0  # 平均收益率
        self.median_return: float = 0.0  # 中位数收益率
        self.win_rate: float = 0.0  # 胜率（正收益率占比）
        self.max_return: float = 0.0  # 最大收益率
        self.min_return: float = 0.0  # 最小收益率
        self.positive_count: int = 0  # 正收益股票数
        self.negative_count: int = 0  # 负收益股票数
        
        # 详细数据
        self.stock_performances: List[Dict] = []  # 各股票表现详情
        
    def to_dict(self) -> Dict:
        """转换为字典"""
        return {
            'screening_date': self.screening_date,
            'screening_condition': self.screening_condition,
            'total_screened': self.total_screened,
            'analysis_days': self.analysis_days,
            'avg_return': self.avg_return,
            'median_return': self.median_return,
            'win_rate': self.win_rate,
            'max_return': self.max_return,
            'min_return': self.min_return,
            'positive_count': self.positive_count,
            'negative_count': self.negative_count,
            'stock_performances': self.stock_performances
        }


class StockScreenerService:
    """股票筛选和表现分析服务"""
    
    # 主要指数代码映射
    INDEX_MAPPING = {
        'sz': '000001.SH',    # 上证指数
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
    
    def __init__(self, session: Session, logger=None):
        self.session = session
        self.logger = logger or get_logger(__name__)
        
        if self.logger:
            self.logger.info("[筛选服务初始化] 股票筛选和表现分析服务已初始化")
    
    def screen_stocks(
        self,
        screening_date: str,
        conditions: List[ScreeningCondition],
        market_filter: str = None
    ) -> pd.DataFrame:
        """
        根据条件筛选股票
        
        Args:
            screening_date: 筛选日期 YYYYMMDD
            conditions: 筛选条件列表
            market_filter: 市场过滤器（'main'=主板，'sme'=中小板，'gem'=创业板）
            
        Returns:
            筛选结果DataFrame
        """
        if self.logger:
            self.logger.info(f"[开始筛选] 筛选日期={screening_date}，条件数量={len(conditions)}")
        
        # 1. 获取基础数据
        base_data = self._get_screening_base_data(screening_date, market_filter)
        if base_data.empty:
            if self.logger:
                self.logger.warning(f"[筛选数据] 筛选日期={screening_date} 未找到基础数据")
            return pd.DataFrame()
        
        if self.logger:
            self.logger.info(f"[基础数据] 加载了{len(base_data)}只股票的基础数据")
        
        # 2. 逐步应用筛选条件
        filtered_data = base_data.copy()
        
        for i, condition in enumerate(conditions):
            before_count = len(filtered_data)
            filtered_data = condition.apply(filtered_data)
            after_count = len(filtered_data)
            
            if self.logger:
                self.logger.info(f"[应用条件{i+1}] {condition.name}：{before_count} -> {after_count} 只股票")
            
            if filtered_data.empty:
                if self.logger:
                    self.logger.warning(f"[筛选结果] 应用条件'{condition.name}'后无股票符合条件")
                break
        
        if self.logger:
            self.logger.info(f"[筛选完成] 最终筛选出{len(filtered_data)}只股票")
        
        return filtered_data
    
    def analyze_performance(
        self,
        screened_stocks: pd.DataFrame,
        screening_date: str,
        analysis_days: int = 5,
        condition_description: str = ""
    ) -> PerformanceAnalysisResult:
        """
        分析筛选出的股票后续表现
        
        Args:
            screened_stocks: 筛选出的股票数据
            screening_date: 筛选日期
            analysis_days: 分析天数（几天后的表现）
            condition_description: 筛选条件描述
            
        Returns:
            表现分析结果
        """
        if self.logger:
            self.logger.info(f"[开始表现分析] 股票数量={len(screened_stocks)}，分析天数={analysis_days}")
        
        result = PerformanceAnalysisResult()
        result.screening_date = screening_date
        result.screening_condition = condition_description
        result.total_screened = len(screened_stocks)
        result.analysis_days = analysis_days
        
        if screened_stocks.empty:
            return result
        
        # 获取筛选股票列表
        stock_codes = screened_stocks['ts_code'].tolist()
        
        # 计算目标日期
        target_date = self._get_next_trading_date(screening_date, analysis_days)
        if not target_date:
            if self.logger:
                self.logger.warning(f"[表现分析] 无法找到{screening_date}后第{analysis_days}个交易日")
            return result
        
        if self.logger:
            self.logger.info(f"[表现分析] 分析目标日期={target_date}")
        
        # 获取筛选日和目标日的价格数据
        screening_prices = self._get_prices_by_date(stock_codes, screening_date)
        target_prices = self._get_prices_by_date(stock_codes, target_date)
        
        if screening_prices.empty or target_prices.empty:
            if self.logger:
                self.logger.warning("[表现分析] 缺少必要的价格数据")
            return result
        
        # 合并价格数据计算收益率
        price_data = pd.merge(
            screening_prices[['ts_code', 'close']].rename(columns={'close': 'screening_close'}),
            target_prices[['ts_code', 'close']].rename(columns={'close': 'target_close'}),
            on='ts_code',
            how='inner'
        )
        
        if price_data.empty:
            if self.logger:
                self.logger.warning("[表现分析] 价格数据匹配失败")
            return result
        
        # 计算收益率
        price_data['return_pct'] = (price_data['target_close'] - price_data['screening_close']) / price_data['screening_close'] * 100
        
        # 合并股票基本信息
        stock_info = screened_stocks[['ts_code', 'name']].copy() if 'name' in screened_stocks.columns else pd.DataFrame()
        if not stock_info.empty:
            performance_data = pd.merge(price_data, stock_info, on='ts_code', how='left')
        else:
            performance_data = price_data.copy()
            performance_data['name'] = '未知'
        
        # 计算统计指标
        returns = performance_data['return_pct'].dropna()
        if len(returns) > 0:
            result.avg_return = float(returns.mean())
            result.median_return = float(returns.median())
            result.max_return = float(returns.max())
            result.min_return = float(returns.min())
            result.positive_count = int((returns > 0).sum())
            result.negative_count = int((returns < 0).sum())
            result.win_rate = result.positive_count / len(returns) * 100 if len(returns) > 0 else 0
        
        # 详细股票表现
        result.stock_performances = []
        for _, row in performance_data.iterrows():
            result.stock_performances.append({
                'ts_code': row['ts_code'],
                'name': row.get('name', '未知'),
                'screening_price': float(row['screening_close']),
                'target_price': float(row['target_close']),
                'return_pct': float(row['return_pct']) if pd.notna(row['return_pct']) else 0.0
            })
        
        # 按收益率排序
        result.stock_performances.sort(key=lambda x: x['return_pct'], reverse=True)
        
        if self.logger:
            self.logger.info(f"[表现分析完成] 平均收益率={result.avg_return:.2f}%，胜率={result.win_rate:.2f}%")
        
        return result
    
    def _get_screening_base_data(self, screening_date: str, market_filter: str = None) -> pd.DataFrame:
        """获取筛选基础数据（包含价格、基本面数据）"""
        if self.logger:
            self.logger.info(f"[加载基础数据] 开始加载筛选日期={screening_date}的基础数据")
        
        # 构建查询：连接股票基本信息、日线价格、每日基本面数据
        stmt = select(
            StockBasic.ts_code,
            StockBasic.name,
            StockBasic.industry,
            StockBasic.area,
            StockBasic.market,
            DailyPrice.trade_date,
            DailyPrice.open,
            DailyPrice.high,
            DailyPrice.low,
            DailyPrice.close,
            DailyPrice.pre_close,
            DailyPrice.pct_chg,
            DailyPrice.vol,
            DailyPrice.amount,
            DailyBasic.pe,
            DailyBasic.pe_ttm,
            DailyBasic.pb,
            DailyBasic.ps,
            DailyBasic.ps_ttm,
            DailyBasic.total_mv,
            DailyBasic.circ_mv,
            DailyBasic.turnover_rate,
            DailyBasic.volume_ratio
        ).select_from(
            StockBasic.__table__.join(
                DailyPrice.__table__, StockBasic.ts_code == DailyPrice.ts_code
            ).join(
                DailyBasic.__table__, 
                and_(
                    DailyPrice.ts_code == DailyBasic.ts_code,
                    DailyPrice.trade_date == DailyBasic.trade_date
                )
            )
        ).where(
            and_(
                DailyPrice.trade_date == screening_date,
                StockBasic.list_status == 'L',  # 只选择正常上市的股票
                DailyPrice.close.isnot(None),  # 确保有收盘价
                DailyPrice.vol > 0  # 确保有成交量
            )
        )
        
        # 应用市场过滤器
        if market_filter == 'main':
            # 主板：上海600开头，深圳000开头
            stmt = stmt.where(
                or_(
                    StockBasic.ts_code.like('600%'),
                    StockBasic.ts_code.like('000%')
                )
            )
        elif market_filter == 'sme':
            # 中小板：002开头
            stmt = stmt.where(StockBasic.ts_code.like('002%'))
        elif market_filter == 'gem':
            # 创业板：300开头
            stmt = stmt.where(StockBasic.ts_code.like('300%'))
        
        # 执行查询
        result = self.session.execute(stmt).fetchall()
        
        if not result:
            return pd.DataFrame()
        
        # 转换为DataFrame
        columns = [
            'ts_code', 'name', 'industry', 'area', 'market', 'trade_date',
            'open', 'high', 'low', 'close', 'pre_close', 'pct_chg', 'vol', 'amount',
            'pe', 'pe_ttm', 'pb', 'ps', 'ps_ttm', 'total_mv', 'circ_mv', 'turnover_rate', 'volume_ratio'
        ]
        
        data = [dict(zip(columns, row)) for row in result]
        df = pd.DataFrame(data)
        
        if self.logger:
            self.logger.info(f"[基础数据加载完成] 共加载{len(df)}条记录")
        
        return df
    
    def _get_next_trading_date(self, start_date: str, days_offset: int) -> Optional[str]:
        """获取指定日期后第N个交易日"""
        if self.logger:
            self.logger.info(f"[查找交易日] 查找{start_date}后第{days_offset}个交易日")
        
        # 查询数据库中的交易日期
        stmt = select(DailyPrice.trade_date.distinct()).where(
            DailyPrice.trade_date > start_date
        ).order_by(DailyPrice.trade_date).limit(days_offset)
        
        result = self.session.execute(stmt).scalars().all()
        
        if len(result) >= days_offset:
            target_date = result[days_offset - 1]
            if self.logger:
                self.logger.info(f"[找到交易日] {start_date}后第{days_offset}个交易日为{target_date}")
            return target_date
        else:
            if self.logger:
                self.logger.warning(f"[交易日不足] 只找到{len(result)}个交易日，需要{days_offset}个")
            return None
    
    def _get_prices_by_date(self, stock_codes: List[str], trade_date: str) -> pd.DataFrame:
        """获取指定股票在指定日期的价格数据"""
        if self.logger:
            self.logger.info(f"[获取价格数据] 日期={trade_date}，股票数量={len(stock_codes)}")
        
        stmt = select(DailyPrice).where(
            and_(
                DailyPrice.ts_code.in_(stock_codes),
                DailyPrice.trade_date == trade_date
            )
        )
        
        result = self.session.execute(stmt).scalars().all()
        
        if not result:
            return pd.DataFrame()
        
        data = []
        for record in result:
            data.append({
                'ts_code': record.ts_code,
                'trade_date': record.trade_date,
                'open': record.open,
                'high': record.high,
                'low': record.low,
                'close': record.close,
                'vol': record.vol,
                'amount': record.amount
            })
        
        df = pd.DataFrame(data)
        
        if self.logger:
            self.logger.info(f"[价格数据获取完成] 获取{len(df)}条价格记录")
        
        return df
    
    def export_analysis_result(
        self,
        result: PerformanceAnalysisResult,
        output_dir: str = "/Users/nxm/PycharmProjects/dataDig/results"
    ) -> Dict[str, str]:
        """
        导出分析结果到文件
        
        Args:
            result: 分析结果
            output_dir: 输出目录
            
        Returns:
            导出的文件路径字典
        """
        import os
        
        # 创建输出目录
        os.makedirs(output_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        exported_files = {}
        
        try:
            # 导出详细股票表现
            if result.stock_performances:
                detail_file = os.path.join(output_dir, f"股票筛选表现分析_{result.screening_date}_{timestamp}.csv")
                df = pd.DataFrame(result.stock_performances)
                df.to_csv(detail_file, index=False, encoding='utf-8-sig')
                exported_files['detail'] = detail_file
                
                if self.logger:
                    self.logger.info(f"[导出详情] 详细表现数据已导出到{detail_file}")
            
            # 导出统计摘要
            summary_file = os.path.join(output_dir, f"股票筛选统计摘要_{result.screening_date}_{timestamp}.txt")
            with open(summary_file, 'w', encoding='utf-8') as f:
                f.write(f"股票筛选表现分析报告\\n")
                f.write("="*50 + "\\n")
                f.write(f"筛选日期: {result.screening_date}\\n")
                f.write(f"筛选条件: {result.screening_condition}\\n")
                f.write(f"分析天数: {result.analysis_days}天后\\n")
                f.write(f"筛选股票总数: {result.total_screened}只\\n")
                f.write("\\n")
                f.write("表现统计:\\n")
                f.write(f"  平均收益率: {result.avg_return:.2f}%\\n")
                f.write(f"  中位数收益率: {result.median_return:.2f}%\\n")
                f.write(f"  胜率: {result.win_rate:.2f}%\\n")
                f.write(f"  最大收益率: {result.max_return:.2f}%\\n")
                f.write(f"  最小收益率: {result.min_return:.2f}%\\n")
                f.write(f"  上涨股票数: {result.positive_count}只\\n")
                f.write(f"  下跌股票数: {result.negative_count}只\\n")
            
            exported_files['summary'] = summary_file
            
            if self.logger:
                self.logger.info(f"[导出摘要] 统计摘要已导出到{summary_file}")
        
        except Exception as e:
            if self.logger:
                self.logger.error(f"[导出失败] 导出分析结果时发生错误: {str(e)}")
        
        return exported_files
    
    # 预定义的筛选条件工厂方法
    def create_pe_condition(self, min_pe: float = None, max_pe: float = None) -> FundamentalCondition:
        """创建市盈率筛选条件"""
        if min_pe is not None and max_pe is not None:
            name = f"市盈率{min_pe}-{max_pe}"
            # 需要自定义逻辑，这里先返回一个基础的
            return FundamentalCondition('pe', '>=', min_pe, name, f"市盈率在{min_pe}到{max_pe}之间")
        elif min_pe is not None:
            return FundamentalCondition('pe', '>=', min_pe, f"市盈率>={min_pe}", f"市盈率大于等于{min_pe}")
        elif max_pe is not None:
            return FundamentalCondition('pe', '<=', max_pe, f"市盈率<={max_pe}", f"市盈率小于等于{max_pe}")
        else:
            raise ValueError("至少需要提供min_pe或max_pe中的一个")
    
    def create_pb_condition(self, min_pb: float = None, max_pb: float = None) -> FundamentalCondition:
        """创建市净率筛选条件"""
        if min_pb is not None and max_pb is not None:
            name = f"市净率{min_pb}-{max_pb}"
            return FundamentalCondition('pb', '>=', min_pb, name, f"市净率在{min_pb}到{max_pb}之间")
        elif min_pb is not None:
            return FundamentalCondition('pb', '>=', min_pb, f"市净率>={min_pb}", f"市净率大于等于{min_pb}")
        elif max_pb is not None:
            return FundamentalCondition('pb', '<=', max_pb, f"市净率<={max_pb}", f"市净率小于等于{max_pb}")
        else:
            raise ValueError("至少需要提供min_pb或max_pb中的一个")
    
    def create_market_cap_condition(self, min_mv: float = None, max_mv: float = None) -> FundamentalCondition:
        """创建市值筛选条件（单位：万元）"""
        if min_mv is not None and max_mv is not None:
            name = f"市值{min_mv/10000:.0f}-{max_mv/10000:.0f}亿"
            return FundamentalCondition('total_mv', '>=', min_mv, name, f"总市值在{min_mv/10000:.0f}到{max_mv/10000:.0f}亿之间")
        elif min_mv is not None:
            return FundamentalCondition('total_mv', '>=', min_mv, f"市值>={min_mv/10000:.0f}亿", f"总市值大于等于{min_mv/10000:.0f}亿")
        elif max_mv is not None:
            return FundamentalCondition('total_mv', '<=', max_mv, f"市值<={max_mv/10000:.0f}亿", f"总市值小于等于{max_mv/10000:.0f}亿")
        else:
            raise ValueError("至少需要提供min_mv或max_mv中的一个")
    
    def create_turnover_condition(self, min_turnover: float = None, max_turnover: float = None) -> FundamentalCondition:
        """创建换手率筛选条件"""
        if min_turnover is not None and max_turnover is not None:
            name = f"换手率{min_turnover}-{max_turnover}%"
            return FundamentalCondition('turnover_rate', '>=', min_turnover, name, f"换手率在{min_turnover}%到{max_turnover}%之间")
        elif min_turnover is not None:
            return FundamentalCondition('turnover_rate', '>=', min_turnover, f"换手率>={min_turnover}%", f"换手率大于等于{min_turnover}%")
        elif max_turnover is not None:
            return FundamentalCondition('turnover_rate', '<=', max_turnover, f"换手率<={max_turnover}%", f"换手率小于等于{max_turnover}%")
        else:
            raise ValueError("至少需要提供min_turnover或max_turnover中的一个")
    
    def create_volume_surge_condition(self, min_volume_ratio: float = 2.0) -> FundamentalCondition:
        """创建放量条件（量比）"""
        return FundamentalCondition('volume_ratio', '>=', min_volume_ratio, f"量比>={min_volume_ratio}", f"量比大于等于{min_volume_ratio}（相对放量）")
    
    def create_price_change_condition(self, min_pct: float = None, max_pct: float = None) -> FundamentalCondition:
        """创建涨跌幅筛选条件"""
        if min_pct is not None and max_pct is not None:
            name = f"涨跌幅{min_pct}%-{max_pct}%"
            return FundamentalCondition('pct_chg', '>=', min_pct, name, f"涨跌幅在{min_pct}%到{max_pct}%之间")
        elif min_pct is not None:
            return FundamentalCondition('pct_chg', '>=', min_pct, f"涨幅>={min_pct}%", f"涨跌幅大于等于{min_pct}%")
        elif max_pct is not None:
            return FundamentalCondition('pct_chg', '<=', max_pct, f"跌幅<={max_pct}%", f"涨跌幅小于等于{max_pct}%")
        else:
            raise ValueError("至少需要提供min_pct或max_pct中的一个")
    
    def get_stock_board(self, ts_code: str) -> str:
        """判断股票所属板块"""
        for board, rule in self.BOARD_RULES.items():
            if rule(ts_code):
                return board
        return 'unknown'
    
    def get_corresponding_index(self, ts_code: str) -> str:
        """获取股票对应的主要指数代码"""
        board = self.get_stock_board(ts_code)
        
        # 根据板块返回对应的主要指数
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
    
    def get_index_performance(self, index_code: str, trade_date: str) -> Optional[float]:
        """获取指数在指定日期的涨跌幅"""
        if self.logger:
            self.logger.info(f"[获取指数表现] 查询指数={index_code}，日期={trade_date}")
        
        stmt = select(IndexDaily.pct_chg).where(
            and_(
                IndexDaily.ts_code == index_code,
                IndexDaily.trade_date == trade_date
            )
        )
        
        result = self.session.execute(stmt).scalar()
        
        if result is not None:
            if self.logger:
                self.logger.info(f"[指数表现] {index_code}在{trade_date}涨跌幅={result:.2f}%")
            return float(result)
        else:
            if self.logger:
                self.logger.warning(f"[指数表现] 未找到{index_code}在{trade_date}的数据")
            return None
    
    def get_stock_historical_performance(self, ts_code: str, end_date: str, days: int = 20) -> Optional[float]:
        """获取股票历史期间涨幅"""
        if self.logger:
            self.logger.info(f"[历史涨幅查询] 股票={ts_code}，截止日期={end_date}，天数={days}")
        
        # 查询最近N个交易日的价格数据
        stmt = select(DailyPrice.trade_date, DailyPrice.close).where(
            and_(
                DailyPrice.ts_code == ts_code,
                DailyPrice.trade_date <= end_date
            )
        ).order_by(DailyPrice.trade_date.desc()).limit(days + 1)
        
        result = self.session.execute(stmt).fetchall()
        
        if len(result) < days:
            if self.logger:
                self.logger.warning(f"[历史涨幅] {ts_code}历史数据不足，仅{len(result)}条记录")
            return None
        
        # 计算涨幅：(最新价 - N天前价格) / N天前价格 * 100
        latest_price = result[0][1]  # 最新收盘价
        past_price = result[days - 1][1]  # N天前收盘价（注意索引）
        
        if past_price and past_price > 0:
            performance = (latest_price - past_price) / past_price * 100
            if self.logger:
                self.logger.info(f"[历史涨幅] {ts_code}过去{days}日涨幅={performance:.2f}%")
            return performance
        else:
            if self.logger:
                self.logger.warning(f"[历史涨幅] {ts_code}历史价格数据异常")
            return None


class ContrarianCondition(ScreeningCondition):
    """逆向投资筛选条件：大盘涨个股跌"""
    
    def __init__(self, 
                 screener_service: StockScreenerService,
                 screening_date: str,
                 min_index_rise: float = 2.0,      # 大盘最小涨幅
                 max_stock_fall: float = -6.0,     # 个股最大跌幅（负数）
                 max_historical_rise: float = 20.0, # 历史最大涨幅
                 historical_days: int = 20):       # 历史天数
        name = f"逆向策略(指数涨≥{min_index_rise}%,个股跌≤{max_stock_fall}%,{historical_days}日涨幅≤{max_historical_rise}%)"
        super().__init__(name, "大盘上涨时个股下跌的逆向投资机会")
        
        self.screener_service = screener_service
        self.screening_date = screening_date
        self.min_index_rise = min_index_rise
        self.max_stock_fall = max_stock_fall
        self.max_historical_rise = max_historical_rise
        self.historical_days = historical_days
    
    def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        """应用逆向投资筛选条件"""
        if data.empty:
            return data
        
        logger = self.screener_service.logger
        if logger:
            logger.info(f"[逆向条件筛选] 开始应用逆向条件，输入股票数={len(data)}")
        
        filtered_stocks = []
        
        for _, row in data.iterrows():
            ts_code = row['ts_code']
            stock_pct_chg = row.get('pct_chg', 0)
            
            # 1. 检查个股跌幅是否符合条件
            if stock_pct_chg > self.max_stock_fall:
                continue
            
            # 2. 获取对应指数涨幅
            index_code = self.screener_service.get_corresponding_index(ts_code)
            index_performance = self.screener_service.get_index_performance(index_code, self.screening_date)
            
            if index_performance is None or index_performance < self.min_index_rise:
                continue
            
            # 3. 检查历史涨幅
            historical_performance = self.screener_service.get_stock_historical_performance(
                ts_code, self.screening_date, self.historical_days
            )
            
            if historical_performance is None or historical_performance > self.max_historical_rise:
                continue
            
            # 符合所有条件，添加到结果中
            stock_info = row.to_dict()
            stock_info['corresponding_index'] = index_code
            stock_info['index_pct_chg'] = index_performance
            stock_info['historical_performance'] = historical_performance
            stock_info['board'] = self.screener_service.get_stock_board(ts_code)
            
            filtered_stocks.append(stock_info)
            
            if logger:
                logger.info(f"[符合条件] {ts_code} {row.get('name', '')}：个股跌{stock_pct_chg:.2f}%，"
                          f"对应指数({index_code})涨{index_performance:.2f}%，"
                          f"{self.historical_days}日涨幅{historical_performance:.2f}%")
        
        result_df = pd.DataFrame(filtered_stocks) if filtered_stocks else pd.DataFrame()
        
        if logger:
            logger.info(f"[逆向条件结果] 筛选出{len(result_df)}只符合逆向条件的股票")
        
        return result_df
