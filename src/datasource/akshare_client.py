from typing import Optional, Dict
import time
import pandas as pd
import akshare as ak
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type


class AKShareClient:
    """
    AKShare数据客户端
    主要用于获取个股基本信息等数据
    """
    
    def __init__(self, sleep_seconds_between_calls: float = 0.5, logger=None):
        """
        初始化AKShare客户端
        
        Args:
            sleep_seconds_between_calls: 调用间隔，避免频繁请求
            logger: 日志记录器
        """
        self._sleep = sleep_seconds_between_calls
        self._logger = logger
        if self._logger:
            self._logger.info("[流程] 已初始化 AKShare 客户端，调用间隔=%ss", self._sleep)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10), reraise=True)
    def get_stock_individual_info(self, symbol: str) -> Optional[Dict]:
        """
        获取个股详细信息 - 使用东方财富接口
        
        Args:
            symbol: 股票代码，如 "000001"
            
        Returns:
            包含个股信息的字典，失败返回None
        """
        if self._logger:
            self._logger.info("[流程] 调用 AKShare stock_individual_info_em 接口，symbol=%s", symbol)
        
        try:
            # 使用AKShare的个股信息查询接口
            df = ak.stock_individual_info_em(symbol=symbol)
            time.sleep(self._sleep)
            
            if df is None or df.empty:
                if self._logger:
                    self._logger.warning("[流程] 股票 %s 未返回个股信息数据", symbol)
                return None
                
            # 将DataFrame转换为字典格式，方便使用
            info_dict = {}
            for _, row in df.iterrows():
                key = row['item'] if 'item' in row else row.iloc[0]
                value = row['value'] if 'value' in row else row.iloc[1]
                info_dict[key] = value
                
            if self._logger:
                self._logger.info("[流程] 股票 %s 个股信息获取成功，包含 %d 项信息", symbol, len(info_dict))
            
            return info_dict
            
        except Exception as e:
            if self._logger:
                self._logger.error("[流程] 获取股票 %s 个股信息失败: %s", symbol, str(e))
            return None

    def extract_key_metrics(self, stock_info: Dict) -> Dict:
        """
        从个股信息中提取关键指标
        
        Args:
            stock_info: 个股信息字典
            
        Returns:
            包含关键指标的字典
        """
        if not stock_info:
            return {}
            
        metrics = {}
        
        # 定义需要提取的关键指标映射
        # AKShare返回的字段名可能有所不同，需要根据实际情况调整
        key_mappings = {
            '总股本': 'total_share',
            '流通股': 'float_share', 
            '总市值': 'total_mv',
            '流通市值': 'circ_mv',
            # 可能的其他字段名
            '总股本(万股)': 'total_share',
            '流通股本(万股)': 'float_share',
            '总市值(万元)': 'total_mv', 
            '流通市值(万元)': 'circ_mv',
            '总市值(元)': 'total_mv_yuan',  # 如果是元为单位，需要转换
            '流通市值(元)': 'circ_mv_yuan'
        }
        
        for ak_key, our_key in key_mappings.items():
            if ak_key in stock_info:
                value = stock_info[ak_key]
                if value is not None and str(value).strip() != '' and str(value) != '-':
                    try:
                        # 清理数字字符串，移除逗号等
                        value_str = str(value).replace(',', '').replace('万', '').replace('元', '').strip()
                        numeric_value = float(value_str)
                        
                        # 如果原始单位是元，转换为万元
                        if our_key.endswith('_yuan'):
                            numeric_value = numeric_value / 10000
                            our_key = our_key.replace('_yuan', '')
                            
                        metrics[our_key] = numeric_value
                        
                        if self._logger:
                            self._logger.debug("[数据转换] %s: %s -> %s = %f", ak_key, value, our_key, numeric_value)
                            
                    except (ValueError, TypeError) as e:
                        if self._logger:
                            self._logger.warning("[数据转换] 无法转换 %s 的值 %s: %s", ak_key, value, str(e))
        
        if self._logger:
            self._logger.info("[流程] 提取关键指标完成，包含 %d 项指标", len(metrics))
            
        return metrics

    def batch_get_stock_metrics(self, symbols: list) -> pd.DataFrame:
        """
        批量获取多只股票的关键指标
        
        Args:
            symbols: 股票代码列表
            
        Returns:
            包含所有股票关键指标的DataFrame
        """
        if self._logger:
            self._logger.info("[流程] 开始批量获取 %d 只股票的关键指标", len(symbols))
            
        results = []
        
        for i, symbol in enumerate(symbols):
            if self._logger and (i + 1) % 100 == 0:
                self._logger.info("[进度] 已处理 %d/%d 只股票", i + 1, len(symbols))
                
            try:
                # 去除后缀，只保留数字部分
                clean_symbol = symbol.split('.')[0]
                
                stock_info = self.get_stock_individual_info(clean_symbol)
                if stock_info:
                    metrics = self.extract_key_metrics(stock_info)
                    if metrics:
                        metrics['symbol'] = clean_symbol
                        metrics['ts_code'] = symbol  # 保留完整的ts_code
                        results.append(metrics)
                        
            except Exception as e:
                if self._logger:
                    self._logger.error("[流程] 处理股票 %s 时发生错误: %s", symbol, str(e))
                continue
                
        if self._logger:
            self._logger.info("[流程] 批量获取完成，成功获取 %d/%d 只股票的数据", len(results), len(symbols))
            
        return pd.DataFrame(results) if results else pd.DataFrame()
