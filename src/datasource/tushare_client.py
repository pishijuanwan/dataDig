from typing import Optional, List
import time
import tushare as ts
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type


class TushareClient:
    def __init__(self, token: str, requests_per_minute_limit: int = 450, sleep_seconds_between_calls: float = 0.15, logger=None):
        self._token = token
        self._rpm_limit = requests_per_minute_limit
        self._sleep = sleep_seconds_between_calls
        self._logger = logger
        ts.set_token(self._token)
        self._pro = ts.pro_api()
        if self._logger:
            self._logger.info("[流程] 已初始化 Tushare 客户端，速率限制=%s rpm，调用间隔=%ss", self._rpm_limit, self._sleep)

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=10), reraise=True)
    def query_daily(self, ts_code: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None, trade_date: Optional[str] = None):
        if self._logger:
            self._logger.info("[流程] 调用 Tushare daily 接口，ts_code=%s, start_date=%s, end_date=%s, trade_date=%s", ts_code, start_date, end_date, trade_date)
        df = self._pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date, trade_date=trade_date)
        time.sleep(self._sleep)
        if self._logger:
            self._logger.info("[流程] Tushare daily 返回数据行数=%s", len(df) if df is not None else 0)
        return df

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=10), reraise=True)
    def query_trade_cal(self, exchange: str = "SSE", start_date: Optional[str] = None, end_date: Optional[str] = None):
        if self._logger:
            self._logger.info("[流程] 调用 Tushare 交易日历接口，exchange=%s, start_date=%s, end_date=%s", exchange, start_date, end_date)
        df = self._pro.trade_cal(exchange=exchange, start_date=start_date, end_date=end_date, is_open=1)
        time.sleep(self._sleep)
        if self._logger:
            self._logger.info("[流程] 交易日历返回交易日数量=%s", len(df) if df is not None else 0)
        return df

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=10), reraise=True)
    def query_stock_basic(self, list_status: str = "L"):
        if self._logger:
            self._logger.info("[流程] 调用 Tushare 股票列表接口，list_status=%s", list_status)
        df = self._pro.stock_basic(fields="ts_code,symbol,name,area,industry,market,list_date,list_status")
        time.sleep(self._sleep)
        if self._logger:
            self._logger.info("[流程] 股票列表返回数量=%s", len(df) if df is not None else 0)
        return df

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=10), reraise=True)
    def query_daily_basic(self, ts_code: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None, trade_date: Optional[str] = None):
        if self._logger:
            self._logger.info("[流程] 调用 Tushare 每日指标接口，ts_code=%s, start_date=%s, end_date=%s, trade_date=%s", ts_code, start_date, end_date, trade_date)
        df = self._pro.daily_basic(
            ts_code=ts_code, 
            start_date=start_date, 
            end_date=end_date, 
            trade_date=trade_date,
            fields="ts_code,trade_date,close,turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,total_share,float_share,free_share,total_mv,circ_mv"
        )
        time.sleep(self._sleep)
        if self._logger:
            self._logger.info("[流程] Tushare 每日指标返回数据行数=%s", len(df) if df is not None else 0)
        return df

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=10), reraise=True)
    def query_index_basic(self, market: str = ""):
        if self._logger:
            self._logger.info("[流程] 调用 Tushare 指数基本信息接口，market=%s", market)
        df = self._pro.index_basic(
            market=market,
            fields="ts_code,name,market,publisher,index_type,category,base_date,base_point,list_date"
        )
        time.sleep(self._sleep)
        if self._logger:
            self._logger.info("[流程] 指数基本信息返回数量=%s", len(df) if df is not None else 0)
        return df

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=10), reraise=True)
    def query_index_daily(self, ts_code: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None, trade_date: Optional[str] = None):
        if self._logger:
            self._logger.info("[流程] 调用 Tushare 指数日线接口，ts_code=%s, start_date=%s, end_date=%s, trade_date=%s", ts_code, start_date, end_date, trade_date)
        df = self._pro.index_daily(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            trade_date=trade_date,
            fields="ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount"
        )
        time.sleep(self._sleep)
        if self._logger:
            self._logger.info("[流程] 指数日线返回数据行数=%s", len(df) if df is not None else 0)
        return df
