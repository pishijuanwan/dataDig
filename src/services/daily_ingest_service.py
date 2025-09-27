from datetime import datetime
from typing import Optional
import pandas as pd

from src.datasource.tushare_client import TushareClient
from src.repository.daily_repository import DailyRepository


class DailyIngestService:
    def __init__(self, ts_client: TushareClient, repo: DailyRepository, logger=None):
        self._ts = ts_client
        self._repo = repo
        self._logger = logger

    def _normalize_date(self, dt: Optional[str]) -> Optional[str]:
        if not dt:
            return None
        if isinstance(dt, str) and len(dt) == 10 and dt[4] == '-' and dt[7] == '-':
            return dt.replace('-', '')
        return dt

    def ingest_all_from_to(self, start_date: str, end_date: Optional[str] = None):
        start = self._normalize_date(start_date)
        end = self._normalize_date(end_date) if end_date else datetime.today().strftime("%Y%m%d")
        if self._logger:
            self._logger.info("[流程] 开始全市场日线拉取，起始=%s，结束=%s", start, end)

        # 1) 股票列表
        stock_df = self._ts.query_stock_basic()
        self._repo.upsert_stock_basic(stock_df)

        # 2) 交易日历
        cal_df = self._ts.query_trade_cal(start_date=start, end_date=end)
        trade_dates = cal_df["cal_date"].tolist()
        if self._logger:
            self._logger.info("[流程] 交易日总数=%s，将逐日拉取 daily 数据", len(trade_dates))

        # 3) 逐交易日获取全量 daily（按日期，两市所有可交易股票）
        for idx, trade_date in enumerate(trade_dates, 1):
            if self._logger:
                self._logger.info("[流程] (%s/%s) 拉取交易日=%s 的全市场日线", idx, len(trade_dates), trade_date)
            df = self._ts.query_daily(trade_date=trade_date)
            if df is None or df.empty:
                if self._logger:
                    self._logger.info("[流程] 交易日=%s 无数据，跳过", trade_date)
                continue
            self._repo.upsert_daily_prices(df)
            if self._logger:
                self._logger.info("[流程] 交易日=%s 写入完成，记录数=%s", trade_date, len(df))

    def ingest_incremental(self, start_date: str):
        """基于库内最大交易日增量拉取（包含空库场景）。"""
        start = self._normalize_date(start_date)
        max_date = self._repo.get_max_trade_date()
        if max_date is None:
            if self._logger:
                self._logger.info("[流程] 库内无历史数据，首次全量自%s开始拉取", start)
            self.ingest_all_from_to(start_date=start)
            return
        
        # 获取从库内最大交易日之后的所有交易日
        if self._logger:
            self._logger.info("[流程] 增量模式：从库内最大交易日(%s)的下一个交易日起拉取", max_date)
        
        # 从最大交易日的下一天开始查询交易日历，避免重复
        from datetime import datetime, timedelta
        max_date_obj = datetime.strptime(max_date, "%Y%m%d")
        next_day = (max_date_obj + timedelta(days=1)).strftime("%Y%m%d")
        today = datetime.today().strftime("%Y%m%d")
        
        cal_df = self._ts.query_trade_cal(start_date=next_day, end_date=today)
        trade_dates = cal_df["cal_date"].tolist()
        trade_dates.sort()  # 确保按时间正序排列
        
        if not trade_dates:
            if self._logger:
                self._logger.info("[流程] 无需增量：最新交易日已在库中")
            return
            
        if self._logger:
            self._logger.info("[流程] 需要增量交易日数量=%s", len(trade_dates))
            
        for idx, trade_date in enumerate(trade_dates, 1):
            if self._logger:
                self._logger.info("[流程] (增量 %s/%s) 拉取交易日=%s 的全市场日线", idx, len(trade_dates), trade_date)
            df = self._ts.query_daily(trade_date=trade_date)
            if df is None or df.empty:
                if self._logger:
                    self._logger.info("[流程] 交易日=%s 无数据，跳过", trade_date)
                continue
            self._repo.upsert_daily_prices(df)
            if self._logger:
                self._logger.info("[流程] 交易日=%s 写入完成，记录数=%s", trade_date, len(df))
