from datetime import datetime
from typing import Optional, List
import pandas as pd

from src.datasource.tushare_client import TushareClient
from src.repository.daily_repository import DailyRepository


class IndexIngestService:
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

    def _get_major_index_codes(self) -> List[str]:
        """返回主要大盘指数代码列表"""
        # 主要的大盘指数代码
        major_indices = [
            "000001.SH",  # 上证指数
            "399001.SZ",  # 深证成指
            "399006.SZ",  # 创业板指
            "000300.SH",  # 沪深300
            "000016.SH",  # 上证50
            "000905.SH",  # 中证500
            "000852.SH",  # 中证1000
            "000688.SH",  # 科创50
        ]
        return major_indices

    def ingest_index_basic_all(self):
        """下载并存储所有指数基本信息"""
        if self._logger:
            self._logger.info("[流程] 开始下载指数基本信息数据")
        
        # 获取指数基本信息（默认为空获取全部）
        index_basic_df = self._ts.query_index_basic()
        
        if index_basic_df is not None and not index_basic_df.empty:
            self._repo.upsert_index_basic(index_basic_df)
            if self._logger:
                self._logger.info("[流程] 指数基本信息下载完成，共处理 %s 条记录", len(index_basic_df))
        else:
            if self._logger:
                self._logger.warning("[流程] 指数基本信息API返回空数据")

    def ingest_major_indices_from_to(self, start_date: str, end_date: Optional[str] = None):
        """下载主要指数的日线数据（指定日期范围）"""
        start = self._normalize_date(start_date)
        end = self._normalize_date(end_date) if end_date else datetime.today().strftime("%Y%m%d")
        if self._logger:
            self._logger.info("[流程] 开始下载主要指数日线数据，起始=%s，结束=%s", start, end)

        # 1) 先更新指数基本信息
        self.ingest_index_basic_all()

        # 2) 获取交易日历
        cal_df = self._ts.query_trade_cal(start_date=start, end_date=end)
        trade_dates = cal_df["cal_date"].tolist()
        if self._logger:
            self._logger.info("[流程] 交易日总数=%s，将逐日拉取指数日线数据", len(trade_dates))

        # 3) 逐交易日获取主要指数数据
        major_indices = self._get_major_index_codes()
        
        for idx, trade_date in enumerate(trade_dates, 1):
            if self._logger:
                self._logger.info("[流程] (%s/%s) 检查交易日=%s 的指数日线数据", idx, len(trade_dates), trade_date)
            
            # 先检查数据库中是否已有该交易日的指数数据
            if self._repo.has_index_daily_data(trade_date):
                if self._logger:
                    self._logger.info("[流程] 交易日=%s 指数数据库中已存在数据，跳过API调用", trade_date)
                continue
            
            if self._logger:
                self._logger.info("[流程] 交易日=%s 指数数据库中无数据，开始逐个指数调用API拉取", trade_date)
            
            # 由于ts_code是必选参数，需要逐个指数获取数据
            all_df_list = []
            for ts_code in major_indices:
                try:
                    if self._logger:
                        self._logger.info("[流程] 获取指数=%s 交易日=%s 的数据", ts_code, trade_date)
                    df = self._ts.query_index_daily(ts_code=ts_code, trade_date=trade_date)
                    if df is not None and not df.empty:
                        all_df_list.append(df)
                        if self._logger:
                            self._logger.info("[流程] 指数=%s 交易日=%s 获取成功，记录数=%s", ts_code, trade_date, len(df))
                    else:
                        if self._logger:
                            self._logger.info("[流程] 指数=%s 交易日=%s 无数据", ts_code, trade_date)
                except Exception as e:
                    if self._logger:
                        self._logger.warning("[流程] 指数=%s 交易日=%s 获取失败：%s", ts_code, trade_date, str(e))
            
            # 合并所有指数数据并写入数据库
            if all_df_list:
                combined_df = pd.concat(all_df_list, ignore_index=True)
                self._repo.upsert_index_daily(combined_df)
                if self._logger:
                    self._logger.info("[流程] 交易日=%s 指数数据写入完成，总记录数=%s", trade_date, len(combined_df))
            else:
                if self._logger:
                    self._logger.info("[流程] 交易日=%s 无任何指数数据", trade_date)

    def ingest_specific_indices_from_to(self, ts_codes: List[str], start_date: str, end_date: Optional[str] = None):
        """下载指定指数的日线数据（指定日期范围）"""
        start = self._normalize_date(start_date)
        end = self._normalize_date(end_date) if end_date else datetime.today().strftime("%Y%m%d")
        if self._logger:
            self._logger.info("[流程] 开始下载指定指数日线数据，指数=%s，起始=%s，结束=%s", ts_codes, start, end)

        # 1) 先更新指数基本信息
        self.ingest_index_basic_all()

        # 2) 逐指数拉取数据
        for ts_code in ts_codes:
            if self._logger:
                self._logger.info("[流程] 开始下载指数=%s 的日线数据", ts_code)
            
            df = self._ts.query_index_daily(ts_code=ts_code, start_date=start, end_date=end)
            if df is None or df.empty:
                if self._logger:
                    self._logger.warning("[流程] 指数=%s 在指定时间范围内无数据", ts_code)
                continue
            
            self._repo.upsert_index_daily(df)
            if self._logger:
                self._logger.info("[流程] 指数=%s 数据写入完成，记录数=%s", ts_code, len(df))

    def ingest_incremental(self, start_date: str):
        """基于库内最大交易日增量拉取指数数据（包含空库场景）"""
        start = self._normalize_date(start_date)
        max_date = self._repo.get_max_trade_date_index()
        
        if max_date is None:
            if self._logger:
                self._logger.info("[流程] 库内无指数历史数据，首次全量自%s开始拉取", start)
            self.ingest_major_indices_from_to(start_date=start)
            return
        
        # 获取从库内最大交易日之后的所有交易日
        if self._logger:
            self._logger.info("[流程] 指数增量模式：从库内最大交易日(%s)的下一个交易日起拉取", max_date)
        
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
                self._logger.info("[流程] 无需指数增量：最新交易日已在库中")
            return
            
        if self._logger:
            self._logger.info("[流程] 需要增量的指数交易日数量=%s", len(trade_dates))
            
        # 获取主要指数列表
        major_indices = self._get_major_index_codes()
        
        for idx, trade_date in enumerate(trade_dates, 1):
            if self._logger:
                self._logger.info("[流程] (指数增量 %s/%s) 检查交易日=%s 的指数日线数据", idx, len(trade_dates), trade_date)
            
            # 先检查数据库中是否已有该交易日的指数数据
            if self._repo.has_index_daily_data(trade_date):
                if self._logger:
                    self._logger.info("[流程] 交易日=%s 指数数据库中已存在数据，跳过API调用", trade_date)
                continue
            
            if self._logger:
                self._logger.info("[流程] 交易日=%s 指数数据库中无数据，开始逐个指数调用API拉取", trade_date)
            
            # 由于ts_code是必选参数，需要逐个指数获取数据
            all_df_list = []
            for ts_code in major_indices:
                try:
                    if self._logger:
                        self._logger.info("[流程] 增量获取指数=%s 交易日=%s 的数据", ts_code, trade_date)
                    df = self._ts.query_index_daily(ts_code=ts_code, trade_date=trade_date)
                    if df is not None and not df.empty:
                        all_df_list.append(df)
                        if self._logger:
                            self._logger.info("[流程] 指数=%s 交易日=%s 增量获取成功，记录数=%s", ts_code, trade_date, len(df))
                    else:
                        if self._logger:
                            self._logger.info("[流程] 指数=%s 交易日=%s 无数据", ts_code, trade_date)
                except Exception as e:
                    if self._logger:
                        self._logger.warning("[流程] 指数=%s 交易日=%s 增量获取失败：%s", ts_code, trade_date, str(e))
            
            # 合并所有指数数据并写入数据库
            if all_df_list:
                combined_df = pd.concat(all_df_list, ignore_index=True)
                self._repo.upsert_index_daily(combined_df)
                if self._logger:
                    self._logger.info("[流程] 交易日=%s 指数增量数据写入完成，总记录数=%s", trade_date, len(combined_df))
            else:
                if self._logger:
                    self._logger.info("[流程] 交易日=%s 无任何指数增量数据", trade_date)
