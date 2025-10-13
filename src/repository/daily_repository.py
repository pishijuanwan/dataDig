from typing import List
import pandas as pd
import numpy as np
from sqlalchemy import insert
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.orm import Session
from sqlalchemy import select, func

from src.models.daily_price import StockBasic, DailyPrice, DailyBasic, IndexBasic, IndexDaily


class DailyRepository:
    def __init__(self, session: Session, logger=None):
        self._session = session
        self._logger = logger

    def upsert_stock_basic(self, df: pd.DataFrame):
        if df is None or df.empty:
            if self._logger:
                self._logger.info("[流程] 股票基础信息数据为空，跳过 upsert")
            return
        
        # 处理 NaN 值：将 NaN 替换为 None，避免 MySQL 数据库错误
        # 使用多种方法确保彻底清除 NaN 值
        df = df.replace([np.nan, np.inf, -np.inf], None)
        df = df.where(pd.notna(df), None)
        if self._logger:
            self._logger.info("[流程] 股票基础信息数据预处理完成，已将 NaN 值替换为 None")
        
        records = df.to_dict(orient="records")
        
        # 二次清理：确保 records 中没有任何 NaN 值
        for record in records:
            for key, value in record.items():
                if pd.isna(value) or (isinstance(value, float) and (np.isnan(value) or np.isinf(value))):
                    record[key] = None
        
        if self._logger:
            self._logger.info("[流程] 股票基础信息记录级别的 NaN 值二次清理完成")
        # 仅更新传入字段中的非主键字段；禁止更新 created_at/updated_at 以避免引用不存在的 inserted 列
        keys = set(records[0].keys()) if records else set()
        update_field_names = [
            k for k in keys if k not in ("ts_code", "created_at", "updated_at")
        ]

        def _do_batch(batch):
            stmt = mysql_insert(StockBasic).values(batch)
            update_cols = {name: getattr(stmt.inserted, name) for name in update_field_names}
            ondup = stmt.on_duplicate_key_update(update_cols)
            self._session.execute(ondup)

        batch_size = 1000
        total = len(records)
        if self._logger:
            self._logger.info("[流程] 执行 stock_basic 批量 upsert，总记录数=%s，批大小=%s", total, batch_size)
        for i in range(0, total, batch_size):
            batch = records[i:i + batch_size]
            _do_batch(batch)
            if self._logger:
                self._logger.info("[流程] stock_basic 已写入进度：%s/%s", min(i + batch_size, total), total)

    def upsert_daily_prices(self, df: pd.DataFrame):
        if df is None or df.empty:
            if self._logger:
                self._logger.info("[流程] 日线数据为空，跳过 upsert")
            return
        cols = [
            "ts_code","trade_date","open","high","low","close","pre_close","change","pct_chg","vol","amount"
        ]
        df = df[cols]
        
        # 处理 NaN 值：将 NaN 替换为 None，避免 MySQL 数据库错误
        # 使用多种方法确保彻底清除 NaN 值
        df = df.replace([np.nan, np.inf, -np.inf], None)
        df = df.where(pd.notna(df), None)
        if self._logger:
            self._logger.info("[流程] 数据预处理完成，已将 NaN 值替换为 None")
        
        records = df.to_dict(orient="records")
        
        # 二次清理：确保 records 中没有任何 NaN 值
        for record in records:
            for key, value in record.items():
                if pd.isna(value) or (isinstance(value, float) and (np.isnan(value) or np.isinf(value))):
                    record[key] = None
        
        if self._logger:
            self._logger.info("[流程] 记录级别的 NaN 值二次清理完成")

        update_field_names = [
            k for k in cols if k not in ("id", "trade_date", "ts_code", "created_at", "updated_at")
        ]

        def _do_batch(batch):
            stmt = mysql_insert(DailyPrice).values(batch)
            update_cols = {name: getattr(stmt.inserted, name) for name in update_field_names}
            ondup = stmt.on_duplicate_key_update(update_cols)
            self._session.execute(ondup)

        batch_size = 5000
        total = len(records)
        if self._logger:
            self._logger.info("[流程] 执行 daily_price 批量 upsert，总记录数=%s，批大小=%s", total, batch_size)
        for i in range(0, total, batch_size):
            batch = records[i:i + batch_size]
            _do_batch(batch)
            if self._logger:
                self._logger.info("[流程] daily_price 已写入进度：%s/%s", min(i + batch_size, total), total)

    def get_max_trade_date(self) -> str:
        stmt = select(func.max(DailyPrice.trade_date))
        result = self._session.execute(stmt).scalar()
        max_date = result or None
        if self._logger:
            self._logger.info("[流程] 当前库中 daily_price 最大交易日=%s", max_date)
        return max_date
    
    def has_daily_price_data(self, trade_date: str) -> bool:
        """检查指定交易日是否已有日线数据"""
        stmt = select(func.count(DailyPrice.id)).where(DailyPrice.trade_date == trade_date)
        count = self._session.execute(stmt).scalar()
        has_data = count > 0
        if self._logger:
            self._logger.info("[流程] 检查交易日=%s 的日线数据，数据库中已有记录数=%s", trade_date, count)
        return has_data

    def upsert_daily_basic(self, df: pd.DataFrame):
        """批量插入或更新每日指标数据"""
        if df is None or df.empty:
            if self._logger:
                self._logger.info("[流程] 每日指标数据为空，跳过 upsert")
            return
        
        # 定义需要的字段列表
        cols = [
            "ts_code", "trade_date", "close", "turnover_rate", "turnover_rate_f", "volume_ratio",
            "pe", "pe_ttm", "pb", "ps", "ps_ttm", "dv_ratio", "dv_ttm", 
            "total_share", "float_share", "free_share", "total_mv", "circ_mv"
        ]
        df = df[cols]
        
        # 处理 NaN 值：将 NaN 替换为 None，避免 MySQL 数据库错误
        # 使用多种方法确保彻底清除 NaN 值
        df = df.replace([np.nan, np.inf, -np.inf], None)
        df = df.where(pd.notna(df), None)
        if self._logger:
            self._logger.info("[流程] 每日指标数据预处理完成，已将 NaN 值替换为 None")
        
        records = df.to_dict(orient="records")
        
        # 二次清理：确保 records 中没有任何 NaN 值
        for record in records:
            for key, value in record.items():
                if pd.isna(value) or (isinstance(value, float) and (np.isnan(value) or np.isinf(value))):
                    record[key] = None
        
        if self._logger:
            self._logger.info("[流程] 每日指标记录级别的 NaN 值二次清理完成")

        update_field_names = [
            k for k in cols if k not in ("id", "trade_date", "ts_code", "created_at", "updated_at")
        ]

        def _do_batch(batch):
            stmt = mysql_insert(DailyBasic).values(batch)
            update_cols = {name: getattr(stmt.inserted, name) for name in update_field_names}
            ondup = stmt.on_duplicate_key_update(update_cols)
            self._session.execute(ondup)

        batch_size = 5000
        total = len(records)
        if self._logger:
            self._logger.info("[流程] 执行 daily_basic 批量 upsert，总记录数=%s，批大小=%s", total, batch_size)
        for i in range(0, total, batch_size):
            batch = records[i:i + batch_size]
            _do_batch(batch)
            if self._logger:
                self._logger.info("[流程] daily_basic 已写入进度：%s/%s", min(i + batch_size, total), total)

    def get_max_trade_date_basic(self) -> str:
        """获取每日指标数据表中的最大交易日"""
        stmt = select(func.max(DailyBasic.trade_date))
        result = self._session.execute(stmt).scalar()
        max_date = result or None
        if self._logger:
            self._logger.info("[流程] 当前库中 daily_basic 最大交易日=%s", max_date)
        return max_date

    def upsert_index_basic(self, df: pd.DataFrame):
        """批量插入或更新指数基本信息"""
        if df is None or df.empty:
            if self._logger:
                self._logger.info("[流程] 指数基本信息数据为空，跳过 upsert")
            return
        
        # 处理 NaN 值：将 NaN 替换为 None，避免 MySQL 数据库错误
        # 使用多种方法确保彻底清除 NaN 值
        df = df.replace([np.nan, np.inf, -np.inf], None)
        df = df.where(pd.notna(df), None)
        if self._logger:
            self._logger.info("[流程] 指数基本信息数据预处理完成，已将 NaN 值替换为 None")
        
        records = df.to_dict(orient="records")
        
        # 二次清理：确保 records 中没有任何 NaN 值
        for record in records:
            for key, value in record.items():
                if pd.isna(value) or (isinstance(value, float) and (np.isnan(value) or np.isinf(value))):
                    record[key] = None
        
        if self._logger:
            self._logger.info("[流程] 指数基本信息记录级别的 NaN 值二次清理完成")
        
        # 仅更新传入字段中的非主键字段；禁止更新 created_at/updated_at 以避免引用不存在的 inserted 列
        keys = set(records[0].keys()) if records else set()
        update_field_names = [
            k for k in keys if k not in ("ts_code", "created_at", "updated_at")
        ]

        def _do_batch(batch):
            stmt = mysql_insert(IndexBasic).values(batch)
            update_cols = {name: getattr(stmt.inserted, name) for name in update_field_names}
            ondup = stmt.on_duplicate_key_update(update_cols)
            self._session.execute(ondup)

        batch_size = 1000
        total = len(records)
        if self._logger:
            self._logger.info("[流程] 执行 index_basic 批量 upsert，总记录数=%s，批大小=%s", total, batch_size)
        for i in range(0, total, batch_size):
            batch = records[i:i + batch_size]
            _do_batch(batch)
            if self._logger:
                self._logger.info("[流程] index_basic 已写入进度：%s/%s", min(i + batch_size, total), total)

    def upsert_index_daily(self, df: pd.DataFrame):
        """批量插入或更新指数日线数据"""
        if df is None or df.empty:
            if self._logger:
                self._logger.info("[流程] 指数日线数据为空，跳过 upsert")
            return
        
        cols = [
            "ts_code", "trade_date", "open", "high", "low", "close", "pre_close", "change", "pct_chg", "vol", "amount"
        ]
        df = df[cols]
        
        # 处理 NaN 值：将 NaN 替换为 None，避免 MySQL 数据库错误
        # 使用多种方法确保彻底清除 NaN 值
        df = df.replace([np.nan, np.inf, -np.inf], None)
        df = df.where(pd.notna(df), None)
        if self._logger:
            self._logger.info("[流程] 指数日线数据预处理完成，已将 NaN 值替换为 None")
        
        records = df.to_dict(orient="records")
        
        # 二次清理：确保 records 中没有任何 NaN 值
        for record in records:
            for key, value in record.items():
                if pd.isna(value) or (isinstance(value, float) and (np.isnan(value) or np.isinf(value))):
                    record[key] = None
        
        if self._logger:
            self._logger.info("[流程] 指数日线记录级别的 NaN 值二次清理完成")

        update_field_names = [
            k for k in cols if k not in ("id", "trade_date", "ts_code", "created_at", "updated_at")
        ]

        def _do_batch(batch):
            stmt = mysql_insert(IndexDaily).values(batch)
            update_cols = {name: getattr(stmt.inserted, name) for name in update_field_names}
            ondup = stmt.on_duplicate_key_update(update_cols)
            self._session.execute(ondup)

        batch_size = 5000
        total = len(records)
        if self._logger:
            self._logger.info("[流程] 执行 index_daily 批量 upsert，总记录数=%s，批大小=%s", total, batch_size)
        for i in range(0, total, batch_size):
            batch = records[i:i + batch_size]
            _do_batch(batch)
            if self._logger:
                self._logger.info("[流程] index_daily 已写入进度：%s/%s", min(i + batch_size, total), total)

    def get_max_trade_date_index(self) -> str:
        """获取指数日线数据表中的最大交易日"""
        stmt = select(func.max(IndexDaily.trade_date))
        result = self._session.execute(stmt).scalar()
        max_date = result or None
        if self._logger:
            self._logger.info("[流程] 当前库中 index_daily 最大交易日=%s", max_date)
        return max_date
    
    def has_index_daily_data(self, trade_date: str) -> bool:
        """检查指定交易日是否已有指数日线数据"""
        stmt = select(func.count(IndexDaily.id)).where(IndexDaily.trade_date == trade_date)
        count = self._session.execute(stmt).scalar()
        has_data = count > 0
        if self._logger:
            self._logger.info("[流程] 检查交易日=%s 的指数日线数据，数据库中已有记录数=%s", trade_date, count)
        return has_data
