from typing import List
import pandas as pd
from sqlalchemy import insert
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.orm import Session
from sqlalchemy import select, func

from src.models.daily_price import StockBasic, DailyPrice


class DailyRepository:
    def __init__(self, session: Session, logger=None):
        self._session = session
        self._logger = logger

    def upsert_stock_basic(self, df: pd.DataFrame):
        if df is None or df.empty:
            if self._logger:
                self._logger.info("[流程] 股票基础信息数据为空，跳过 upsert")
            return
        records = df.to_dict(orient="records")
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
        records = df.to_dict(orient="records")

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
