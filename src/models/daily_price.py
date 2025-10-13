from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, String, Date, DateTime, Float, BigInteger, UniqueConstraint, Index
from sqlalchemy.sql import func

Base = declarative_base()


class StockBasic(Base):
    __tablename__ = "stock_basic"

    ts_code = Column(String(20), primary_key=True, comment="TS代码")
    symbol = Column(String(20), nullable=False, comment="股票代码")
    name = Column(String(100), nullable=False, comment="股票名称")
    area = Column(String(100), nullable=True, comment="地域")
    industry = Column(String(100), nullable=True, comment="行业")
    market = Column(String(50), nullable=True, comment="市场类别")
    list_date = Column(String(8), nullable=True, comment="上市日期")
    list_status = Column(String(2), nullable=True, comment="上市状态 L/P/D")
    created_at = Column(DateTime, nullable=False, server_default=func.now(), comment="创建时间")
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now(), comment="更新时间")


class DailyPrice(Base):
    __tablename__ = "daily_price"
    __table_args__ = (
        UniqueConstraint('ts_code', 'trade_date', name='uq_ts_trade_date'),
        Index('idx_trade_date', 'trade_date'),
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    ts_code = Column(String(20), nullable=False, comment="TS代码")
    trade_date = Column(String(8), nullable=False, comment="交易日期 YYYYMMDD")
    open = Column(Float, nullable=True)
    high = Column(Float, nullable=True)
    low = Column(Float, nullable=True)
    close = Column(Float, nullable=True)
    pre_close = Column(Float, nullable=True)
    change = Column(Float, nullable=True)
    pct_chg = Column(Float, nullable=True)
    vol = Column(Float, nullable=True)
    amount = Column(Float, nullable=True)
    created_at = Column(DateTime, nullable=False, server_default=func.now(), comment="创建时间")
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now(), comment="更新时间")


class DailyBasic(Base):
    __tablename__ = "daily_basic"
    __table_args__ = (
        UniqueConstraint('ts_code', 'trade_date', name='uq_ts_code_trade_date'),
        Index('idx_trade_date', 'trade_date'),
        Index('idx_ts_code', 'ts_code'),
        Index('idx_pe', 'pe'),
        Index('idx_pb', 'pb'),
        Index('idx_total_mv', 'total_mv'),
        Index('idx_turnover_rate', 'turnover_rate'),
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    ts_code = Column(String(20), nullable=False, comment="TS代码")
    trade_date = Column(String(8), nullable=False, comment="交易日期 YYYYMMDD")
    close = Column(Float, nullable=True, comment="当日收盘价")
    turnover_rate = Column(Float, nullable=True, comment="换手率（%）")
    turnover_rate_f = Column(Float, nullable=True, comment="换手率（自由流通股）（%）")
    volume_ratio = Column(Float, nullable=True, comment="量比")
    pe = Column(Float, nullable=True, comment="市盈率（总市值/净利润）")
    pe_ttm = Column(Float, nullable=True, comment="市盈率（TTM滚动12个月）")
    pb = Column(Float, nullable=True, comment="市净率（总市值/净资产）")
    ps = Column(Float, nullable=True, comment="市销率")
    ps_ttm = Column(Float, nullable=True, comment="市销率（TTM滚动12个月）")
    dv_ratio = Column(Float, nullable=True, comment="股息率（%）")
    dv_ttm = Column(Float, nullable=True, comment="股息率（TTM）（%）")
    total_share = Column(Float, nullable=True, comment="总股本（万股）")
    float_share = Column(Float, nullable=True, comment="流通股本（万股）")
    free_share = Column(Float, nullable=True, comment="自由流通股本（万股）")
    total_mv = Column(Float, nullable=True, comment="总市值（万元）")
    circ_mv = Column(Float, nullable=True, comment="流通市值（万元）")
    created_at = Column(DateTime, nullable=False, server_default=func.now(), comment="创建时间")
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now(), comment="更新时间")


class IndexBasic(Base):
    __tablename__ = "index_basic"

    ts_code = Column(String(20), primary_key=True, comment="指数代码")
    name = Column(String(100), nullable=False, comment="指数名称")
    market = Column(String(50), nullable=True, comment="市场")
    publisher = Column(String(50), nullable=True, comment="发布方")
    index_type = Column(String(50), nullable=True, comment="指数类别")
    category = Column(String(50), nullable=True, comment="指数分类")
    base_date = Column(String(8), nullable=True, comment="基期")
    base_point = Column(Float, nullable=True, comment="基点")
    list_date = Column(String(8), nullable=True, comment="发布日期")
    created_at = Column(DateTime, nullable=False, server_default=func.now(), comment="创建时间")
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now(), comment="更新时间")


class IndexDaily(Base):
    __tablename__ = "index_daily"
    __table_args__ = (
        UniqueConstraint('ts_code', 'trade_date', name='uq_index_ts_trade_date'),
        Index('idx_index_trade_date', 'trade_date'),
        Index('idx_index_ts_code', 'ts_code'),
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    ts_code = Column(String(20), nullable=False, comment="指数代码")
    trade_date = Column(String(8), nullable=False, comment="交易日期 YYYYMMDD")
    open = Column(Float, nullable=True, comment="开盘点位")
    high = Column(Float, nullable=True, comment="最高点位")
    low = Column(Float, nullable=True, comment="最低点位")
    close = Column(Float, nullable=True, comment="收盘点位")
    pre_close = Column(Float, nullable=True, comment="昨收盘")
    change = Column(Float, nullable=True, comment="涨跌点")
    pct_chg = Column(Float, nullable=True, comment="涨跌幅")
    vol = Column(Float, nullable=True, comment="成交量（手）")
    amount = Column(Float, nullable=True, comment="成交额（千元）")
    created_at = Column(DateTime, nullable=False, server_default=func.now(), comment="创建时间")
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now(), comment="更新时间")
