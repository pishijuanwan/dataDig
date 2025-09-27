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
