from datetime import datetime

from sqlalchemy import DateTime, Float, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base


class StockBasic(Base):
    __tablename__ = "stock_basic"
    ts_code: Mapped[str] = mapped_column(String(16), primary_key=True)
    symbol: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(64), nullable=False)
    area: Mapped[str | None] = mapped_column(String(32), nullable=True)
    industry: Mapped[str | None] = mapped_column(String(64), nullable=True)
    market: Mapped[str | None] = mapped_column(String(16), nullable=True)
    list_status: Mapped[str] = mapped_column(String(2), nullable=False, index=True)
    list_date: Mapped[str | None] = mapped_column(String(8), nullable=True)
    delist_date: Mapped[str | None] = mapped_column(String(8), nullable=True)
    is_hs: Mapped[str | None] = mapped_column(String(8), nullable=True)


class TradeCalendar(Base):
    __tablename__ = "trade_calendar"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    exchange: Mapped[str] = mapped_column(String(8), nullable=False, index=True)
    cal_date: Mapped[str] = mapped_column(String(8), nullable=False, index=True)
    is_open: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    pretrade_date: Mapped[str | None] = mapped_column(String(8), nullable=True)
    __table_args__ = (UniqueConstraint("exchange", "cal_date", name="uq_trade_calendar_exchange_cal_date"),)


class StockDaily(Base):
    __tablename__ = "stock_daily"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ts_code: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    trade_date: Mapped[str] = mapped_column(String(8), nullable=False, index=True)
    open: Mapped[float | None] = mapped_column(Float, nullable=True)
    high: Mapped[float | None] = mapped_column(Float, nullable=True)
    low: Mapped[float | None] = mapped_column(Float, nullable=True)
    close: Mapped[float | None] = mapped_column(Float, nullable=True)
    pre_close: Mapped[float | None] = mapped_column(Float, nullable=True)
    change: Mapped[float | None] = mapped_column(Float, nullable=True)
    pct_chg: Mapped[float | None] = mapped_column(Float, nullable=True)
    vol: Mapped[float | None] = mapped_column(Float, nullable=True)
    amount: Mapped[float | None] = mapped_column(Float, nullable=True)
    __table_args__ = (UniqueConstraint("ts_code", "trade_date", name="uq_stock_daily_ts_code_trade_date"),)


class JobRun(Base):
    __tablename__ = "job_run"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_name: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    inserted: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    updated: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    replay_of_job_run_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    job_payload: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    error_message: Mapped[str | None] = mapped_column(String(512), nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow, index=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class JobLock(Base):
    __tablename__ = "job_lock"
    job_name: Mapped[str] = mapped_column(String(64), primary_key=True)
    owner: Mapped[str] = mapped_column(String(64), nullable=False)
    locked_until: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)


class IndexDaily(Base):
    __tablename__ = "index_daily"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ts_code: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    trade_date: Mapped[str] = mapped_column(String(8), nullable=False, index=True)
    close: Mapped[float | None] = mapped_column(Float, nullable=True)
    open: Mapped[float | None] = mapped_column(Float, nullable=True)
    high: Mapped[float | None] = mapped_column(Float, nullable=True)
    low: Mapped[float | None] = mapped_column(Float, nullable=True)
    pre_close: Mapped[float | None] = mapped_column(Float, nullable=True)
    change: Mapped[float | None] = mapped_column(Float, nullable=True)
    pct_chg: Mapped[float | None] = mapped_column(Float, nullable=True)
    vol: Mapped[float | None] = mapped_column(Float, nullable=True)
    amount: Mapped[float | None] = mapped_column(Float, nullable=True)
    __table_args__ = (UniqueConstraint("ts_code", "trade_date", name="uq_index_daily_ts_code_trade_date"),)


class IndustryBoard(Base):
    __tablename__ = "industry_board"
    index_code: Mapped[str] = mapped_column(String(16), primary_key=True)
    industry_name: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    level: Mapped[str | None] = mapped_column(String(16), nullable=True)
    industry_code: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    src: Mapped[str | None] = mapped_column(String(16), nullable=True)


class IndustryBoardMember(Base):
    __tablename__ = "industry_board_member"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    index_code: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    con_code: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    con_name: Mapped[str | None] = mapped_column(String(64), nullable=True)
    in_date: Mapped[str | None] = mapped_column(String(8), nullable=True)
    out_date: Mapped[str | None] = mapped_column(String(8), nullable=True)
    is_new: Mapped[str | None] = mapped_column(String(4), nullable=True)
    __table_args__ = (UniqueConstraint("index_code", "con_code", "in_date", name="uq_industry_member"),)


class Moneyflow(Base):
    __tablename__ = "moneyflow"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ts_code: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    trade_date: Mapped[str] = mapped_column(String(8), nullable=False, index=True)
    buy_sm_vol: Mapped[float | None] = mapped_column(Float, nullable=True)
    buy_sm_amount: Mapped[float | None] = mapped_column(Float, nullable=True)
    sell_sm_vol: Mapped[float | None] = mapped_column(Float, nullable=True)
    sell_sm_amount: Mapped[float | None] = mapped_column(Float, nullable=True)
    buy_md_vol: Mapped[float | None] = mapped_column(Float, nullable=True)
    buy_md_amount: Mapped[float | None] = mapped_column(Float, nullable=True)
    sell_md_vol: Mapped[float | None] = mapped_column(Float, nullable=True)
    sell_md_amount: Mapped[float | None] = mapped_column(Float, nullable=True)
    buy_lg_vol: Mapped[float | None] = mapped_column(Float, nullable=True)
    buy_lg_amount: Mapped[float | None] = mapped_column(Float, nullable=True)
    sell_lg_vol: Mapped[float | None] = mapped_column(Float, nullable=True)
    sell_lg_amount: Mapped[float | None] = mapped_column(Float, nullable=True)
    buy_elg_vol: Mapped[float | None] = mapped_column(Float, nullable=True)
    buy_elg_amount: Mapped[float | None] = mapped_column(Float, nullable=True)
    sell_elg_vol: Mapped[float | None] = mapped_column(Float, nullable=True)
    sell_elg_amount: Mapped[float | None] = mapped_column(Float, nullable=True)
    net_mf_vol: Mapped[float | None] = mapped_column(Float, nullable=True)
    net_mf_amount: Mapped[float | None] = mapped_column(Float, nullable=True)
    __table_args__ = (UniqueConstraint("ts_code", "trade_date", name="uq_moneyflow_ts_code_trade_date"),)


class AdjFactor(Base):
    __tablename__ = "adj_factor"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ts_code: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    trade_date: Mapped[str] = mapped_column(String(8), nullable=False, index=True)
    adj_factor: Mapped[float | None] = mapped_column(Float, nullable=True)
    __table_args__ = (UniqueConstraint("ts_code", "trade_date", name="uq_adj_factor_ts_code_trade_date"),)
