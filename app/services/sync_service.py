from dataclasses import dataclass
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import and_, func, select
from sqlalchemy.orm import Session

from app.models import StockBasic, StockDaily, TradeCalendar
from app.services.tushare_client import TushareClient


@dataclass
class SyncCounter:
    inserted: int = 0
    updated: int = 0


@dataclass
class QualityReport:
    start_date: str
    end_date: str
    exchange: str
    expected_trade_days: int
    existing_trade_days: int
    missing_trade_days: int
    invalid_price_rows: int


def _value(v):
    if pd.isna(v):
        return None
    return v


def _upsert_stock_daily_row(db: Session, row: pd.Series) -> tuple[int, int]:
    ts_code = _value(row.get("ts_code"))
    trade_date = _value(row.get("trade_date"))
    if not ts_code or not trade_date:
        return 0, 0
    existing = db.execute(
        select(StockDaily).where(StockDaily.ts_code == ts_code, StockDaily.trade_date == trade_date)
    ).scalar_one_or_none()
    data = {
        "open": _value(row.get("open")),
        "high": _value(row.get("high")),
        "low": _value(row.get("low")),
        "close": _value(row.get("close")),
        "pre_close": _value(row.get("pre_close")),
        "change": _value(row.get("change")),
        "pct_chg": _value(row.get("pct_chg")),
        "vol": _value(row.get("vol")),
        "amount": _value(row.get("amount")),
    }
    if existing:
        for k, v in data.items():
            setattr(existing, k, v)
        return 0, 1
    db.add(StockDaily(ts_code=ts_code, trade_date=trade_date, **data))
    return 1, 0


def sync_stock_basic(db: Session) -> SyncCounter:
    client = TushareClient()
    df = client.fetch_stock_basic()
    counter = SyncCounter()
    for _, row in df.iterrows():
        ts_code = _value(row.get("ts_code"))
        if not ts_code:
            continue
        existing = db.get(StockBasic, ts_code)
        data = {
            "symbol": _value(row.get("symbol")),
            "name": _value(row.get("name")),
            "area": _value(row.get("area")),
            "industry": _value(row.get("industry")),
            "market": _value(row.get("market")),
            "list_status": _value(row.get("list_status")) or "L",
            "list_date": _value(row.get("list_date")),
            "delist_date": _value(row.get("delist_date")),
            "is_hs": _value(row.get("is_hs")),
        }
        if existing:
            for k, v in data.items():
                setattr(existing, k, v)
            counter.updated += 1
        else:
            db.add(StockBasic(ts_code=ts_code, **data))
            counter.inserted += 1
    db.commit()
    return counter


def sync_trade_calendar(db: Session, exchange: str = "SSE", days_back: int = 30, days_forward: int = 365) -> SyncCounter:
    client = TushareClient()
    start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y%m%d")
    end_date = (datetime.now() + timedelta(days=days_forward)).strftime("%Y%m%d")
    df = client.fetch_trade_calendar(exchange=exchange, start_date=start_date, end_date=end_date)
    counter = SyncCounter()
    for _, row in df.iterrows():
        cal_date = _value(row.get("cal_date"))
        if not cal_date:
            continue
        existing = db.execute(
            select(TradeCalendar).where(TradeCalendar.exchange == exchange, TradeCalendar.cal_date == cal_date)
        ).scalar_one_or_none()
        data = {
            "is_open": int(_value(row.get("is_open")) or 0),
            "pretrade_date": _value(row.get("pretrade_date")),
        }
        if existing:
            existing.is_open = data["is_open"]
            existing.pretrade_date = data["pretrade_date"]
            counter.updated += 1
        else:
            db.add(TradeCalendar(exchange=exchange, cal_date=cal_date, **data))
            counter.inserted += 1
    db.commit()
    return counter


def sync_stock_daily(db: Session, ts_code: str, start_date: str, end_date: str) -> SyncCounter:
    client = TushareClient()
    df = client.fetch_stock_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
    counter = SyncCounter()
    for _, row in df.iterrows():
        inserted, updated = _upsert_stock_daily_row(db, row)
        counter.inserted += inserted
        counter.updated += updated
    db.commit()
    return counter


def sync_stock_daily_by_date(db: Session, trade_date: str) -> SyncCounter:
    client = TushareClient()
    df = client.fetch_stock_daily_by_date(trade_date=trade_date)
    counter = SyncCounter()
    for _, row in df.iterrows():
        inserted, updated = _upsert_stock_daily_row(db, row)
        counter.inserted += inserted
        counter.updated += updated
    db.commit()
    return counter


def sync_stock_daily_incremental(db: Session, exchange: str = "SSE", lookback_days: int = 3) -> SyncCounter:
    start_date = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y%m%d")
    end_date = datetime.now().strftime("%Y%m%d")
    trade_days = db.execute(
        select(TradeCalendar.cal_date)
        .where(
            TradeCalendar.exchange == exchange,
            TradeCalendar.is_open == 1,
            TradeCalendar.cal_date >= start_date,
            TradeCalendar.cal_date <= end_date,
        )
        .order_by(TradeCalendar.cal_date.asc())
    ).scalars().all()
    counter = SyncCounter()
    for trade_date in trade_days:
        result = sync_stock_daily_by_date(db=db, trade_date=trade_date)
        counter.inserted += result.inserted
        counter.updated += result.updated
    return counter


def check_stock_daily_quality(db: Session, start_date: str, end_date: str, exchange: str = "SSE") -> QualityReport:
    expected_trade_days = db.execute(
        select(func.count())
        .select_from(TradeCalendar)
        .where(
            TradeCalendar.exchange == exchange,
            TradeCalendar.is_open == 1,
            TradeCalendar.cal_date >= start_date,
            TradeCalendar.cal_date <= end_date,
        )
    ).scalar_one()
    existing_trade_days = db.execute(
        select(func.count(func.distinct(StockDaily.trade_date))).where(
            StockDaily.trade_date >= start_date,
            StockDaily.trade_date <= end_date,
        )
    ).scalar_one()
    invalid_price_rows = db.execute(
        select(func.count())
        .select_from(StockDaily)
        .where(
            and_(
                StockDaily.trade_date >= start_date,
                StockDaily.trade_date <= end_date,
                (
                    (StockDaily.high.is_not(None) & StockDaily.low.is_not(None) & (StockDaily.high < StockDaily.low))
                    | (StockDaily.open.is_not(None) & (StockDaily.open < 0))
                    | (StockDaily.high.is_not(None) & (StockDaily.high < 0))
                    | (StockDaily.low.is_not(None) & (StockDaily.low < 0))
                    | (StockDaily.close.is_not(None) & (StockDaily.close < 0))
                ),
            )
        )
    ).scalar_one()
    missing_trade_days = max(expected_trade_days - existing_trade_days, 0)
    return QualityReport(
        start_date=start_date,
        end_date=end_date,
        exchange=exchange,
        expected_trade_days=expected_trade_days,
        existing_trade_days=existing_trade_days,
        missing_trade_days=missing_trade_days,
        invalid_price_rows=invalid_price_rows,
    )
