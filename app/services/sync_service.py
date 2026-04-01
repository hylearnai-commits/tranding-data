from dataclasses import dataclass
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import and_, func, select
from sqlalchemy.orm import Session

from app.models import AdjFactor, IndexDaily, IndustryBoard, IndustryBoardMember, Moneyflow, StockBasic, StockDaily, TradeCalendar
from app.services.tushare_client import TushareClient

DEFAULT_INDEX_CODES = ["000001.SH", "399001.SZ", "399006.SZ", "000300.SH", "000905.SH"]


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


@dataclass
class BackfillReport:
    checked_trade_days: int
    stock_missing_days: list[str]
    index_missing_days: list[str]
    moneyflow_missing_days: list[str]
    stock_result: SyncCounter
    index_result: SyncCounter
    moneyflow_result: SyncCounter


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


def _upsert_index_daily_row(db: Session, row: pd.Series) -> tuple[int, int]:
    ts_code = _value(row.get("ts_code"))
    trade_date = _value(row.get("trade_date"))
    if not ts_code or not trade_date:
        return 0, 0
    existing = db.execute(
        select(IndexDaily).where(IndexDaily.ts_code == ts_code, IndexDaily.trade_date == trade_date)
    ).scalar_one_or_none()
    data = {
        "close": _value(row.get("close")),
        "open": _value(row.get("open")),
        "high": _value(row.get("high")),
        "low": _value(row.get("low")),
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
    db.add(IndexDaily(ts_code=ts_code, trade_date=trade_date, **data))
    return 1, 0


def _upsert_moneyflow_row(db: Session, row: pd.Series) -> tuple[int, int]:
    ts_code = _value(row.get("ts_code"))
    trade_date = _value(row.get("trade_date"))
    if not ts_code or not trade_date:
        return 0, 0
    existing = db.execute(
        select(Moneyflow).where(Moneyflow.ts_code == ts_code, Moneyflow.trade_date == trade_date)
    ).scalar_one_or_none()
    data = {
        "buy_sm_vol": _value(row.get("buy_sm_vol")),
        "buy_sm_amount": _value(row.get("buy_sm_amount")),
        "sell_sm_vol": _value(row.get("sell_sm_vol")),
        "sell_sm_amount": _value(row.get("sell_sm_amount")),
        "buy_md_vol": _value(row.get("buy_md_vol")),
        "buy_md_amount": _value(row.get("buy_md_amount")),
        "sell_md_vol": _value(row.get("sell_md_vol")),
        "sell_md_amount": _value(row.get("sell_md_amount")),
        "buy_lg_vol": _value(row.get("buy_lg_vol")),
        "buy_lg_amount": _value(row.get("buy_lg_amount")),
        "sell_lg_vol": _value(row.get("sell_lg_vol")),
        "sell_lg_amount": _value(row.get("sell_lg_amount")),
        "buy_elg_vol": _value(row.get("buy_elg_vol")),
        "buy_elg_amount": _value(row.get("buy_elg_amount")),
        "sell_elg_vol": _value(row.get("sell_elg_vol")),
        "sell_elg_amount": _value(row.get("sell_elg_amount")),
        "net_mf_vol": _value(row.get("net_mf_vol")),
        "net_mf_amount": _value(row.get("net_mf_amount")),
    }
    if existing:
        for k, v in data.items():
            setattr(existing, k, v)
        return 0, 1
    db.add(Moneyflow(ts_code=ts_code, trade_date=trade_date, **data))
    return 1, 0


def _upsert_adj_factor_row(db: Session, row: pd.Series) -> tuple[int, int]:
    ts_code = _value(row.get("ts_code"))
    trade_date = _value(row.get("trade_date"))
    if not ts_code or not trade_date:
        return 0, 0
    existing = db.execute(
        select(AdjFactor).where(AdjFactor.ts_code == ts_code, AdjFactor.trade_date == trade_date)
    ).scalar_one_or_none()
    data = {"adj_factor": _value(row.get("adj_factor"))}
    if existing:
        existing.adj_factor = data["adj_factor"]
        return 0, 1
    db.add(AdjFactor(ts_code=ts_code, trade_date=trade_date, **data))
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


def sync_index_daily(db: Session, ts_code: str, start_date: str, end_date: str) -> SyncCounter:
    client = TushareClient()
    df = client.fetch_index_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
    counter = SyncCounter()
    for _, row in df.iterrows():
        inserted, updated = _upsert_index_daily_row(db, row)
        counter.inserted += inserted
        counter.updated += updated
    db.commit()
    return counter


def sync_index_daily_by_date(db: Session, trade_date: str) -> SyncCounter:
    client = TushareClient()
    existing_codes = db.execute(select(func.distinct(IndexDaily.ts_code))).scalars().all()
    index_codes = list(dict.fromkeys(DEFAULT_INDEX_CODES + existing_codes))
    counter = SyncCounter()
    for ts_code in index_codes:
        df = client.fetch_index_daily(ts_code=ts_code, start_date=trade_date, end_date=trade_date)
        for _, row in df.iterrows():
            inserted, updated = _upsert_index_daily_row(db, row)
            counter.inserted += inserted
            counter.updated += updated
    db.commit()
    return counter


def sync_industry_boards(db: Session, src: str = "SW") -> SyncCounter:
    client = TushareClient()
    df = client.fetch_industry_board_list(src=src)
    counter = SyncCounter()
    for _, row in df.iterrows():
        index_code = _value(row.get("index_code"))
        if not index_code:
            continue
        existing = db.get(IndustryBoard, index_code)
        data = {
            "industry_name": _value(row.get("industry_name")),
            "level": _value(row.get("level")),
            "industry_code": _value(row.get("industry_code")),
            "src": _value(row.get("src")) or src,
        }
        if existing:
            for k, v in data.items():
                setattr(existing, k, v)
            counter.updated += 1
        else:
            db.add(IndustryBoard(index_code=index_code, **data))
            counter.inserted += 1
    db.commit()
    return counter


def sync_industry_board_members(db: Session, index_code: str, src: str = "SW") -> SyncCounter:
    client = TushareClient()
    df = client.fetch_industry_board_members(index_code=index_code, src=src)
    counter = SyncCounter()
    for _, row in df.iterrows():
        con_code = _value(row.get("con_code"))
        in_date = _value(row.get("in_date"))
        if not con_code:
            continue
        existing = db.execute(
            select(IndustryBoardMember).where(
                IndustryBoardMember.index_code == index_code,
                IndustryBoardMember.con_code == con_code,
                IndustryBoardMember.in_date == in_date,
            )
        ).scalar_one_or_none()
        data = {
            "con_name": _value(row.get("con_name")),
            "out_date": _value(row.get("out_date")),
            "is_new": _value(row.get("is_new")),
        }
        if existing:
            for k, v in data.items():
                setattr(existing, k, v)
            counter.updated += 1
        else:
            db.add(IndustryBoardMember(index_code=index_code, con_code=con_code, in_date=in_date, **data))
            counter.inserted += 1
    db.commit()
    return counter


def sync_industry_board_members_all(db: Session, src: str = "SW") -> SyncCounter:
    boards = db.execute(select(IndustryBoard.index_code).where(IndustryBoard.src == src)).scalars().all()
    counter = SyncCounter()
    for index_code in boards:
        result = sync_industry_board_members(db=db, index_code=index_code, src=src)
        counter.inserted += result.inserted
        counter.updated += result.updated
    return counter


def sync_moneyflow(db: Session, ts_code: str, start_date: str, end_date: str) -> SyncCounter:
    client = TushareClient()
    df = client.fetch_moneyflow(ts_code=ts_code, start_date=start_date, end_date=end_date)
    counter = SyncCounter()
    for _, row in df.iterrows():
        inserted, updated = _upsert_moneyflow_row(db, row)
        counter.inserted += inserted
        counter.updated += updated
    db.commit()
    return counter


def sync_moneyflow_by_date(db: Session, trade_date: str) -> SyncCounter:
    client = TushareClient()
    df = client.fetch_moneyflow_by_date(trade_date=trade_date)
    counter = SyncCounter()
    for _, row in df.iterrows():
        inserted, updated = _upsert_moneyflow_row(db, row)
        counter.inserted += inserted
        counter.updated += updated
    db.commit()
    return counter


def sync_index_daily_incremental(db: Session, exchange: str = "SSE", lookback_days: int = 3) -> SyncCounter:
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
        result = sync_index_daily_by_date(db=db, trade_date=trade_date)
        counter.inserted += result.inserted
        counter.updated += result.updated
    return counter


def sync_moneyflow_incremental(db: Session, exchange: str = "SSE", lookback_days: int = 3) -> SyncCounter:
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
        result = sync_moneyflow_by_date(db=db, trade_date=trade_date)
        counter.inserted += result.inserted
        counter.updated += result.updated
    return counter


def sync_adj_factor(db: Session, ts_code: str, start_date: str, end_date: str) -> SyncCounter:
    client = TushareClient()
    df = client.fetch_adj_factor(ts_code=ts_code, start_date=start_date, end_date=end_date)
    counter = SyncCounter()
    for _, row in df.iterrows():
        inserted, updated = _upsert_adj_factor_row(db, row)
        counter.inserted += inserted
        counter.updated += updated
    db.commit()
    return counter


def sync_adj_factor_by_date(db: Session, trade_date: str) -> SyncCounter:
    client = TushareClient()
    df = client.fetch_adj_factor_by_date(trade_date=trade_date)
    counter = SyncCounter()
    for _, row in df.iterrows():
        inserted, updated = _upsert_adj_factor_row(db, row)
        counter.inserted += inserted
        counter.updated += updated
    db.commit()
    return counter


def _missing_trade_dates_for_table(db: Session, exchange: str, lookback_days: int, date_column) -> tuple[list[str], int]:
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
    existing_days = db.execute(
        select(func.distinct(date_column)).where(date_column >= start_date, date_column <= end_date)
    ).scalars().all()
    missing = [d for d in trade_days if d not in set(existing_days)]
    return missing, len(trade_days)


def auto_backfill_recent(
    db: Session, exchange: str = "SSE", lookback_days: int = 10, max_backfill_days: int = 5
) -> BackfillReport:
    stock_missing, checked = _missing_trade_dates_for_table(
        db=db, exchange=exchange, lookback_days=lookback_days, date_column=StockDaily.trade_date
    )
    index_missing, _ = _missing_trade_dates_for_table(
        db=db, exchange=exchange, lookback_days=lookback_days, date_column=IndexDaily.trade_date
    )
    moneyflow_missing, _ = _missing_trade_dates_for_table(
        db=db, exchange=exchange, lookback_days=lookback_days, date_column=Moneyflow.trade_date
    )
    stock_counter = SyncCounter()
    for trade_date in stock_missing[:max_backfill_days]:
        c = sync_stock_daily_by_date(db=db, trade_date=trade_date)
        stock_counter.inserted += c.inserted
        stock_counter.updated += c.updated
    index_counter = SyncCounter()
    for trade_date in index_missing[:max_backfill_days]:
        c = sync_index_daily_by_date(db=db, trade_date=trade_date)
        index_counter.inserted += c.inserted
        index_counter.updated += c.updated
    moneyflow_counter = SyncCounter()
    for trade_date in moneyflow_missing[:max_backfill_days]:
        c = sync_moneyflow_by_date(db=db, trade_date=trade_date)
        moneyflow_counter.inserted += c.inserted
        moneyflow_counter.updated += c.updated
    return BackfillReport(
        checked_trade_days=checked,
        stock_missing_days=stock_missing,
        index_missing_days=index_missing,
        moneyflow_missing_days=moneyflow_missing,
        stock_result=stock_counter,
        index_result=index_counter,
        moneyflow_result=moneyflow_counter,
    )
