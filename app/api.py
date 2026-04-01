import json

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db import get_db
from app.errors import raise_api_error
from app.models import AdjFactor, IndexDaily, IndustryBoard, IndustryBoardMember, JobRun, Moneyflow, StockBasic, StockDaily, TradeCalendar
from app.observability import snapshot_metrics
from app.schemas import (
    AdjFactorOut,
    IndexDailyOut,
    IndustryBoardMemberOut,
    IndustryBoardOut,
    JobRunOut,
    JobRunPageOut,
    MoneyflowOut,
    QualityReportOut,
    StockBasicOut,
    StockDailyAdjustedOut,
    StockDailyOut,
    SyncResult,
    TradeCalendarOut,
)
from app.services.job_service import execute_sync_job, get_job_run_by_id, list_job_runs_page
from app.services.sync_service import (
    SyncCounter,
    auto_backfill_recent,
    check_stock_daily_quality,
    sync_adj_factor,
    sync_adj_factor_by_date,
    sync_index_daily,
    sync_index_daily_by_date,
    sync_industry_board_members,
    sync_industry_board_members_all,
    sync_industry_boards,
    sync_moneyflow,
    sync_moneyflow_by_date,
    sync_stock_basic,
    sync_stock_daily,
    sync_stock_daily_by_date,
    sync_stock_daily_incremental,
    sync_trade_calendar,
)


router = APIRouter()


def _serialize_job_run(r: JobRun) -> dict:
    return {
        "id": r.id,
        "job_name": r.job_name,
        "status": r.status,
        "attempts": r.attempts,
        "inserted": r.inserted,
        "updated": r.updated,
        "replay_of_job_run_id": r.replay_of_job_run_id,
        "job_payload": r.job_payload,
        "error_message": r.error_message,
        "started_at": r.started_at.isoformat(),
        "finished_at": r.finished_at.isoformat() if r.finished_at else None,
    }


def _raise_sync_error(e: Exception):
    if isinstance(e, RuntimeError):
        raise_api_error(code="JOB_LOCK_CONFLICT", message=str(e))
    if isinstance(e, ValueError):
        raise_api_error(code="INVALID_ARGUMENT", message=str(e))
    raise_api_error(code="INTERNAL_ERROR", message=str(e))


def _replay_job(db: Session, source: JobRun):
    payload = json.loads(source.job_payload) if source.job_payload else {}
    if source.job_name.startswith("sync_stock_basic"):
        return execute_sync_job(
            db=db,
            job_name=f"{source.job_name}_replay",
            runner=lambda: sync_stock_basic(db),
            max_retries=1,
            replay_of_job_run_id=source.id,
            job_payload=payload,
        )
    if source.job_name.startswith("sync_trade_calendar"):
        exchange = payload.get("exchange", "SSE")
        return execute_sync_job(
            db=db,
            job_name=f"sync_trade_calendar_{exchange}_replay",
            runner=lambda: sync_trade_calendar(db, exchange=exchange),
            max_retries=1,
            replay_of_job_run_id=source.id,
            job_payload={"exchange": exchange},
        )
    if source.job_name.startswith("sync_stock_daily_by_date"):
        trade_date = payload.get("trade_date")
        if not trade_date:
            raise ValueError("源任务缺少trade_date，无法重放")
        return execute_sync_job(
            db=db,
            job_name=f"sync_stock_daily_by_date_{trade_date}_replay",
            runner=lambda: sync_stock_daily_by_date(db, trade_date=trade_date),
            max_retries=1,
            replay_of_job_run_id=source.id,
            job_payload={"trade_date": trade_date},
        )
    if source.job_name.startswith("sync_stock_daily_incremental"):
        exchange = payload.get("exchange", "SSE")
        lookback_days = int(payload.get("lookback_days", 3))
        return execute_sync_job(
            db=db,
            job_name=f"sync_stock_daily_incremental_{exchange}_replay",
            runner=lambda: sync_stock_daily_incremental(db, exchange=exchange, lookback_days=lookback_days),
            max_retries=1,
            replay_of_job_run_id=source.id,
            job_payload={"exchange": exchange, "lookback_days": lookback_days},
        )
    if source.job_name.startswith("sync_stock_daily_"):
        ts_code = payload.get("ts_code")
        start_date = payload.get("start_date")
        end_date = payload.get("end_date")
        if not ts_code or not start_date or not end_date:
            raise ValueError("源任务缺少ts_code/start_date/end_date，无法重放")
        return execute_sync_job(
            db=db,
            job_name=f"sync_stock_daily_{ts_code}_replay",
            runner=lambda: sync_stock_daily(db, ts_code=ts_code, start_date=start_date, end_date=end_date),
            max_retries=1,
            replay_of_job_run_id=source.id,
            job_payload={"ts_code": ts_code, "start_date": start_date, "end_date": end_date},
        )
    if source.job_name.startswith("sync_index_daily_by_date"):
        trade_date = payload.get("trade_date")
        if not trade_date:
            raise ValueError("源任务缺少trade_date，无法重放")
        return execute_sync_job(
            db=db,
            job_name=f"sync_index_daily_by_date_{trade_date}_replay",
            runner=lambda: sync_index_daily_by_date(db, trade_date=trade_date),
            max_retries=1,
            replay_of_job_run_id=source.id,
            job_payload={"trade_date": trade_date},
        )
    if source.job_name.startswith("sync_index_daily_"):
        ts_code = payload.get("ts_code")
        start_date = payload.get("start_date")
        end_date = payload.get("end_date")
        if not ts_code or not start_date or not end_date:
            raise ValueError("源任务缺少ts_code/start_date/end_date，无法重放")
        return execute_sync_job(
            db=db,
            job_name=f"sync_index_daily_{ts_code}_replay",
            runner=lambda: sync_index_daily(db, ts_code=ts_code, start_date=start_date, end_date=end_date),
            max_retries=1,
            replay_of_job_run_id=source.id,
            job_payload={"ts_code": ts_code, "start_date": start_date, "end_date": end_date},
        )
    if source.job_name.startswith("sync_industry_boards"):
        src = payload.get("src", "SW")
        return execute_sync_job(
            db=db,
            job_name=f"sync_industry_boards_{src}_replay",
            runner=lambda: sync_industry_boards(db, src=src),
            max_retries=1,
            replay_of_job_run_id=source.id,
            job_payload={"src": src},
        )
    if source.job_name.startswith("sync_industry_members_all"):
        src = payload.get("src", "SW")
        return execute_sync_job(
            db=db,
            job_name=f"sync_industry_members_all_{src}_replay",
            runner=lambda: sync_industry_board_members_all(db, src=src),
            max_retries=1,
            replay_of_job_run_id=source.id,
            job_payload={"src": src},
        )
    if source.job_name.startswith("sync_industry_members"):
        index_code = payload.get("index_code")
        src = payload.get("src", "SW")
        if not index_code:
            raise ValueError("源任务缺少index_code，无法重放")
        return execute_sync_job(
            db=db,
            job_name=f"sync_industry_members_{index_code}_replay",
            runner=lambda: sync_industry_board_members(db, index_code=index_code, src=src),
            max_retries=1,
            replay_of_job_run_id=source.id,
            job_payload={"index_code": index_code, "src": src},
        )
    if source.job_name.startswith("sync_moneyflow_by_date"):
        trade_date = payload.get("trade_date")
        if not trade_date:
            raise ValueError("源任务缺少trade_date，无法重放")
        return execute_sync_job(
            db=db,
            job_name=f"sync_moneyflow_by_date_{trade_date}_replay",
            runner=lambda: sync_moneyflow_by_date(db, trade_date=trade_date),
            max_retries=1,
            replay_of_job_run_id=source.id,
            job_payload={"trade_date": trade_date},
        )
    if source.job_name.startswith("sync_moneyflow_"):
        ts_code = payload.get("ts_code")
        start_date = payload.get("start_date")
        end_date = payload.get("end_date")
        if not ts_code or not start_date or not end_date:
            raise ValueError("源任务缺少ts_code/start_date/end_date，无法重放")
        return execute_sync_job(
            db=db,
            job_name=f"sync_moneyflow_{ts_code}_replay",
            runner=lambda: sync_moneyflow(db, ts_code=ts_code, start_date=start_date, end_date=end_date),
            max_retries=1,
            replay_of_job_run_id=source.id,
            job_payload={"ts_code": ts_code, "start_date": start_date, "end_date": end_date},
        )
    if source.job_name.startswith("sync_adj_factor_by_date"):
        trade_date = payload.get("trade_date")
        if not trade_date:
            raise ValueError("源任务缺少trade_date，无法重放")
        return execute_sync_job(
            db=db,
            job_name=f"sync_adj_factor_by_date_{trade_date}_replay",
            runner=lambda: sync_adj_factor_by_date(db, trade_date=trade_date),
            max_retries=1,
            replay_of_job_run_id=source.id,
            job_payload={"trade_date": trade_date},
        )
    if source.job_name.startswith("sync_adj_factor_"):
        ts_code = payload.get("ts_code")
        start_date = payload.get("start_date")
        end_date = payload.get("end_date")
        if not ts_code or not start_date or not end_date:
            raise ValueError("源任务缺少ts_code/start_date/end_date，无法重放")
        return execute_sync_job(
            db=db,
            job_name=f"sync_adj_factor_{ts_code}_replay",
            runner=lambda: sync_adj_factor(db, ts_code=ts_code, start_date=start_date, end_date=end_date),
            max_retries=1,
            replay_of_job_run_id=source.id,
            job_payload={"ts_code": ts_code, "start_date": start_date, "end_date": end_date},
        )
    if source.job_name.startswith("auto_backfill_recent"):
        exchange = payload.get("exchange", "SSE")
        lookback_days = int(payload.get("lookback_days", 10))
        max_backfill_days = int(payload.get("max_backfill_days", 5))
        return execute_sync_job(
            db=db,
            job_name=f"auto_backfill_recent_{exchange}_replay",
            runner=lambda: _run_backfill_api_job(db, exchange=exchange, lookback_days=lookback_days, max_backfill_days=max_backfill_days),
            max_retries=1,
            replay_of_job_run_id=source.id,
            job_payload={"exchange": exchange, "lookback_days": lookback_days, "max_backfill_days": max_backfill_days},
        )
    raise ValueError("当前任务类型暂不支持重放")


def _run_backfill_api_job(db: Session, exchange: str, lookback_days: int, max_backfill_days: int):
    report = auto_backfill_recent(
        db=db, exchange=exchange, lookback_days=lookback_days, max_backfill_days=max_backfill_days
    )
    return SyncCounter(
        inserted=report.stock_result.inserted + report.index_result.inserted + report.moneyflow_result.inserted,
        updated=report.stock_result.updated + report.index_result.updated + report.moneyflow_result.updated,
    )


@router.get("/basic/stock", response_model=list[StockBasicOut])
def get_stock_basic(
    list_status: str = Query(default="L"),
    limit: int = Query(default=100, ge=1, le=5000),
    db: Session = Depends(get_db),
):
    rows = db.execute(select(StockBasic).where(StockBasic.list_status == list_status).limit(limit)).scalars().all()
    return rows


@router.get("/calendar/trade-days", response_model=list[TradeCalendarOut])
def get_trade_days(
    exchange: str = Query(default="SSE"),
    start_date: str = Query(...),
    end_date: str = Query(...),
    is_open: int | None = Query(default=1),
    db: Session = Depends(get_db),
):
    stmt = select(TradeCalendar).where(
        TradeCalendar.exchange == exchange,
        TradeCalendar.cal_date >= start_date,
        TradeCalendar.cal_date <= end_date,
    )
    if is_open is not None:
        stmt = stmt.where(TradeCalendar.is_open == is_open)
    rows = db.execute(stmt).scalars().all()
    return rows


@router.get("/market/daily", response_model=list[StockDailyOut])
def get_stock_daily(
    ts_code: str = Query(...),
    start_date: str = Query(...),
    end_date: str = Query(...),
    limit: int = Query(default=2000, ge=1, le=10000),
    db: Session = Depends(get_db),
):
    rows = (
        db.execute(
            select(StockDaily)
            .where(
                StockDaily.ts_code == ts_code,
                StockDaily.trade_date >= start_date,
                StockDaily.trade_date <= end_date,
            )
            .order_by(StockDaily.trade_date.desc())
            .limit(limit)
        )
        .scalars()
        .all()
    )
    return rows


@router.get("/market/index-daily", response_model=list[IndexDailyOut])
def get_index_daily(
    ts_code: str = Query(...),
    start_date: str = Query(...),
    end_date: str = Query(...),
    limit: int = Query(default=2000, ge=1, le=10000),
    db: Session = Depends(get_db),
):
    rows = (
        db.execute(
            select(IndexDaily)
            .where(
                IndexDaily.ts_code == ts_code,
                IndexDaily.trade_date >= start_date,
                IndexDaily.trade_date <= end_date,
            )
            .order_by(IndexDaily.trade_date.desc())
            .limit(limit)
        )
        .scalars()
        .all()
    )
    return rows


@router.get("/market/moneyflow", response_model=list[MoneyflowOut])
def get_moneyflow(
    ts_code: str = Query(...),
    start_date: str = Query(...),
    end_date: str = Query(...),
    limit: int = Query(default=2000, ge=1, le=10000),
    db: Session = Depends(get_db),
):
    rows = (
        db.execute(
            select(Moneyflow)
            .where(
                Moneyflow.ts_code == ts_code,
                Moneyflow.trade_date >= start_date,
                Moneyflow.trade_date <= end_date,
            )
            .order_by(Moneyflow.trade_date.desc())
            .limit(limit)
        )
        .scalars()
        .all()
    )
    return rows


@router.get("/market/adj-factor", response_model=list[AdjFactorOut])
def get_adj_factor(
    ts_code: str = Query(...),
    start_date: str = Query(...),
    end_date: str = Query(...),
    limit: int = Query(default=3000, ge=1, le=10000),
    db: Session = Depends(get_db),
):
    rows = (
        db.execute(
            select(AdjFactor)
            .where(
                AdjFactor.ts_code == ts_code,
                AdjFactor.trade_date >= start_date,
                AdjFactor.trade_date <= end_date,
            )
            .order_by(AdjFactor.trade_date.desc())
            .limit(limit)
        )
        .scalars()
        .all()
    )
    return rows


@router.get("/market/daily/adjusted", response_model=list[StockDailyAdjustedOut])
def get_stock_daily_adjusted(
    ts_code: str = Query(...),
    start_date: str = Query(...),
    end_date: str = Query(...),
    adj_type: str = Query(default="qfq", pattern="^(qfq|hfq)$"),
    limit: int = Query(default=2000, ge=1, le=10000),
    db: Session = Depends(get_db),
):
    daily_rows = (
        db.execute(
            select(StockDaily)
            .where(
                StockDaily.ts_code == ts_code,
                StockDaily.trade_date >= start_date,
                StockDaily.trade_date <= end_date,
            )
            .order_by(StockDaily.trade_date.asc())
            .limit(limit)
        )
        .scalars()
        .all()
    )
    factor_rows = (
        db.execute(
            select(AdjFactor)
            .where(
                AdjFactor.ts_code == ts_code,
                AdjFactor.trade_date >= start_date,
                AdjFactor.trade_date <= end_date,
            )
            .order_by(AdjFactor.trade_date.asc())
        )
        .scalars()
        .all()
    )
    factor_by_date = {x.trade_date: x.adj_factor for x in factor_rows}
    factor_values = [x.adj_factor for x in factor_rows if x.adj_factor is not None]
    if not factor_values:
        factor_values = [1.0]
    latest_factor = factor_values[-1]
    earliest_factor = factor_values[0]
    out = []
    for row in daily_rows:
        factor = factor_by_date.get(row.trade_date)
        if factor is None:
            ratio = 1.0
        elif adj_type == "qfq":
            ratio = factor / latest_factor if latest_factor else 1.0
        else:
            ratio = factor / earliest_factor if earliest_factor else 1.0
        out.append(
            {
                "ts_code": row.ts_code,
                "trade_date": row.trade_date,
                "adj_type": adj_type,
                "factor": factor,
                "open": round(row.open * ratio, 6) if row.open is not None else None,
                "high": round(row.high * ratio, 6) if row.high is not None else None,
                "low": round(row.low * ratio, 6) if row.low is not None else None,
                "close": round(row.close * ratio, 6) if row.close is not None else None,
                "pre_close": round(row.pre_close * ratio, 6) if row.pre_close is not None else None,
                "change": round(row.change * ratio, 6) if row.change is not None else None,
                "pct_chg": row.pct_chg,
                "vol": row.vol,
                "amount": row.amount,
            }
        )
    return sorted(out, key=lambda x: x["trade_date"], reverse=True)


@router.get("/board/industry", response_model=list[IndustryBoardOut])
def get_industry_boards(
    src: str = Query(default="SW"),
    limit: int = Query(default=1000, ge=1, le=5000),
    db: Session = Depends(get_db),
):
    rows = db.execute(select(IndustryBoard).where(IndustryBoard.src == src).limit(limit)).scalars().all()
    return rows


@router.get("/board/industry/members", response_model=list[IndustryBoardMemberOut])
def get_industry_board_members(
    index_code: str = Query(...),
    limit: int = Query(default=3000, ge=1, le=10000),
    db: Session = Depends(get_db),
):
    rows = (
        db.execute(
            select(IndustryBoardMember)
            .where(IndustryBoardMember.index_code == index_code)
            .order_by(IndustryBoardMember.id.desc())
            .limit(limit)
        )
        .scalars()
        .all()
    )
    return rows


@router.post("/jobs/sync/stock-basic", response_model=SyncResult)
def run_sync_stock_basic(db: Session = Depends(get_db)):
    try:
        result = execute_sync_job(
            db,
            "sync_stock_basic",
            lambda: sync_stock_basic(db),
            max_retries=1,
            job_payload={},
        )
    except Exception as e:
        _raise_sync_error(e)
    return {"inserted": result.inserted, "updated": result.updated}


@router.post("/jobs/sync/trade-calendar", response_model=SyncResult)
def run_sync_trade_calendar(exchange: str = Query(default="SSE"), db: Session = Depends(get_db)):
    try:
        result = execute_sync_job(
            db,
            f"sync_trade_calendar_{exchange}",
            lambda: sync_trade_calendar(db, exchange=exchange),
            max_retries=1,
            job_payload={"exchange": exchange},
        )
    except Exception as e:
        _raise_sync_error(e)
    return {"inserted": result.inserted, "updated": result.updated}


@router.post("/jobs/sync/stock-daily", response_model=SyncResult)
def run_sync_stock_daily(
    ts_code: str = Query(...),
    start_date: str = Query(...),
    end_date: str = Query(...),
    db: Session = Depends(get_db),
):
    if len(start_date) != 8 or len(end_date) != 8:
        raise_api_error(code="INVALID_DATE_FORMAT", message="start_date/end_date 需为YYYYMMDD")
    try:
        result = execute_sync_job(
            db,
            f"sync_stock_daily_{ts_code}",
            lambda: sync_stock_daily(db, ts_code=ts_code, start_date=start_date, end_date=end_date),
            max_retries=1,
            job_payload={"ts_code": ts_code, "start_date": start_date, "end_date": end_date},
        )
    except Exception as e:
        _raise_sync_error(e)
    return {"inserted": result.inserted, "updated": result.updated}


@router.post("/jobs/sync/stock-daily/by-date", response_model=SyncResult)
def run_sync_stock_daily_by_date(
    trade_date: str = Query(...),
    db: Session = Depends(get_db),
):
    if len(trade_date) != 8:
        raise_api_error(code="INVALID_DATE_FORMAT", message="trade_date 需为YYYYMMDD")
    try:
        result = execute_sync_job(
            db,
            f"sync_stock_daily_by_date_{trade_date}",
            lambda: sync_stock_daily_by_date(db, trade_date=trade_date),
            max_retries=1,
            job_payload={"trade_date": trade_date},
        )
    except Exception as e:
        _raise_sync_error(e)
    return {"inserted": result.inserted, "updated": result.updated}


@router.post("/jobs/sync/stock-daily/incremental", response_model=SyncResult)
def run_sync_stock_daily_incremental(
    exchange: str = Query(default="SSE"),
    lookback_days: int = Query(default=3, ge=1, le=30),
    db: Session = Depends(get_db),
):
    try:
        result = execute_sync_job(
            db,
            f"sync_stock_daily_incremental_{exchange}",
            lambda: sync_stock_daily_incremental(db, exchange=exchange, lookback_days=lookback_days),
            max_retries=1,
            job_payload={"exchange": exchange, "lookback_days": lookback_days},
        )
    except Exception as e:
        _raise_sync_error(e)
    return {"inserted": result.inserted, "updated": result.updated}


@router.post("/jobs/sync/index-daily", response_model=SyncResult)
def run_sync_index_daily(
    ts_code: str = Query(...),
    start_date: str = Query(...),
    end_date: str = Query(...),
    db: Session = Depends(get_db),
):
    if len(start_date) != 8 or len(end_date) != 8:
        raise_api_error(code="INVALID_DATE_FORMAT", message="start_date/end_date 需为YYYYMMDD")
    try:
        result = execute_sync_job(
            db,
            f"sync_index_daily_{ts_code}",
            lambda: sync_index_daily(db, ts_code=ts_code, start_date=start_date, end_date=end_date),
            max_retries=1,
            job_payload={"ts_code": ts_code, "start_date": start_date, "end_date": end_date},
        )
    except Exception as e:
        _raise_sync_error(e)
    return {"inserted": result.inserted, "updated": result.updated}


@router.post("/jobs/sync/index-daily/by-date", response_model=SyncResult)
def run_sync_index_daily_by_date(
    trade_date: str = Query(...),
    db: Session = Depends(get_db),
):
    if len(trade_date) != 8:
        raise_api_error(code="INVALID_DATE_FORMAT", message="trade_date 需为YYYYMMDD")
    try:
        result = execute_sync_job(
            db,
            f"sync_index_daily_by_date_{trade_date}",
            lambda: sync_index_daily_by_date(db, trade_date=trade_date),
            max_retries=1,
            job_payload={"trade_date": trade_date},
        )
    except Exception as e:
        _raise_sync_error(e)
    return {"inserted": result.inserted, "updated": result.updated}


@router.post("/jobs/sync/industry/boards", response_model=SyncResult)
def run_sync_industry_boards(
    src: str = Query(default="SW"),
    db: Session = Depends(get_db),
):
    try:
        result = execute_sync_job(
            db,
            f"sync_industry_boards_{src}",
            lambda: sync_industry_boards(db, src=src),
            max_retries=1,
            job_payload={"src": src},
        )
    except Exception as e:
        _raise_sync_error(e)
    return {"inserted": result.inserted, "updated": result.updated}


@router.post("/jobs/sync/industry/members", response_model=SyncResult)
def run_sync_industry_members(
    index_code: str | None = Query(default=None),
    src: str = Query(default="SW"),
    db: Session = Depends(get_db),
):
    try:
        if index_code:
            result = execute_sync_job(
                db,
                f"sync_industry_members_{index_code}",
                lambda: sync_industry_board_members(db, index_code=index_code, src=src),
                max_retries=1,
                job_payload={"index_code": index_code, "src": src},
            )
        else:
            result = execute_sync_job(
                db,
                f"sync_industry_members_all_{src}",
                lambda: sync_industry_board_members_all(db, src=src),
                max_retries=1,
                job_payload={"src": src},
            )
    except Exception as e:
        _raise_sync_error(e)
    return {"inserted": result.inserted, "updated": result.updated}


@router.post("/jobs/sync/moneyflow", response_model=SyncResult)
def run_sync_moneyflow(
    ts_code: str = Query(...),
    start_date: str = Query(...),
    end_date: str = Query(...),
    db: Session = Depends(get_db),
):
    if len(start_date) != 8 or len(end_date) != 8:
        raise_api_error(code="INVALID_DATE_FORMAT", message="start_date/end_date 需为YYYYMMDD")
    try:
        result = execute_sync_job(
            db,
            f"sync_moneyflow_{ts_code}",
            lambda: sync_moneyflow(db, ts_code=ts_code, start_date=start_date, end_date=end_date),
            max_retries=1,
            job_payload={"ts_code": ts_code, "start_date": start_date, "end_date": end_date},
        )
    except Exception as e:
        _raise_sync_error(e)
    return {"inserted": result.inserted, "updated": result.updated}


@router.post("/jobs/sync/moneyflow/by-date", response_model=SyncResult)
def run_sync_moneyflow_by_date(
    trade_date: str = Query(...),
    db: Session = Depends(get_db),
):
    if len(trade_date) != 8:
        raise_api_error(code="INVALID_DATE_FORMAT", message="trade_date 需为YYYYMMDD")
    try:
        result = execute_sync_job(
            db,
            f"sync_moneyflow_by_date_{trade_date}",
            lambda: sync_moneyflow_by_date(db, trade_date=trade_date),
            max_retries=1,
            job_payload={"trade_date": trade_date},
        )
    except Exception as e:
        _raise_sync_error(e)
    return {"inserted": result.inserted, "updated": result.updated}


@router.post("/jobs/sync/adj-factor", response_model=SyncResult)
def run_sync_adj_factor(
    ts_code: str = Query(...),
    start_date: str = Query(...),
    end_date: str = Query(...),
    db: Session = Depends(get_db),
):
    if len(start_date) != 8 or len(end_date) != 8:
        raise_api_error(code="INVALID_DATE_FORMAT", message="start_date/end_date 需为YYYYMMDD")
    try:
        result = execute_sync_job(
            db,
            f"sync_adj_factor_{ts_code}",
            lambda: sync_adj_factor(db, ts_code=ts_code, start_date=start_date, end_date=end_date),
            max_retries=1,
            job_payload={"ts_code": ts_code, "start_date": start_date, "end_date": end_date},
        )
    except Exception as e:
        _raise_sync_error(e)
    return {"inserted": result.inserted, "updated": result.updated}


@router.post("/jobs/sync/adj-factor/by-date", response_model=SyncResult)
def run_sync_adj_factor_by_date(
    trade_date: str = Query(...),
    db: Session = Depends(get_db),
):
    if len(trade_date) != 8:
        raise_api_error(code="INVALID_DATE_FORMAT", message="trade_date 需为YYYYMMDD")
    try:
        result = execute_sync_job(
            db,
            f"sync_adj_factor_by_date_{trade_date}",
            lambda: sync_adj_factor_by_date(db, trade_date=trade_date),
            max_retries=1,
            job_payload={"trade_date": trade_date},
        )
    except Exception as e:
        _raise_sync_error(e)
    return {"inserted": result.inserted, "updated": result.updated}


@router.post("/jobs/backfill/recent", response_model=SyncResult)
def run_backfill_recent(
    exchange: str = Query(default="SSE"),
    lookback_days: int = Query(default=10, ge=1, le=180),
    max_backfill_days: int = Query(default=5, ge=1, le=60),
    db: Session = Depends(get_db),
):
    try:
        result = execute_sync_job(
            db,
            f"auto_backfill_recent_{exchange}",
            lambda: _run_backfill_api_job(
                db, exchange=exchange, lookback_days=lookback_days, max_backfill_days=max_backfill_days
            ),
            max_retries=1,
            job_payload={"exchange": exchange, "lookback_days": lookback_days, "max_backfill_days": max_backfill_days},
        )
    except Exception as e:
        _raise_sync_error(e)
    return {"inserted": result.inserted, "updated": result.updated}


@router.get("/quality/stock-daily", response_model=QualityReportOut)
def get_stock_daily_quality(
    start_date: str = Query(...),
    end_date: str = Query(...),
    exchange: str = Query(default="SSE"),
    db: Session = Depends(get_db),
):
    if len(start_date) != 8 or len(end_date) != 8:
        raise_api_error(code="INVALID_DATE_FORMAT", message="start_date/end_date 需为YYYYMMDD")
    report = check_stock_daily_quality(db, start_date=start_date, end_date=end_date, exchange=exchange)
    return report


@router.get("/jobs/runs", response_model=JobRunPageOut)
def get_job_runs(
    limit: int = Query(default=100, ge=1, le=500),
    cursor: int | None = Query(default=None, ge=1),
    job_name: str | None = Query(default=None),
    status: str | None = Query(default=None),
    db: Session = Depends(get_db),
):
    rows, next_cursor = list_job_runs_page(db=db, limit=limit, cursor=cursor, job_name=job_name, status=status)
    return {"items": [_serialize_job_run(r) for r in rows], "next_cursor": next_cursor}


@router.post("/jobs/runs/{job_run_id}/replay", response_model=SyncResult)
def replay_job_run(job_run_id: int, db: Session = Depends(get_db)):
    source = get_job_run_by_id(db=db, job_run_id=job_run_id)
    if not source:
        raise_api_error(code="NOT_FOUND", message="任务运行记录不存在")
    try:
        result = _replay_job(db=db, source=source)
    except Exception as e:
        _raise_sync_error(e)
    return {"inserted": result.inserted, "updated": result.updated}


@router.get("/ops/metrics")
def get_metrics():
    return snapshot_metrics()
