import json

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db import get_db
from app.models import JobRun, StockBasic, StockDaily, TradeCalendar
from app.schemas import JobRunOut, JobRunPageOut, QualityReportOut, StockBasicOut, StockDailyOut, SyncResult, TradeCalendarOut
from app.services.job_service import execute_sync_job, get_job_run_by_id, list_job_runs_page
from app.services.sync_service import (
    check_stock_daily_quality,
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
        raise HTTPException(status_code=409, detail=str(e)) from e
    if isinstance(e, ValueError):
        raise HTTPException(status_code=400, detail=str(e)) from e
    raise HTTPException(status_code=500, detail=str(e)) from e


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
    raise ValueError("当前任务类型暂不支持重放")


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
        raise HTTPException(status_code=400, detail="start_date/end_date 需为YYYYMMDD")
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
        raise HTTPException(status_code=400, detail="trade_date 需为YYYYMMDD")
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


@router.get("/quality/stock-daily", response_model=QualityReportOut)
def get_stock_daily_quality(
    start_date: str = Query(...),
    end_date: str = Query(...),
    exchange: str = Query(default="SSE"),
    db: Session = Depends(get_db),
):
    if len(start_date) != 8 or len(end_date) != 8:
        raise HTTPException(status_code=400, detail="start_date/end_date 需为YYYYMMDD")
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
        raise HTTPException(status_code=404, detail="任务运行记录不存在")
    try:
        result = _replay_job(db=db, source=source)
    except Exception as e:
        _raise_sync_error(e)
    return {"inserted": result.inserted, "updated": result.updated}
