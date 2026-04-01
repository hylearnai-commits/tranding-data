from datetime import datetime

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.config import settings
from app.db import SessionLocal
from app.services.job_service import execute_sync_job
from app.services.sync_service import (
    SyncCounter,
    auto_backfill_recent,
    sync_adj_factor_by_date,
    sync_index_daily_incremental,
    sync_moneyflow_incremental,
    sync_stock_basic,
    sync_stock_daily_incremental,
    sync_trade_calendar,
)


scheduler = BackgroundScheduler()


def _run_sync_basic():
    db = SessionLocal()
    try:
        execute_sync_job(
            db,
            "sync_stock_basic_scheduled",
            lambda: sync_stock_basic(db),
            max_retries=1,
            job_payload={},
        )
    finally:
        db.close()


def _run_sync_calendar():
    db = SessionLocal()
    try:
        execute_sync_job(
            db,
            "sync_trade_calendar_sse_scheduled",
            lambda: sync_trade_calendar(db, exchange="SSE"),
            max_retries=1,
            job_payload={"exchange": "SSE"},
        )
        execute_sync_job(
            db,
            "sync_trade_calendar_szse_scheduled",
            lambda: sync_trade_calendar(db, exchange="SZSE"),
            max_retries=1,
            job_payload={"exchange": "SZSE"},
        )
    finally:
        db.close()


def _run_sync_daily():
    db = SessionLocal()
    try:
        execute_sync_job(
            db,
            "sync_stock_daily_incremental_sse_scheduled",
            lambda: sync_stock_daily_incremental(db, exchange="SSE", lookback_days=settings.sync_daily_lookback_days),
            max_retries=1,
            job_payload={"exchange": "SSE", "lookback_days": settings.sync_daily_lookback_days},
        )
    finally:
        db.close()


def _run_sync_index():
    db = SessionLocal()
    try:
        execute_sync_job(
            db,
            "sync_index_daily_incremental_sse_scheduled",
            lambda: sync_index_daily_incremental(db, exchange="SSE", lookback_days=settings.sync_daily_lookback_days),
            max_retries=1,
            job_payload={"exchange": "SSE", "lookback_days": settings.sync_daily_lookback_days},
        )
    finally:
        db.close()


def _run_sync_moneyflow():
    db = SessionLocal()
    try:
        execute_sync_job(
            db,
            "sync_moneyflow_incremental_sse_scheduled",
            lambda: sync_moneyflow_incremental(db, exchange="SSE", lookback_days=settings.sync_daily_lookback_days),
            max_retries=1,
            job_payload={"exchange": "SSE", "lookback_days": settings.sync_daily_lookback_days},
        )
    finally:
        db.close()


def _run_sync_adj_factor():
    db = SessionLocal()
    try:
        execute_sync_job(
            db,
            "sync_adj_factor_by_date_scheduled",
            lambda: sync_adj_factor_by_date(db, trade_date=datetime.now().strftime("%Y%m%d")),
            max_retries=1,
            job_payload={"trade_date": datetime.now().strftime("%Y%m%d")},
        )
    finally:
        db.close()


def _run_auto_backfill():
    db = SessionLocal()
    try:
        execute_sync_job(
            db,
            "auto_backfill_recent_scheduled",
            lambda: _run_backfill_job(db),
            max_retries=1,
            job_payload={
                "exchange": "SSE",
                "lookback_days": settings.backfill_lookback_days,
                "max_backfill_days": settings.backfill_max_days,
            },
        )
    finally:
        db.close()


def _run_backfill_job(db):
    report = auto_backfill_recent(
        db=db,
        exchange="SSE",
        lookback_days=settings.backfill_lookback_days,
        max_backfill_days=settings.backfill_max_days,
    )
    return SyncCounter(
        inserted=report.stock_result.inserted + report.index_result.inserted + report.moneyflow_result.inserted,
        updated=report.stock_result.updated + report.index_result.updated + report.moneyflow_result.updated,
    )


def setup_scheduler():
    if not settings.scheduler_enabled:
        return
    if not scheduler.running:
        scheduler.add_job(_run_sync_basic, CronTrigger.from_crontab(settings.sync_basic_cron), id="sync_basic")
        scheduler.add_job(
            _run_sync_calendar, CronTrigger.from_crontab(settings.sync_calendar_cron), id="sync_calendar"
        )
        scheduler.add_job(_run_sync_daily, CronTrigger.from_crontab(settings.sync_daily_cron), id="sync_daily")
        scheduler.add_job(_run_sync_index, CronTrigger.from_crontab(settings.sync_index_cron), id="sync_index")
        scheduler.add_job(
            _run_sync_moneyflow, CronTrigger.from_crontab(settings.sync_moneyflow_cron), id="sync_moneyflow"
        )
        scheduler.add_job(
            _run_sync_adj_factor, CronTrigger.from_crontab(settings.sync_adj_factor_cron), id="sync_adj_factor"
        )
        scheduler.add_job(_run_auto_backfill, CronTrigger.from_crontab(settings.backfill_cron), id="auto_backfill")
        scheduler.start()


def shutdown_scheduler():
    if scheduler.running:
        scheduler.shutdown(wait=False)
