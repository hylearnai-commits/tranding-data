from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.config import settings
from app.db import SessionLocal
from app.services.job_service import execute_sync_job
from app.services.sync_service import sync_stock_basic, sync_stock_daily_incremental, sync_trade_calendar


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


def setup_scheduler():
    if not settings.scheduler_enabled:
        return
    if not scheduler.running:
        scheduler.add_job(_run_sync_basic, CronTrigger.from_crontab(settings.sync_basic_cron), id="sync_basic")
        scheduler.add_job(
            _run_sync_calendar, CronTrigger.from_crontab(settings.sync_calendar_cron), id="sync_calendar"
        )
        scheduler.add_job(_run_sync_daily, CronTrigger.from_crontab(settings.sync_daily_cron), id="sync_daily")
        scheduler.start()


def shutdown_scheduler():
    if scheduler.running:
        scheduler.shutdown(wait=False)
