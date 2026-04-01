import json
import uuid
from datetime import datetime, timedelta
from typing import Callable

import requests
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config import settings
from app.models import JobLock, JobRun
from app.observability import get_trace_id, new_trace_id, record_job, set_trace_id
from app.services.sync_service import SyncCounter


def _notify_failure(job_name: str, attempts: int, error_message: str):
    if not settings.alert_enabled or not settings.alert_webhook_url:
        return
    level = "warning" if attempts <= 1 else ("error" if attempts <= 3 else "critical")
    payload = {
        "event": "sync_job_failed",
        "job_name": job_name,
        "attempts": attempts,
        "level": level,
        "error_message": error_message,
        "trace_id": get_trace_id() or new_trace_id(),
        "timestamp": datetime.utcnow().isoformat(),
    }
    try:
        requests.post(settings.alert_webhook_url, json=payload, timeout=settings.alert_timeout_seconds)
    except Exception:
        return


def _acquire_job_lock(db: Session, job_name: str) -> str:
    now = datetime.utcnow()
    owner = str(uuid.uuid4())
    lock = db.execute(select(JobLock).where(JobLock.job_name == job_name)).scalar_one_or_none()
    if lock and lock.locked_until > now:
        raise RuntimeError(f"任务锁冲突: {job_name}")
    lock_until = now + timedelta(seconds=settings.job_lock_ttl_seconds)
    if lock is None:
        lock = JobLock(job_name=job_name, owner=owner, locked_until=lock_until)
        db.add(lock)
    else:
        lock.owner = owner
        lock.locked_until = lock_until
    db.commit()
    return owner


def _release_job_lock(db: Session, job_name: str, owner: str):
    lock = db.execute(select(JobLock).where(JobLock.job_name == job_name)).scalar_one_or_none()
    if lock and lock.owner == owner:
        lock.locked_until = datetime.utcnow()
        db.commit()


def execute_sync_job(
    db: Session,
    job_name: str,
    runner: Callable[[], SyncCounter],
    max_retries: int = 1,
    replay_of_job_run_id: int | None = None,
    job_payload: dict | None = None,
) -> SyncCounter:
    if not get_trace_id():
        set_trace_id(new_trace_id())
    owner = _acquire_job_lock(db=db, job_name=job_name)
    attempts = 0
    last_error: Exception | None = None
    try:
        while attempts <= max_retries:
            attempts += 1
            started_at = datetime.utcnow()
            run = JobRun(
                job_name=job_name,
                status="running",
                attempts=attempts,
                started_at=started_at,
                replay_of_job_run_id=replay_of_job_run_id,
                job_payload=json.dumps(job_payload, ensure_ascii=False) if job_payload else None,
            )
            db.add(run)
            db.commit()
            db.refresh(run)
            try:
                result = runner()
                run.status = "success"
                run.inserted = result.inserted
                run.updated = result.updated
                run.finished_at = datetime.utcnow()
                db.commit()
                duration_ms = (run.finished_at - run.started_at).total_seconds() * 1000
                record_job(job_name=job_name, status="success", duration_ms=duration_ms)
                return result
            except Exception as e:
                last_error = e
                run.status = "failed"
                run.error_message = str(e)[:512]
                run.finished_at = datetime.utcnow()
                db.commit()
                duration_ms = (run.finished_at - run.started_at).total_seconds() * 1000
                record_job(job_name=job_name, status="failed", duration_ms=duration_ms)
        if last_error:
            _notify_failure(job_name=job_name, attempts=attempts, error_message=str(last_error))
            raise last_error
        return SyncCounter()
    finally:
        _release_job_lock(db=db, job_name=job_name, owner=owner)


def list_job_runs_page(
    db: Session,
    limit: int = 100,
    cursor: int | None = None,
    job_name: str | None = None,
    status: str | None = None,
) -> tuple[list[JobRun], int | None]:
    stmt = select(JobRun)
    if cursor:
        stmt = stmt.where(JobRun.id < cursor)
    if job_name:
        stmt = stmt.where(JobRun.job_name == job_name)
    if status:
        stmt = stmt.where(JobRun.status == status)
    rows = db.execute(stmt.order_by(JobRun.id.desc()).limit(limit)).scalars().all()
    next_cursor = rows[-1].id if len(rows) == limit else None
    return rows, next_cursor


def get_job_run_by_id(db: Session, job_run_id: int) -> JobRun | None:
    return db.get(JobRun, job_run_id)
