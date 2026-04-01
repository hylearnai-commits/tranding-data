import json
import logging
import uuid
from collections import deque
from contextvars import ContextVar
from threading import Lock
from time import perf_counter, time

from fastapi import Request


trace_id_ctx: ContextVar[str] = ContextVar("trace_id", default="")

_metrics_lock = Lock()
_request_timestamps: deque[float] = deque()
_request_total = 0
_request_by_status: dict[str, int] = {}
_request_by_path: dict[str, int] = {}
_request_latency_sum = 0.0
_job_total = 0
_job_success = 0
_job_failed = 0
_job_duration_sum = 0.0
_job_by_name: dict[str, dict[str, float | int]] = {}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "trace_id": trace_id_ctx.get() or "",
        }
        return json.dumps(payload, ensure_ascii=False)


def setup_logging():
    root_logger = logging.getLogger()
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    root_logger.handlers = [handler]
    root_logger.setLevel(logging.INFO)


def new_trace_id() -> str:
    return str(uuid.uuid4())


def set_trace_id(trace_id: str):
    trace_id_ctx.set(trace_id)


def get_trace_id() -> str:
    return trace_id_ctx.get() or ""


async def request_observability_middleware(request: Request, call_next):
    trace_id = request.headers.get("X-Trace-Id") or new_trace_id()
    set_trace_id(trace_id)
    started = perf_counter()
    response = await call_next(request)
    cost_ms = (perf_counter() - started) * 1000
    response.headers["X-Trace-Id"] = trace_id
    record_request(path=request.url.path, status_code=response.status_code, duration_ms=cost_ms)
    logging.getLogger("request").info(
        f"path={request.url.path} method={request.method} status={response.status_code} cost_ms={cost_ms:.2f}"
    )
    return response


def record_request(path: str, status_code: int, duration_ms: float):
    now = time()
    with _metrics_lock:
        global _request_total, _request_latency_sum
        _request_total += 1
        _request_latency_sum += duration_ms
        _request_by_status[str(status_code)] = _request_by_status.get(str(status_code), 0) + 1
        _request_by_path[path] = _request_by_path.get(path, 0) + 1
        _request_timestamps.append(now)
        window = now - 60
        while _request_timestamps and _request_timestamps[0] < window:
            _request_timestamps.popleft()


def record_job(job_name: str, status: str, duration_ms: float):
    with _metrics_lock:
        global _job_total, _job_success, _job_failed, _job_duration_sum
        _job_total += 1
        _job_duration_sum += duration_ms
        if status == "success":
            _job_success += 1
        if status == "failed":
            _job_failed += 1
        rec = _job_by_name.get(job_name)
        if rec is None:
            rec = {"total": 0, "success": 0, "failed": 0, "duration_ms_sum": 0.0}
            _job_by_name[job_name] = rec
        rec["total"] += 1
        rec["duration_ms_sum"] += duration_ms
        if status == "success":
            rec["success"] += 1
        if status == "failed":
            rec["failed"] += 1


def snapshot_metrics() -> dict:
    now = time()
    with _metrics_lock:
        qps_60s = len(_request_timestamps) / 60.0
        qps_10s = len([t for t in _request_timestamps if t >= now - 10]) / 10.0
        request_avg_ms = _request_latency_sum / _request_total if _request_total else 0.0
        job_avg_ms = _job_duration_sum / _job_total if _job_total else 0.0
        job_success_rate = _job_success / _job_total if _job_total else 0.0
        return {
            "request": {
                "total": _request_total,
                "avg_latency_ms": round(request_avg_ms, 3),
                "qps_10s": round(qps_10s, 3),
                "qps_60s": round(qps_60s, 3),
                "by_status": dict(_request_by_status),
                "by_path": dict(_request_by_path),
            },
            "job": {
                "total": _job_total,
                "success": _job_success,
                "failed": _job_failed,
                "success_rate": round(job_success_rate, 6),
                "avg_duration_ms": round(job_avg_ms, 3),
                "by_name": _job_by_name,
            },
        }
