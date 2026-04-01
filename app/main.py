from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.requests import Request
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.api import router
from app.config import settings
from app.db import Base, engine, run_startup_migrations
from app.errors import error_detail
from app.observability import get_trace_id
from app.observability import request_observability_middleware, setup_logging
from app.scheduler import setup_scheduler, shutdown_scheduler
from app.security import verify_api_key_and_rate_limit


@asynccontextmanager
async def lifespan(_: FastAPI):
    setup_logging()
    Base.metadata.create_all(bind=engine)
    run_startup_migrations()
    setup_scheduler()
    yield
    shutdown_scheduler()


app = FastAPI(title=settings.app_name, lifespan=lifespan)
app.include_router(router, prefix=settings.api_prefix, dependencies=[Depends(verify_api_key_and_rate_limit)])
app.middleware("http")(request_observability_middleware)


@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(_: Request, exc: StarletteHTTPException):
    trace_id = get_trace_id()
    if isinstance(exc.detail, dict) and "code" in exc.detail and "type" in exc.detail:
        payload = {"error": {**exc.detail, "trace_id": exc.detail.get("trace_id") or trace_id}}
    else:
        status_code_map = {
            400: "BAD_REQUEST",
            401: "UNAUTHORIZED",
            403: "FORBIDDEN",
            404: "NOT_FOUND",
            409: "JOB_LOCK_CONFLICT",
            429: "RATE_LIMITED",
        }
        code = status_code_map.get(exc.status_code, "INTERNAL_ERROR")
        payload = {"error": error_detail(code=code, message=str(exc.detail), trace_id=trace_id)}
    return JSONResponse(status_code=exc.status_code, content=payload)


@app.exception_handler(RequestValidationError)
async def request_validation_exception_handler(_: Request, exc: RequestValidationError):
    trace_id = get_trace_id()
    return JSONResponse(
        status_code=400,
        content={
            "error": error_detail(
                code="INVALID_ARGUMENT",
                message=str(exc.errors()),
                trace_id=trace_id,
            )
        },
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(_: Request, exc: Exception):
    trace_id = get_trace_id()
    return JSONResponse(
        status_code=500,
        content={"error": error_detail(code="INTERNAL_ERROR", message=str(exc), trace_id=trace_id)},
    )


@app.get("/health")
def health():
    return {"status": "ok"}
