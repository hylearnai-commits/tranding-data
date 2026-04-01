from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI

from app.api import router
from app.config import settings
from app.db import Base, engine, run_startup_migrations
from app.scheduler import setup_scheduler, shutdown_scheduler
from app.security import verify_api_key_and_rate_limit


@asynccontextmanager
async def lifespan(_: FastAPI):
    Base.metadata.create_all(bind=engine)
    run_startup_migrations()
    setup_scheduler()
    yield
    shutdown_scheduler()


app = FastAPI(title=settings.app_name, lifespan=lifespan)
app.include_router(router, prefix=settings.api_prefix, dependencies=[Depends(verify_api_key_and_rate_limit)])


@app.get("/health")
def health():
    return {"status": "ok"}
