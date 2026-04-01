from sqlalchemy import create_engine, text
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from app.config import settings


class Base(DeclarativeBase):
    pass


engine = create_engine(
    settings.database_url,
    echo=False,
    future=True,
    connect_args={"check_same_thread": False} if settings.database_url.startswith("sqlite") else {},
)
SessionLocal = sessionmaker(bind=engine, class_=Session, autoflush=False, autocommit=False)


def run_startup_migrations():
    if not settings.database_url.startswith("sqlite"):
        return
    with engine.begin() as conn:
        table_names = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'")).fetchall()
        table_name_set = {row[0] for row in table_names}
        if "job_run" in table_name_set:
            columns = conn.execute(text("PRAGMA table_info(job_run)")).fetchall()
            column_name_set = {row[1] for row in columns}
            if "replay_of_job_run_id" not in column_name_set:
                conn.execute(text("ALTER TABLE job_run ADD COLUMN replay_of_job_run_id INTEGER"))
            if "job_payload" not in column_name_set:
                conn.execute(text("ALTER TABLE job_run ADD COLUMN job_payload VARCHAR(1024)"))


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
