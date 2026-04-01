from pydantic import BaseModel


class StockBasicOut(BaseModel):
    ts_code: str
    symbol: str
    name: str
    area: str | None
    industry: str | None
    market: str | None
    list_status: str
    list_date: str | None
    delist_date: str | None
    is_hs: str | None


class TradeCalendarOut(BaseModel):
    exchange: str
    cal_date: str
    is_open: int
    pretrade_date: str | None


class StockDailyOut(BaseModel):
    ts_code: str
    trade_date: str
    open: float | None
    high: float | None
    low: float | None
    close: float | None
    pre_close: float | None
    change: float | None
    pct_chg: float | None
    vol: float | None
    amount: float | None


class SyncResult(BaseModel):
    inserted: int
    updated: int


class QualityReportOut(BaseModel):
    start_date: str
    end_date: str
    exchange: str
    expected_trade_days: int
    existing_trade_days: int
    missing_trade_days: int
    invalid_price_rows: int


class JobRunOut(BaseModel):
    id: int
    job_name: str
    status: str
    attempts: int
    inserted: int
    updated: int
    replay_of_job_run_id: int | None
    job_payload: str | None
    error_message: str | None
    started_at: str
    finished_at: str | None


class JobRunPageOut(BaseModel):
    items: list[JobRunOut]
    next_cursor: int | None
