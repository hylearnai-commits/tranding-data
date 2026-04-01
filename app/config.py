from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Trading Data Center"
    api_prefix: str = "/api/v1"
    database_url: str = "sqlite:///./trading_data.db"
    tushare_token: str = ""
    scheduler_enabled: bool = True
    sync_daily_cron: str = "5 18 * * 1-5"
    sync_daily_lookback_days: int = 3
    sync_basic_cron: str = "10 18 * * 1-5"
    sync_calendar_cron: str = "15 18 * * 1-5"
    job_lock_ttl_seconds: int = 900
    alert_enabled: bool = False
    alert_webhook_url: str = ""
    alert_timeout_seconds: int = 5
    auth_enabled: bool = False
    api_keys: str = ""
    rate_limit_per_minute: int = 120
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")


settings = Settings()
