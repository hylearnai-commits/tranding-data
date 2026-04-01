from collections import deque
from threading import Lock
from time import time

from fastapi import Header, HTTPException

from app.config import settings


_rate_limit_lock = Lock()
_rate_limit_windows: dict[str, deque[float]] = {}


def _get_configured_api_keys() -> set[str]:
    return {k.strip() for k in settings.api_keys.split(",") if k.strip()}


def verify_api_key_and_rate_limit(x_api_key: str | None = Header(default=None)):
    if not settings.auth_enabled:
        return
    configured_keys = _get_configured_api_keys()
    if not configured_keys:
        raise HTTPException(status_code=500, detail="AUTH_ENABLED=true 但未配置API_KEYS")
    if not x_api_key or x_api_key not in configured_keys:
        raise HTTPException(status_code=401, detail="无效的API Key")
    now = time()
    window_start = now - 60
    with _rate_limit_lock:
        q = _rate_limit_windows.get(x_api_key)
        if q is None:
            q = deque()
            _rate_limit_windows[x_api_key] = q
        while q and q[0] < window_start:
            q.popleft()
        if len(q) >= settings.rate_limit_per_minute:
            raise HTTPException(status_code=429, detail="请求过于频繁，请稍后再试")
        q.append(now)
