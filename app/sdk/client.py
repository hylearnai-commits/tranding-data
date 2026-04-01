import requests

from app.sdk.errors import ERROR_CLASS_BY_CODE, TradingDataSDKError, TransportError


class TradingDataClient:
    def __init__(self, base_url: str, api_key: str | None = None):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key

    def _headers(self) -> dict:
        if not self.api_key:
            return {}
        return {"X-API-Key": self.api_key}

    def _request(self, method: str, path: str, params: dict | None = None, timeout: int = 30, auth: bool = True):
        headers = self._headers() if auth else {}
        try:
            resp = requests.request(
                method=method,
                url=f"{self.base_url}{path}",
                params=params,
                headers=headers,
                timeout=timeout,
            )
        except requests.RequestException as e:
            raise TransportError(message=str(e), code="TRANSPORT_ERROR", error_type="transport_error") from e
        if 200 <= resp.status_code < 300:
            return resp.json()
        try:
            payload = resp.json()
        except Exception:
            payload = {"error": {"code": "INTERNAL_ERROR", "type": "internal_error", "message": resp.text, "trace_id": ""}}
        err = payload.get("error", {})
        code = err.get("code", "INTERNAL_ERROR")
        error_class = ERROR_CLASS_BY_CODE.get(code, TradingDataSDKError)
        raise error_class(
            message=err.get("message", "request failed"),
            code=code,
            error_type=err.get("type", ""),
            status_code=resp.status_code,
            trace_id=err.get("trace_id", ""),
        )

    def _post_sync(self, path: str, params: dict | None = None) -> dict:
        return self._request("POST", path, params=params)

    def health(self) -> dict:
        return self._request("GET", "/health", timeout=10, auth=False)

    def get_stock_basic(self, list_status: str = "L", limit: int = 100) -> list[dict]:
        return self._request("GET", "/api/v1/basic/stock", params={"list_status": list_status, "limit": limit})

    def get_trade_days(self, exchange: str, start_date: str, end_date: str, is_open: int = 1) -> list[dict]:
        return self._request(
            "GET",
            "/api/v1/calendar/trade-days",
            params={"exchange": exchange, "start_date": start_date, "end_date": end_date, "is_open": is_open},
        )

    def get_stock_daily(self, ts_code: str, start_date: str, end_date: str, limit: int = 2000) -> list[dict]:
        return self._request(
            "GET",
            "/api/v1/market/daily",
            params={"ts_code": ts_code, "start_date": start_date, "end_date": end_date, "limit": limit},
        )

    def get_index_daily(self, ts_code: str, start_date: str, end_date: str, limit: int = 2000) -> list[dict]:
        return self._request(
            "GET",
            "/api/v1/market/index-daily",
            params={"ts_code": ts_code, "start_date": start_date, "end_date": end_date, "limit": limit},
        )

    def get_moneyflow(self, ts_code: str, start_date: str, end_date: str, limit: int = 2000) -> list[dict]:
        return self._request(
            "GET",
            "/api/v1/market/moneyflow",
            params={"ts_code": ts_code, "start_date": start_date, "end_date": end_date, "limit": limit},
        )

    def get_industry_boards(self, src: str = "SW", limit: int = 1000) -> list[dict]:
        return self._request("GET", "/api/v1/board/industry", params={"src": src, "limit": limit})

    def get_industry_board_members(self, index_code: str, limit: int = 3000) -> list[dict]:
        return self._request(
            "GET",
            "/api/v1/board/industry/members",
            params={"index_code": index_code, "limit": limit},
        )

    def get_adj_factor(self, ts_code: str, start_date: str, end_date: str, limit: int = 3000) -> list[dict]:
        return self._request(
            "GET",
            "/api/v1/market/adj-factor",
            params={"ts_code": ts_code, "start_date": start_date, "end_date": end_date, "limit": limit},
        )

    def get_stock_daily_adjusted(
        self, ts_code: str, start_date: str, end_date: str, adj_type: str = "qfq", limit: int = 2000
    ) -> list[dict]:
        return self._request(
            "GET",
            "/api/v1/market/daily/adjusted",
            params={"ts_code": ts_code, "start_date": start_date, "end_date": end_date, "adj_type": adj_type, "limit": limit},
        )

    def get_job_runs(
        self, limit: int = 100, cursor: int | None = None, job_name: str | None = None, status: str | None = None
    ) -> dict:
        params = {"limit": limit}
        if cursor is not None:
            params["cursor"] = cursor
        if job_name is not None:
            params["job_name"] = job_name
        if status is not None:
            params["status"] = status
        return self._request("GET", "/api/v1/jobs/runs", params=params)

    def replay_job_run(self, job_run_id: int) -> dict:
        return self._post_sync(f"/api/v1/jobs/runs/{job_run_id}/replay")

    def get_metrics(self) -> dict:
        return self._request("GET", "/api/v1/ops/metrics")

    def sync_stock_basic(self) -> dict:
        return self._post_sync("/api/v1/jobs/sync/stock-basic")

    def sync_trade_calendar(self, exchange: str = "SSE") -> dict:
        return self._post_sync("/api/v1/jobs/sync/trade-calendar", params={"exchange": exchange})

    def sync_stock_daily(self, ts_code: str, start_date: str, end_date: str) -> dict:
        return self._post_sync(
            "/api/v1/jobs/sync/stock-daily",
            params={"ts_code": ts_code, "start_date": start_date, "end_date": end_date},
        )

    def sync_stock_daily_by_date(self, trade_date: str) -> dict:
        return self._post_sync("/api/v1/jobs/sync/stock-daily/by-date", params={"trade_date": trade_date})

    def sync_stock_daily_incremental(self, exchange: str = "SSE", lookback_days: int = 3) -> dict:
        return self._post_sync(
            "/api/v1/jobs/sync/stock-daily/incremental",
            params={"exchange": exchange, "lookback_days": lookback_days},
        )

    def sync_index_daily(self, ts_code: str, start_date: str, end_date: str) -> dict:
        return self._post_sync(
            "/api/v1/jobs/sync/index-daily",
            params={"ts_code": ts_code, "start_date": start_date, "end_date": end_date},
        )

    def sync_index_daily_by_date(self, trade_date: str) -> dict:
        return self._post_sync("/api/v1/jobs/sync/index-daily/by-date", params={"trade_date": trade_date})

    def sync_industry_boards(self, src: str = "SW") -> dict:
        return self._post_sync("/api/v1/jobs/sync/industry/boards", params={"src": src})

    def sync_industry_members(self, index_code: str | None = None, src: str = "SW") -> dict:
        params = {"src": src}
        if index_code is not None:
            params["index_code"] = index_code
        return self._post_sync("/api/v1/jobs/sync/industry/members", params=params)

    def sync_moneyflow(self, ts_code: str, start_date: str, end_date: str) -> dict:
        return self._post_sync(
            "/api/v1/jobs/sync/moneyflow",
            params={"ts_code": ts_code, "start_date": start_date, "end_date": end_date},
        )

    def sync_moneyflow_by_date(self, trade_date: str) -> dict:
        return self._post_sync("/api/v1/jobs/sync/moneyflow/by-date", params={"trade_date": trade_date})

    def sync_adj_factor(self, ts_code: str, start_date: str, end_date: str) -> dict:
        return self._post_sync(
            "/api/v1/jobs/sync/adj-factor",
            params={"ts_code": ts_code, "start_date": start_date, "end_date": end_date},
        )

    def sync_adj_factor_by_date(self, trade_date: str) -> dict:
        return self._post_sync("/api/v1/jobs/sync/adj-factor/by-date", params={"trade_date": trade_date})

    def backfill_recent(self, exchange: str = "SSE", lookback_days: int = 10, max_backfill_days: int = 5) -> dict:
        return self._post_sync(
            "/api/v1/jobs/backfill/recent",
            params={"exchange": exchange, "lookback_days": lookback_days, "max_backfill_days": max_backfill_days},
        )
