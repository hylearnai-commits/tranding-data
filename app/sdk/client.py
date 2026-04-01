import requests


class TradingDataClient:
    def __init__(self, base_url: str, api_key: str | None = None):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key

    def _headers(self) -> dict:
        if not self.api_key:
            return {}
        return {"X-API-Key": self.api_key}

    def health(self) -> dict:
        return requests.get(f"{self.base_url}/health", timeout=10).json()

    def get_stock_basic(self, list_status: str = "L", limit: int = 100) -> list[dict]:
        return requests.get(
            f"{self.base_url}/api/v1/basic/stock",
            params={"list_status": list_status, "limit": limit},
            headers=self._headers(),
            timeout=30,
        ).json()

    def get_trade_days(self, exchange: str, start_date: str, end_date: str, is_open: int = 1) -> list[dict]:
        return requests.get(
            f"{self.base_url}/api/v1/calendar/trade-days",
            params={"exchange": exchange, "start_date": start_date, "end_date": end_date, "is_open": is_open},
            headers=self._headers(),
            timeout=30,
        ).json()

    def get_stock_daily(self, ts_code: str, start_date: str, end_date: str, limit: int = 2000) -> list[dict]:
        return requests.get(
            f"{self.base_url}/api/v1/market/daily",
            params={"ts_code": ts_code, "start_date": start_date, "end_date": end_date, "limit": limit},
            headers=self._headers(),
            timeout=30,
        ).json()

    def get_index_daily(self, ts_code: str, start_date: str, end_date: str, limit: int = 2000) -> list[dict]:
        return requests.get(
            f"{self.base_url}/api/v1/market/index-daily",
            params={"ts_code": ts_code, "start_date": start_date, "end_date": end_date, "limit": limit},
            headers=self._headers(),
            timeout=30,
        ).json()

    def get_moneyflow(self, ts_code: str, start_date: str, end_date: str, limit: int = 2000) -> list[dict]:
        return requests.get(
            f"{self.base_url}/api/v1/market/moneyflow",
            params={"ts_code": ts_code, "start_date": start_date, "end_date": end_date, "limit": limit},
            headers=self._headers(),
            timeout=30,
        ).json()

    def get_industry_boards(self, src: str = "SW", limit: int = 1000) -> list[dict]:
        return requests.get(
            f"{self.base_url}/api/v1/board/industry",
            params={"src": src, "limit": limit},
            headers=self._headers(),
            timeout=30,
        ).json()

    def get_industry_board_members(self, index_code: str, limit: int = 3000) -> list[dict]:
        return requests.get(
            f"{self.base_url}/api/v1/board/industry/members",
            params={"index_code": index_code, "limit": limit},
            headers=self._headers(),
            timeout=30,
        ).json()
