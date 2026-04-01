import pandas as pd
import tushare as ts

from app.config import settings


class TushareClient:
    def __init__(self):
        if not settings.tushare_token:
            raise ValueError("TUSHARE_TOKEN 未配置")
        ts.set_token(settings.tushare_token)
        self.pro = ts.pro_api()

    def fetch_stock_basic(self) -> pd.DataFrame:
        return self.pro.stock_basic(
            exchange="",
            list_status="L",
            fields="ts_code,symbol,name,area,industry,market,list_status,list_date,delist_date,is_hs",
        )

    def fetch_trade_calendar(self, exchange: str, start_date: str, end_date: str) -> pd.DataFrame:
        return self.pro.trade_cal(exchange=exchange, start_date=start_date, end_date=end_date)

    def fetch_stock_daily(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        return self.pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)

    def fetch_stock_daily_by_date(self, trade_date: str) -> pd.DataFrame:
        return self.pro.daily(trade_date=trade_date)

    def fetch_index_daily(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        return self.pro.index_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)

    def fetch_index_daily_by_date(self, trade_date: str) -> pd.DataFrame:
        return self.pro.index_daily(trade_date=trade_date)

    def fetch_industry_board_list(self, src: str = "SW") -> pd.DataFrame:
        return self.pro.index_classify(src=src)

    def fetch_industry_board_members(self, index_code: str, src: str = "SW") -> pd.DataFrame:
        return self.pro.index_member(index_code=index_code, src=src)

    def fetch_moneyflow(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        return self.pro.moneyflow(ts_code=ts_code, start_date=start_date, end_date=end_date)

    def fetch_moneyflow_by_date(self, trade_date: str) -> pd.DataFrame:
        return self.pro.moneyflow(trade_date=trade_date)

    def fetch_adj_factor(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        return self.pro.adj_factor(ts_code=ts_code, start_date=start_date, end_date=end_date)

    def fetch_adj_factor_by_date(self, trade_date: str) -> pd.DataFrame:
        return self.pro.adj_factor(trade_date=trade_date)
