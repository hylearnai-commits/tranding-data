# Trading Data Center

一个基于 FastAPI + Tushare 的个人交易数据中心，目标是把常用交易数据落到自己的数据库，通过统一 API 和 SDK 给量化项目使用。

## 当前能力

- 股票基础信息、交易日历、股票日线
- 指数日线
- 行业板块与板块成分
- 个股资金流
- 任务调度、任务运行记录、失败重放
- API Key 鉴权与限流

## 技术栈

- FastAPI
- SQLAlchemy
- APScheduler
- Tushare
- SQLite（默认，可切换）

## 快速开始

1. 安装依赖

```bash
pip install -r requirements.txt
```

2. 配置环境变量（复制 `.env.example` 为 `.env` 并填写）

关键配置：

- `TUSHARE_TOKEN`
- `DATABASE_URL`
- `AUTH_ENABLED`
- `API_KEYS`
- `RATE_LIMIT_PER_MINUTE`

3. 启动服务

```bash
python -m uvicorn app.main:app --host 127.0.0.1 --port 8099
```

4. 健康检查

```bash
GET /health
```

## 鉴权与限流

- 当 `AUTH_ENABLED=false` 时，`/api/v1/*` 无需 API Key
- 当 `AUTH_ENABLED=true` 时，`/api/v1/*` 需要请求头：
  - `X-API-Key: your_key`
- 限流基于 API Key，按每分钟窗口计数，阈值由 `RATE_LIMIT_PER_MINUTE` 控制

## 调度任务

默认已内置以下定时任务：

- `sync_basic_cron`：股票基础信息
- `sync_calendar_cron`：交易日历
- `sync_daily_cron`：股票日线增量
- `sync_index_cron`：指数日线增量
- `sync_moneyflow_cron`：资金流增量

## Python SDK

```python
from app.sdk import TradingDataClient

client = TradingDataClient("http://127.0.0.1:8099", api_key="your_key")
print(client.health())
print(client.get_stock_daily("000001.SZ", "20260301", "20260331", 50))
print(client.get_index_daily("000300.SH", "20260301", "20260331", 50))
```

## 文档

- API 使用文档见 [API.md](file:///d:/repo/tranding-data/API.md)
