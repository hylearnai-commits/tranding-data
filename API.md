# API 使用文档

Base URL 示例：`http://127.0.0.1:8099`

## 认证

- 当 `AUTH_ENABLED=true` 时，请在所有 `/api/v1/*` 请求中添加：
  - `X-API-Key: your_key`
- `/health` 不受认证限制

## 通用说明

- 日期格式：`YYYYMMDD`
- 同步接口统一返回：

```json
{"inserted": 0, "updated": 0}
```

---

## 1) 基础查询

### 股票基础信息

`GET /api/v1/basic/stock`

参数：

- `list_status`，默认 `L`
- `limit`，默认 `100`

### 交易日历

`GET /api/v1/calendar/trade-days`

参数：

- `exchange`，默认 `SSE`
- `start_date`
- `end_date`
- `is_open`，默认 `1`

---

## 2) 行情与交易数据查询

### 股票日线

`GET /api/v1/market/daily`

参数：

- `ts_code`
- `start_date`
- `end_date`
- `limit`，默认 `2000`

### 指数日线

`GET /api/v1/market/index-daily`

参数：

- `ts_code`
- `start_date`
- `end_date`
- `limit`，默认 `2000`

### 资金流

`GET /api/v1/market/moneyflow`

参数：

- `ts_code`
- `start_date`
- `end_date`
- `limit`，默认 `2000`

### 行业板块列表

`GET /api/v1/board/industry`

参数：

- `src`，默认 `SW`
- `limit`，默认 `1000`

### 行业板块成分

`GET /api/v1/board/industry/members`

参数：

- `index_code`
- `limit`，默认 `3000`

---

## 3) 同步任务接口

### 股票基础

- `POST /api/v1/jobs/sync/stock-basic`

### 交易日历

- `POST /api/v1/jobs/sync/trade-calendar?exchange=SSE`

### 股票日线

- `POST /api/v1/jobs/sync/stock-daily?ts_code=000001.SZ&start_date=20260301&end_date=20260331`
- `POST /api/v1/jobs/sync/stock-daily/by-date?trade_date=20260331`
- `POST /api/v1/jobs/sync/stock-daily/incremental?exchange=SSE&lookback_days=3`

### 指数日线

- `POST /api/v1/jobs/sync/index-daily?ts_code=000300.SH&start_date=20260301&end_date=20260331`
- `POST /api/v1/jobs/sync/index-daily/by-date?trade_date=20260331`

### 行业板块

- `POST /api/v1/jobs/sync/industry/boards?src=SW`
- `POST /api/v1/jobs/sync/industry/members?src=SW`
- `POST /api/v1/jobs/sync/industry/members?index_code=801010.SI&src=SW`

### 资金流

- `POST /api/v1/jobs/sync/moneyflow?ts_code=000001.SZ&start_date=20260301&end_date=20260331`
- `POST /api/v1/jobs/sync/moneyflow/by-date?trade_date=20260331`

---

## 4) 数据质量

### 股票日线质量检查

`GET /api/v1/quality/stock-daily?start_date=20260301&end_date=20260331&exchange=SSE`

返回字段：

- `expected_trade_days`
- `existing_trade_days`
- `missing_trade_days`
- `invalid_price_rows`

---

## 5) 任务运维

### 任务运行分页查询

`GET /api/v1/jobs/runs?limit=100&cursor=&job_name=&status=`

返回结构：

```json
{
  "items": [],
  "next_cursor": 123
}
```

### 重放任务

`POST /api/v1/jobs/runs/{job_run_id}/replay`

---

## 常见状态码

- `200` 成功
- `400` 参数错误
- `401` API Key 无效或缺失
- `409` 任务锁冲突（同名任务并发）
- `429` 超过限流
- `500` 服务内部错误
