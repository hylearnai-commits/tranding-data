# API 使用文档

Base URL 示例：`http://127.0.0.1:8099`

## 0. 快速调用

当开启鉴权时：

```bash
curl -H "X-API-Key: your_key" "http://127.0.0.1:8099/api/v1/jobs/runs?limit=5"
```

未开启鉴权时：

```bash
curl "http://127.0.0.1:8099/api/v1/jobs/runs?limit=5"
```

## 1. 认证与限流

- `/health` 不受鉴权限制
- 其余接口都在 `/api/v1/*`
- 当 `AUTH_ENABLED=true` 时，必须带请求头：
  - `X-API-Key: your_key`
- 当 `AUTH_ENABLED=false` 时，不需要 API Key
- 限流按 API Key 统计，窗口 60 秒，阈值由 `RATE_LIMIT_PER_MINUTE` 控制

常见返回：

- `401`：无 API Key 或 key 无效
- `429`：超过频率限制

## 2. 通用约定

- 日期参数格式统一为 `YYYYMMDD`
- 除 `/health` 外，接口前缀统一为 `/api/v1`
- 同步任务统一返回：

```json
{"inserted": 0, "updated": 0}
```

- 分页接口统一返回：

```json
{
  "items": [],
  "next_cursor": 123
}
```

## 3. 查询接口

### 3.1 股票基础信息

`GET /api/v1/basic/stock`

参数：

- `list_status`：默认 `L`
- `limit`：默认 `100`，范围 `1-5000`

示例：

```bash
curl "http://127.0.0.1:8099/api/v1/basic/stock?list_status=L&limit=20"
```

---

### 3.2 交易日历

`GET /api/v1/calendar/trade-days`

参数：

- `exchange`：默认 `SSE`
- `start_date`：必填
- `end_date`：必填
- `is_open`：默认 `1`，可选 `0/1`

示例请求：

```bash
curl "http://127.0.0.1:8099/api/v1/calendar/trade-days?exchange=SSE&start_date=20260301&end_date=20260310&is_open=1"
```

示例响应：

```json
[
  {"exchange":"SSE","cal_date":"20260302","is_open":1,"pretrade_date":"20260227"},
  {"exchange":"SSE","cal_date":"20260303","is_open":1,"pretrade_date":"20260302"}
]
```

---

### 3.3 股票日线

`GET /api/v1/market/daily`

参数：

- `ts_code`：必填，如 `000001.SZ`
- `start_date`：必填
- `end_date`：必填
- `limit`：默认 `2000`，范围 `1-10000`

返回字段核心：

- `ts_code, trade_date, open, high, low, close, pre_close, change, pct_chg, vol, amount`

示例请求：

```bash
curl "http://127.0.0.1:8099/api/v1/market/daily?ts_code=000001.SZ&start_date=20260301&end_date=20260331&limit=3"
```

示例响应：

```json
[
  {"ts_code":"000001.SZ","trade_date":"20260331","open":11.16,"high":11.28,"low":11.02,"close":11.1,"pre_close":11.22,"change":-0.12,"pct_chg":-1.0695,"vol":1465030.45,"amount":1629087.65}
]
```

---

### 3.4 指数日线

`GET /api/v1/market/index-daily`

参数：

- `ts_code`：必填，如 `000300.SH`
- `start_date`：必填
- `end_date`：必填
- `limit`：默认 `2000`，范围 `1-10000`

示例请求：

```bash
curl "http://127.0.0.1:8099/api/v1/market/index-daily?ts_code=000300.SH&start_date=20260301&end_date=20260331&limit=3"
```

示例响应：

```json
[
  {"ts_code":"000300.SH","trade_date":"20260331","close":4450.0493,"open":4491.9209,"high":4523.1745,"low":4450.0493,"pre_close":4491.95,"change":-41.9007,"pct_chg":-0.9328,"vol":224541585.0,"amount":484558113.011}
]
```

---

### 3.5 资金流

`GET /api/v1/market/moneyflow`

参数：

- `ts_code`：必填
- `start_date`：必填
- `end_date`：必填
- `limit`：默认 `2000`，范围 `1-10000`

返回字段核心：

- 中小单/中单/大单/特大单买卖量与金额
- `net_mf_vol, net_mf_amount`

示例请求：

```bash
curl "http://127.0.0.1:8099/api/v1/market/moneyflow?ts_code=000001.SZ&start_date=20260301&end_date=20260331&limit=1"
```

示例响应：

```json
[
  {"ts_code":"000001.SZ","trade_date":"20260331","buy_sm_vol":220384.0,"buy_sm_amount":24495.25,"sell_sm_vol":267507.0,"sell_sm_amount":29751.29,"buy_md_vol":308935.0,"buy_md_amount":34346.84,"sell_md_vol":332617.0,"sell_md_amount":36989.78,"buy_lg_vol":341029.0,"buy_lg_amount":37917.02,"sell_lg_vol":283239.0,"sell_lg_amount":31484.68,"buy_elg_vol":294218.0,"buy_elg_amount":32708.46,"sell_elg_vol":281202.0,"sell_elg_amount":31241.82,"net_mf_vol":231670.0,"net_mf_amount":25828.44}
]
```

---

### 3.6 行业板块列表

`GET /api/v1/board/industry`

参数：

- `src`：默认 `SW`
- `limit`：默认 `1000`，范围 `1-5000`

返回字段核心：

- `index_code, industry_name, level, industry_code, src`

示例请求：

```bash
curl "http://127.0.0.1:8099/api/v1/board/industry?src=SW&limit=3"
```

示例响应（上游有数据时）：

```json
[
  {"index_code":"801010.SI","industry_name":"农林牧渔","level":"L1","industry_code":"801010","src":"SW"},
  {"index_code":"801020.SI","industry_name":"采掘","level":"L1","industry_code":"801020","src":"SW"}
]
```

---

### 3.7 行业板块成分

`GET /api/v1/board/industry/members`

参数：

- `index_code`：必填
- `limit`：默认 `3000`，范围 `1-10000`

返回字段核心：

- `index_code, con_code, con_name, in_date, out_date, is_new`

示例请求：

```bash
curl "http://127.0.0.1:8099/api/v1/board/industry/members?index_code=801010.SI&limit=3"
```

示例响应（上游有数据时）：

```json
[
  {"index_code":"801010.SI","con_code":"000998.SZ","con_name":"隆平高科","in_date":"20240101","out_date":null,"is_new":"1"}
]
```

---

### 3.8 复权因子

`GET /api/v1/market/adj-factor`

参数：

- `ts_code`：必填，如 `000001.SZ`
- `start_date`：必填
- `end_date`：必填
- `limit`：默认 `3000`，范围 `1-10000`

示例请求：

```bash
curl "http://127.0.0.1:8099/api/v1/market/adj-factor?ts_code=000001.SZ&start_date=20260301&end_date=20260331&limit=3"
```

示例响应：

```json
[
  {"ts_code":"000001.SZ","trade_date":"20260331","adj_factor":134.5794},
  {"ts_code":"000001.SZ","trade_date":"20260330","adj_factor":134.5794}
]
```

---

### 3.9 复权价（前复权/后复权）

`GET /api/v1/market/daily/adjusted`

参数：

- `ts_code`：必填
- `start_date`：必填
- `end_date`：必填
- `adj_type`：默认 `qfq`，可选 `qfq|hfq`
- `limit`：默认 `2000`，范围 `1-10000`

说明：

- `adj_type=qfq`：前复权
- `adj_type=hfq`：后复权

示例请求：

```bash
curl "http://127.0.0.1:8099/api/v1/market/daily/adjusted?ts_code=000001.SZ&start_date=20260301&end_date=20260331&adj_type=qfq&limit=2"
```

示例响应：

```json
[
  {"ts_code":"000001.SZ","trade_date":"20260331","adj_type":"qfq","factor":134.5794,"open":11.0,"high":11.18,"low":10.99,"close":11.08,"pre_close":10.99,"change":0.09,"pct_chg":0.8189,"vol":1164565.34,"amount":1294675.716}
]
```

## 4. 同步任务接口

## 4.1 基础与日历

- `POST /api/v1/jobs/sync/stock-basic`
- `POST /api/v1/jobs/sync/trade-calendar?exchange=SSE`

示例请求：

```bash
curl -X POST "http://127.0.0.1:8099/api/v1/jobs/sync/stock-basic"
curl -X POST "http://127.0.0.1:8099/api/v1/jobs/sync/trade-calendar?exchange=SSE"
```

示例响应：

```json
{"inserted":0,"updated":5390}
```

## 4.2 股票日线

- 区间同步  
  `POST /api/v1/jobs/sync/stock-daily?ts_code=000001.SZ&start_date=20260301&end_date=20260331`
- 按交易日全市场同步  
  `POST /api/v1/jobs/sync/stock-daily/by-date?trade_date=20260331`
- 增量同步（依赖交易日历）  
  `POST /api/v1/jobs/sync/stock-daily/incremental?exchange=SSE&lookback_days=3`

示例响应：

```json
{"inserted":5482,"updated":0}
```

## 4.3 指数日线

- 区间同步  
  `POST /api/v1/jobs/sync/index-daily?ts_code=000300.SH&start_date=20260301&end_date=20260331`
- 按交易日同步（默认指数池 + 已有指数代码）  
  `POST /api/v1/jobs/sync/index-daily/by-date?trade_date=20260331`

示例响应：

```json
{"inserted":5,"updated":0}
```

## 4.4 行业板块

- 同步板块列表  
  `POST /api/v1/jobs/sync/industry/boards?src=SW`
- 同步全部板块成分（先确保板块列表已同步）  
  `POST /api/v1/jobs/sync/industry/members?src=SW`
- 同步单个板块成分  
  `POST /api/v1/jobs/sync/industry/members?index_code=801010.SI&src=SW`

示例响应：

```json
{"inserted":0,"updated":0}
```

## 4.5 资金流

- 区间同步  
  `POST /api/v1/jobs/sync/moneyflow?ts_code=000001.SZ&start_date=20260301&end_date=20260331`
- 按交易日全市场同步  
  `POST /api/v1/jobs/sync/moneyflow/by-date?trade_date=20260331`

示例响应：

```json
{"inserted":5179,"updated":0}
```

## 4.6 复权因子

- 区间同步  
  `POST /api/v1/jobs/sync/adj-factor?ts_code=000001.SZ&start_date=20260301&end_date=20260331`
- 按交易日全市场同步  
  `POST /api/v1/jobs/sync/adj-factor/by-date?trade_date=20260331`

示例响应：

```json
{"inserted":22,"updated":0}
```

## 4.7 自动回补

- 自动发现缺失交易日并回补（股票日线/指数日线/资金流）  
  `POST /api/v1/jobs/backfill/recent?exchange=SSE&lookback_days=10&max_backfill_days=5`

参数：

- `exchange`：默认 `SSE`
- `lookback_days`：默认 `10`，范围 `1-180`
- `max_backfill_days`：默认 `5`，范围 `1-60`

示例响应：

```json
{"inserted":0,"updated":0}
```

## 5. 质量检查

### 股票日线质量检查

`GET /api/v1/quality/stock-daily?start_date=20260301&end_date=20260331&exchange=SSE`

字段说明：

- `expected_trade_days`：交易日历中的应有交易日数
- `existing_trade_days`：数据库已有日线的交易日数
- `missing_trade_days`：缺失交易日数
- `invalid_price_rows`：价格合法性异常记录数

示例响应：

```json
{"start_date":"20260301","end_date":"20260331","exchange":"SSE","expected_trade_days":22,"existing_trade_days":22,"missing_trade_days":0,"invalid_price_rows":0}
```

## 6. 任务运维接口

### 6.1 任务运行分页查询

`GET /api/v1/jobs/runs?limit=100&cursor=&job_name=&status=`

参数：

- `limit`：默认 `100`，范围 `1-500`
- `cursor`：上一页返回的 `next_cursor`
- `job_name`：按任务名过滤
- `status`：按状态过滤（如 `success/failed/running`）

返回 `items` 字段核心：

- `id, job_name, status, attempts, inserted, updated`
- `replay_of_job_run_id`：若为重放任务，会指向原任务 id
- `job_payload`：任务参数快照
- `error_message, started_at, finished_at`

示例请求：

```bash
curl "http://127.0.0.1:8099/api/v1/jobs/runs?limit=2"
```

示例响应：

```json
{
  "items":[
    {"id":12,"job_name":"sync_moneyflow_by_date_20260331","status":"success","attempts":1,"inserted":0,"updated":5179,"replay_of_job_run_id":null,"job_payload":"{\"trade_date\":\"20260331\"}","error_message":null,"started_at":"2026-04-01T08:20:01.123456","finished_at":"2026-04-01T08:20:04.778899"}
  ],
  "next_cursor":12
}
```

---

### 6.2 任务重放

`POST /api/v1/jobs/runs/{job_run_id}/replay`

说明：

- 会读取原任务 `job_payload` 重新执行
- 当前支持已接入同步体系的任务类型
- 若同名任务正在执行，可能返回 `409`（任务锁冲突）

示例请求：

```bash
curl -X POST "http://127.0.0.1:8099/api/v1/jobs/runs/12/replay"
```

示例响应：

```json
{"inserted":0,"updated":5179}
```

## 7. 可观测性接口

### 7.1 指标快照

`GET /api/v1/ops/metrics`

返回包含：

- `request`：请求总量、平均耗时、QPS、状态码分布、路径分布
- `job`：任务总量、成功率、平均耗时、按任务名统计

示例请求：

```bash
curl "http://127.0.0.1:8099/api/v1/ops/metrics"
```

示例响应：

```json
{
  "request":{"total":6,"avg_latency_ms":9.447,"qps_10s":0.2,"qps_60s":0.1,"by_status":{"200":6},"by_path":{"/api/v1/ops/metrics":1}},
  "job":{"total":2,"success":2,"failed":0,"success_rate":1.0,"avg_duration_ms":2847.631,"by_name":{"sync_adj_factor_000001.SZ":{"total":1,"success":1,"failed":0,"duration_ms_sum":1980.01}}}
}
```

### 7.2 链路追踪头

- 可在请求中传 `X-Trace-Id`
- 服务会在响应头回传 `X-Trace-Id`

示例请求：

```bash
curl -H "X-Trace-Id: reg-test-trace" "http://127.0.0.1:8099/api/v1/jobs/runs?limit=1" -i
```

## 8. 状态码

- `200`：成功
- `400`：参数错误或业务校验失败
- `401`：API Key 无效或缺失
- `404`：资源不存在（如重放目标任务不存在）
- `409`：任务锁冲突（同名任务并发）
- `429`：超过限流
- `500`：服务内部错误

### 8.1 错误响应结构（机读）

所有错误响应统一为：

```json
{
  "error": {
    "code": "INVALID_DATE_FORMAT",
    "type": "validation_error",
    "message": "start_date/end_date 需为YYYYMMDD",
    "trace_id": "reg-test-trace"
  }
}
```

字段说明：

- `code`：稳定错误码，客户端建议以此做分支处理
- `type`：错误类型（validation_error / auth_error / resource_error / conflict_error / throttle_error / internal_error）
- `message`：面向人类的错误说明
- `trace_id`：链路追踪ID，便于日志排查

### 8.2 错误码字典

- `BAD_REQUEST`：通用请求错误（400）
- `INVALID_DATE_FORMAT`：日期格式错误（400）
- `INVALID_ARGUMENT`：参数校验失败（400）
- `UNAUTHORIZED`：鉴权失败（401）
- `FORBIDDEN`：无权限访问（403）
- `NOT_FOUND`：资源不存在（404）
- `JOB_LOCK_CONFLICT`：任务锁冲突（409）
- `RATE_LIMITED`：请求被限流（429）
- `INTERNAL_ERROR`：服务内部错误（500）

### 8.3 客户端错误处理示例（Python SDK）

```python
from app.sdk import (
    TradingDataClient,
    InvalidDateFormatError,
    NotFoundError,
    RateLimitedError,
    TradingDataSDKError,
)

client = TradingDataClient("http://127.0.0.1:8099", api_key="your_key")

try:
    result = client.sync_adj_factor("000001.SZ", "20260301", "20260331")
    print("sync ok:", result)
except InvalidDateFormatError as e:
    print("日期参数错误", e.code, e.trace_id)
except RateLimitedError as e:
    print("请求过快，稍后重试", e.code, e.trace_id)
except NotFoundError as e:
    print("资源不存在", e.code, e.trace_id)
except TradingDataSDKError as e:
    print("业务错误", e.code, e.status_code, e.trace_id)
```

同步任务 SDK 常用方法：

- `sync_stock_basic()`
- `sync_trade_calendar(exchange)`
- `sync_stock_daily(ts_code, start_date, end_date)`
- `sync_stock_daily_by_date(trade_date)`
- `sync_stock_daily_incremental(exchange, lookback_days)`
- `sync_index_daily(ts_code, start_date, end_date)`
- `sync_index_daily_by_date(trade_date)`
- `sync_industry_boards(src)`
- `sync_industry_members(index_code=None, src="SW")`
- `sync_moneyflow(ts_code, start_date, end_date)`
- `sync_moneyflow_by_date(trade_date)`
- `sync_adj_factor(ts_code, start_date, end_date)`
- `sync_adj_factor_by_date(trade_date)`
- `backfill_recent(exchange, lookback_days, max_backfill_days)`

## 9. 常见问题

### Q1: 行业板块数据为什么可能是空？

- 可能是上游权限或积分限制
- 建议先调用 `POST /jobs/sync/industry/boards` 看返回计数，再拉查询接口

### Q2: 按日增量同步为什么没数据？

- 增量同步依赖 `trade_calendar`，先同步交易日历
- 确认 `exchange` 与交易日范围配置正确

### Q3: 同步返回 `inserted=0, updated>0` 是异常吗？

- 不是，表示该批数据已存在，本次执行以更新为主

### Q4: 自动回补没补到数据是失败吗？

- 不一定，若近几天没有缺失交易日，返回 `inserted=0, updated=0` 是正常结果
