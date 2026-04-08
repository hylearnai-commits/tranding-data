#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8099}"
EXCHANGE="${EXCHANGE:-SSE}"
LOOKBACK_DAYS="${LOOKBACK_DAYS:-3}"
BACKFILL_LOOKBACK_DAYS="${BACKFILL_LOOKBACK_DAYS:-10}"
BACKFILL_MAX_DAYS="${BACKFILL_MAX_DAYS:-5}"
SKIP_BACKFILL="${SKIP_BACKFILL:-0}"

api() {
  local method="$1"
  local path="$2"
  curl -sS --fail -X "$method" "${BASE_URL}${path}"
}

today="$(date +%Y%m%d)"
start_date="$(date -d '-20 day' +%Y%m%d 2>/dev/null || python3 - <<'PY'
from datetime import datetime, timedelta
print((datetime.now() - timedelta(days=20)).strftime("%Y%m%d"))
PY
)"

cal_json="$(api GET "/api/v1/calendar/trade-days?exchange=${EXCHANGE}&start_date=${start_date}&end_date=${today}&is_open=1")"
latest_trade_date="$(python3 - <<'PY' "$cal_json" "$today"
import json, sys
data = json.loads(sys.argv[1])
fallback = sys.argv[2]
if not data:
    print(fallback)
else:
    print(sorted(x["cal_date"] for x in data)[-1])
PY
)"

echo "latest_trade_date=${latest_trade_date}"

run_task() {
  local name="$1"
  local method="$2"
  local path="$3"
  local resp
  resp="$(api "$method" "$path")"
  echo "${name} -> ${resp}"
}

run_task "stock_basic" "POST" "/api/v1/jobs/sync/stock-basic"
run_task "trade_calendar_sse" "POST" "/api/v1/jobs/sync/trade-calendar?exchange=SSE"
run_task "trade_calendar_szse" "POST" "/api/v1/jobs/sync/trade-calendar?exchange=SZSE"
run_task "stock_daily_incremental" "POST" "/api/v1/jobs/sync/stock-daily/incremental?exchange=${EXCHANGE}&lookback_days=${LOOKBACK_DAYS}"
run_task "stock_daily_by_date" "POST" "/api/v1/jobs/sync/stock-daily/by-date?trade_date=${latest_trade_date}"
run_task "index_daily_by_date" "POST" "/api/v1/jobs/sync/index-daily/by-date?trade_date=${latest_trade_date}"
run_task "moneyflow_by_date" "POST" "/api/v1/jobs/sync/moneyflow/by-date?trade_date=${latest_trade_date}"
run_task "adj_factor_by_date" "POST" "/api/v1/jobs/sync/adj-factor/by-date?trade_date=${latest_trade_date}"

if [[ "$SKIP_BACKFILL" != "1" ]]; then
  run_task "backfill_recent" "POST" "/api/v1/jobs/backfill/recent?exchange=${EXCHANGE}&lookback_days=${BACKFILL_LOOKBACK_DAYS}&max_backfill_days=${BACKFILL_MAX_DAYS}"
fi
