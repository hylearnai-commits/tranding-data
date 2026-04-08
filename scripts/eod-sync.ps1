param(
    [string]$BaseUrl = "http://127.0.0.1:8099",
    [string]$Exchange = "SSE",
    [int]$LookbackDays = 3,
    [int]$BackfillLookbackDays = 10,
    [int]$BackfillMaxDays = 5,
    [switch]$SkipBackfill
)

$ErrorActionPreference = "Stop"

function Invoke-Api {
    param(
        [string]$Method,
        [string]$Path
    )
    $url = "$BaseUrl$Path"
    try {
        $resp = Invoke-RestMethod -Method $Method -Uri $url -TimeoutSec 600
        return [PSCustomObject]@{
            ok = $true
            url = $url
            data = $resp
        }
    }
    catch {
        $body = $null
        if ($_.Exception.Response -and $_.Exception.Response.Content) {
            try {
                $body = $_.Exception.Response.Content.ReadAsStringAsync().GetAwaiter().GetResult()
            }
            catch {
                $body = $_.Exception.Message
            }
        }
        if (-not $body) {
            $body = $_.Exception.Message
        }
        return [PSCustomObject]@{
            ok = $false
            url = $url
            data = $body
        }
    }
}

$health = Invoke-Api -Method GET -Path "/health"
if (-not $health.ok) {
    throw "服务不可用: $($health.data)"
}

$today = Get-Date -Format "yyyyMMdd"
$startDate = (Get-Date).AddDays(-20).ToString("yyyyMMdd")
$cal = Invoke-Api -Method GET -Path "/api/v1/calendar/trade-days?exchange=$Exchange&start_date=$startDate&end_date=$today&is_open=1"
if (-not $cal.ok) {
    throw "获取交易日历失败: $($cal.data)"
}

$latestTradeDate = $today
if ($cal.data -and $cal.data.Count -gt 0) {
    $latestTradeDate = ($cal.data | ForEach-Object { $_.cal_date } | Sort-Object | Select-Object -Last 1)
}

$tasks = @(
    @{ name = "stock_basic"; method = "POST"; path = "/api/v1/jobs/sync/stock-basic" },
    @{ name = "trade_calendar_sse"; method = "POST"; path = "/api/v1/jobs/sync/trade-calendar?exchange=SSE" },
    @{ name = "trade_calendar_szse"; method = "POST"; path = "/api/v1/jobs/sync/trade-calendar?exchange=SZSE" },
    @{ name = "stock_daily_incremental"; method = "POST"; path = "/api/v1/jobs/sync/stock-daily/incremental?exchange=$Exchange&lookback_days=$LookbackDays" },
    @{ name = "stock_daily_by_date"; method = "POST"; path = "/api/v1/jobs/sync/stock-daily/by-date?trade_date=$latestTradeDate" },
    @{ name = "index_daily_by_date"; method = "POST"; path = "/api/v1/jobs/sync/index-daily/by-date?trade_date=$latestTradeDate" },
    @{ name = "moneyflow_by_date"; method = "POST"; path = "/api/v1/jobs/sync/moneyflow/by-date?trade_date=$latestTradeDate" },
    @{ name = "adj_factor_by_date"; method = "POST"; path = "/api/v1/jobs/sync/adj-factor/by-date?trade_date=$latestTradeDate" }
)

if (-not $SkipBackfill) {
    $tasks += @{ name = "backfill_recent"; method = "POST"; path = "/api/v1/jobs/backfill/recent?exchange=$Exchange&lookback_days=$BackfillLookbackDays&max_backfill_days=$BackfillMaxDays" }
}

$results = @()
foreach ($task in $tasks) {
    $r = Invoke-Api -Method $task.method -Path $task.path
    $results += [PSCustomObject]@{
        task = $task.name
        ok = $r.ok
        result = if ($r.ok) { ($r.data | ConvertTo-Json -Compress) } else { $r.data }
    }
}

Write-Host "latest_trade_date=$latestTradeDate"
$results | Format-Table -AutoSize

$failed = $results | Where-Object { -not $_.ok }
if ($failed.Count -gt 0) {
    throw "一键拉取完成，但有失败任务: $($failed.task -join ', ')"
}
