param(
    [string]$HostName = "127.0.0.1",
    [int]$Port = 8099
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path ".\.env")) {
    if (Test-Path ".\.env.example") {
        Copy-Item ".\.env.example" ".\.env"
    }
}

python -m uvicorn app.main:app --host $HostName --port $Port --reload
