import argparse
import json
import sys
import time
from pathlib import Path

import requests

sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.services.tushare_client import TushareClient


def request_json(method: str, url: str, timeout: int = 120):
    try:
        resp = requests.request(method=method, url=url, timeout=timeout)
    except requests.RequestException as e:
        return 599, {"error": str(e)}
    try:
        data = resp.json()
    except Exception:
        data = {"raw": resp.text}
    return resp.status_code, data


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8099")
    parser.add_argument("--exchange", default="SSE")
    parser.add_argument("--start-date", default="19900101")
    parser.add_argument("--end-date", default=time.strftime("%Y%m%d"))
    parser.add_argument("--sleep-seconds", type=float, default=0.7)
    parser.add_argument("--retry-sleep-seconds", type=float, default=30.0)
    parser.add_argument("--max-retries-per-date", type=int, default=0)
    parser.add_argument("--checkpoint", default="scripts/.full_market_backfill_checkpoint.json")
    parser.add_argument("--request-timeout", type=int, default=180)
    args = parser.parse_args()

    base_url = args.base_url.rstrip("/")
    checkpoint_path = Path(args.checkpoint)
    completed = set()
    if checkpoint_path.exists():
        try:
            completed = set(json.loads(checkpoint_path.read_text(encoding="utf-8")).get("completed_dates", []))
        except Exception:
            completed = set()

    while True:
        status_code, health_data = request_json("GET", f"{base_url}/health", timeout=10)
        if status_code == 200:
            break
        print(
            json.dumps(
                {
                    "ok": False,
                    "stage": "health_check",
                    "status": status_code,
                    "data": health_data,
                    "retry_sleep_seconds": args.retry_sleep_seconds,
                },
                ensure_ascii=False,
            )
        )
        time.sleep(args.retry_sleep_seconds)

    client = TushareClient()
    while True:
        try:
            cal_df = client.fetch_trade_calendar(exchange=args.exchange, start_date=args.start_date, end_date=args.end_date)
            break
        except Exception as e:
            print(
                json.dumps(
                    {
                        "ok": False,
                        "stage": "fetch_trade_calendar",
                        "error": str(e),
                        "retry_sleep_seconds": args.retry_sleep_seconds,
                    },
                    ensure_ascii=False,
                )
            )
            time.sleep(args.retry_sleep_seconds)
    cal_df = cal_df[cal_df["is_open"] == 1]
    trade_dates = sorted(cal_df["cal_date"].astype(str).tolist())
    pending_dates = [d for d in trade_dates if d not in completed]

    print(
        json.dumps(
            {
                "exchange": args.exchange,
                "start_date": args.start_date,
                "end_date": args.end_date,
                "total_trade_days": len(trade_dates),
                "completed_trade_days": len(completed),
                "pending_trade_days": len(pending_dates),
            },
            ensure_ascii=False,
        )
    )

    total_pending = len(pending_dates)
    for i, trade_date in enumerate(pending_dates, start=1):
        tasks = [
            f"/api/v1/jobs/sync/stock-daily/by-date?trade_date={trade_date}",
            f"/api/v1/jobs/sync/index-daily/by-date?trade_date={trade_date}",
            f"/api/v1/jobs/sync/moneyflow/by-date?trade_date={trade_date}",
            f"/api/v1/jobs/sync/adj-factor/by-date?trade_date={trade_date}",
        ]
        retry_count = 0
        while True:
            ok = True
            stats = {}
            for path in tasks:
                code, data = request_json("POST", f"{base_url}{path}", timeout=args.request_timeout)
                stats[path] = {"status": code, "data": data}
                if code >= 400:
                    ok = False
                    break
                time.sleep(args.sleep_seconds)
            if ok:
                break
            retry_count += 1
            print(
                json.dumps(
                    {
                        "trade_date": trade_date,
                        "ok": False,
                        "retry_count": retry_count,
                        "retry_sleep_seconds": args.retry_sleep_seconds,
                        "stats": stats,
                    },
                    ensure_ascii=False,
                )
            )
            if args.max_retries_per_date > 0 and retry_count >= args.max_retries_per_date:
                raise RuntimeError(f"回填失败且超过最大重试次数: {trade_date}")
            time.sleep(args.retry_sleep_seconds)

        completed.add(trade_date)
        checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
        checkpoint_path.write_text(
            json.dumps({"completed_dates": sorted(completed), "last_trade_date": trade_date}, ensure_ascii=False),
            encoding="utf-8",
        )
        print(
            json.dumps(
                {
                    "trade_date": trade_date,
                    "ok": True,
                    "progress": f"{i}/{total_pending}",
                    "retry_count": retry_count,
                    "stats": stats,
                },
                ensure_ascii=False,
            )
        )
        time.sleep(args.sleep_seconds)

    print(
        json.dumps(
            {
                "done": True,
                "exchange": args.exchange,
                "start_date": args.start_date,
                "end_date": args.end_date,
                "completed_trade_days": len(completed),
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
