"""Small dependency-free HTTP load test for the dashboard API."""

import argparse
import json
import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.error import HTTPError, URLError
from urllib.request import urlopen


def request_once(url: str, timeout: float) -> tuple[float, int]:
    started = time.perf_counter()
    try:
        with urlopen(url, timeout=timeout) as response:
            response.read()
            status = response.status
    except HTTPError as exc:
        status = exc.code
    except URLError:
        status = 0
    return time.perf_counter() - started, status


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(round((len(ordered) - 1) * fraction)))
    return ordered[index]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://localhost:5000/api/health")
    parser.add_argument("--requests", type=int, default=200)
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument("--timeout", type=float, default=5.0)
    args = parser.parse_args()
    if args.requests < 1 or args.concurrency < 1:
        parser.error("requests and concurrency must be positive")

    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        futures = [
            executor.submit(request_once, args.url, args.timeout) for _ in range(args.requests)
        ]
        results = [future.result() for future in as_completed(futures)]
    elapsed = time.perf_counter() - started
    durations = [duration for duration, _ in results]
    successes = sum(200 <= status < 400 for _, status in results)
    report = {
        "url": args.url,
        "requests": args.requests,
        "concurrency": args.concurrency,
        "successes": successes,
        "errors": args.requests - successes,
        "requests_per_second": round(args.requests / elapsed, 2),
        "latency_ms": {
            "mean": round(statistics.fmean(durations) * 1000, 2),
            "p50": round(percentile(durations, 0.50) * 1000, 2),
            "p95": round(percentile(durations, 0.95) * 1000, 2),
            "p99": round(percentile(durations, 0.99) * 1000, 2),
        },
    }
    print(json.dumps(report, indent=2))
    if successes != args.requests:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
