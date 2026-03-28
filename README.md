# proxy-tester

Nightly pipeline for collecting subscription links, filtering dead proxies, URL testing, speed testing, and exporting a final list of 25 entries with metadata comments.

## Features implemented

- SQLite schema and lifecycle for proxies, URL tests, speed tests, dead-list TTL, and selected final proxies.
- Dead-list cleanup on each run.
- Subscription source loading from `sources.txt`.
- Candidate parsing and deduplication.
- URL test + speed test orchestration with parallel batches.
- Export format with comment replacement (`link # IP=... | Geo=... | URL=... | Speed=...`).
- CLI entrypoint (`python main.py`).

> Note: `app/probe.py` is an adapter stub and should be replaced with real Xray/libXray/python_v2ray-backed checks.

## Run

```bash
python main.py --verbose
```

## Scheduler

Recommended cron (once per day at 03:10):

```cron
10 3 * * * cd /path/to/repo && /usr/bin/python3 main.py >> nightly.log 2>&1
```
