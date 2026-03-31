# proxy-tester

Nightly pipeline for collecting subscription links, filtering dead proxies, URL testing, speed testing, and exporting a final list of 25 entries with metadata comments.

## Features implemented

- SQLite schema and lifecycle for proxies, URL tests, speed tests, dead-list TTL, and selected final proxies.
- Dead-list cleanup on each run.
- Subscription loading from JSON config (`subscription_urls`).
- Candidate parsing and deduplication via external ProxyConverter.
- URL test + speed test orchestration with batched checks via Xray-core runtime.
- Configurable retry count for URL tests (`url_test_attempts`), keeping the best latency per proxy.
- Speed-test failures are excluded from final selection (no fallback with `NULL` speed for failed speed checks).
- Progress bars for long-running stages via `tqdm`.
- Local-file GeoIP enrichment (MaxMind `.mmdb`) for exit IP metadata.
- Export format with comment replacement (`link # IP=... | Geo=... | URL=... | Speed=...`).
- CLI entrypoint (`python main.py`).

## Run

```bash
pip install -r requirements.txt
cp config.json.example config.json
python main.py --config config.json --verbose
```

## GeoIP database

Configure path in `AppConfig.geoip_db_path`. If `AppConfig.geoip_db_url` is set, the `.mmdb` file is downloaded at startup to that path. During tests and lookups only the local file path is used.

## Scheduler

Recommended cron (once per day at 03:10):

```cron
10 3 * * * cd /path/to/repo && /usr/bin/python3 main.py --config config.json >> nightly.log 2>&1
```

## Config file

Only JSON config is supported. Start from `config.json.example` and update values for your environment.


## Runtime binaries

The app downloads and uses the latest releases of:

- `XTLS/Xray-core` (runtime core)
- `maksp86/ProxyConverter` (subscription link -> Xray JSON conversion)

Binaries are cached under `.bin/` in the project root.

HTTP checks are executed from Python (`aiohttp`) through Xray local proxy inbounds, without external `curl`.
