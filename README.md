# proxy-tester

`proxy-tester` collects proxy links from subscription URLs, checks them, and exports a final ranked list.

## How it works

1. **Load config**  
   Reads JSON settings (sources, limits, timeouts, output paths).

2. **Collect candidates**  
   Downloads subscription payloads, normalizes links, removes fragments after `#`, validates links via ProxyConverter, and deduplicates by SHA256 hash.

3. **Prepare pool**  
   Seeds candidates from recent successful records and fresh subscriptions, skipping proxies currently present in the dead list (`dead_proxies`) until TTL expiration.

4. **URL test stage**  
   Runs continuous URL checks through a bounded Xray worker pool (`xray_worker_count`) with worker recycling (`xray_tasks_per_worker`). For each proxy, stores the latest status/latency/geo metadata in `proxies`.

5. **Speed test stage**  
   Selects the best latency subset (`speed_top_n`) and runs rolling-concurrency download speed checks. Proxies that fail speed checks or threshold filters are moved to `dead_proxies`.

6. **Final selection and export**  
   Keeps only proxies that passed speed requirements, limits to `target_final_count`, stores them in `selected_proxies`, and writes the export file.

## Runtime model

- Uses SQLite (`WAL`) as persistent storage.
- Main tables:
  - `proxies`: current active/known proxy state
  - `dead_proxies`: temporarily excluded proxies with TTL
  - `selected_proxies`: last exported result set
- URL and speed checks are executed through a bounded Xray worker pool configured at startup; outbounds and routing rules are pushed over Xray API via stdin payloads.
