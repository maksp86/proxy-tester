# CODEX Notes

## Application Overview

This project validates proxy links from subscriptions, stores probe data in SQLite, and exports a final selected proxy list.

Main flow:
1. Load JSON configuration.
2. Refresh subscriptions and upsert parsed proxies into the database.
3. Run URL reachability/latency checks through Xray.
4. Run speed tests for surviving candidates.
5. Persist final selection and write export output.

## Core Components

- `main.py`: CLI entry point and app bootstrap.
- `app/config.py`: Runtime config model and JSON loader.
- `app/subscriptions.py`: Subscription download, parse, deduplicate, and DB ingestion.
- `app/probe.py`: Xray-based URL and speed probing logic.
- `app/pipeline.py`: End-to-end orchestration of refresh, probing, filtering, and export.
- `app/db.py`: SQLite schema and persistence APIs.
- `app/exporter.py`: Final output file generation.

## Current Runtime Model

- Xray runs as a bounded worker pool.
- URL-stage tasks are fed continuously through a bounded queue sized as:
  `xray_worker_count * xray_tasks_per_worker`.
- URL results are accumulated inside `ProxyProbe` and emitted in chunks of 500 for DB flush.
- Xray API payloads are sent through `stdin`; temporary JSON files are not used.

## Development Principles

- Keep comments and text in English.
- Prefer streaming/continuous processing over large in-memory batches.
- Push filtering/sorting to SQLite when possible.
- Avoid uncontrolled process spawning; respect configured worker limits.
- Keep dead-proxy handling incremental to reduce memory pressure.
