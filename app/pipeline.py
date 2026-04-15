from __future__ import annotations

import logging
import time

from .config import AppConfig
from .db import Database
from .exporter import write_export
from .models import CandidateProxy
from .probe import ProxyProbe
from .subscriptions import ingest_subscriptions

LOGGER = logging.getLogger(__name__)


async def _refresh_candidates(config: AppConfig, db: Database, probe: ProxyProbe) -> int:
    source_urls = list(config.subscription_urls)
    LOGGER.info("Loaded %s source URLs from config", len(source_urls))

    upserted = await ingest_subscriptions(source_urls, db, probe.toolchain)
    total_candidates = db.count_proxies()

    LOGGER.info(
        "Subscriptions refreshed. upserted=%s total_candidates_in_db=%s",
        upserted,
        total_candidates,
    )
    return total_candidates


async def _url_test_stage(
    config: AppConfig,
    db: Database,
    probe: ProxyProbe,
) -> list[CandidateProxy]:
    speed_candidates: list[CandidateProxy] = []
    dead_after_url: list[tuple[str, str]] = []
    dead_flush_size = 1000
    total_url_ok = 0
    total_url_fail = 0

    async def _candidates():
        for row in db.stream_proxies(fetch_size=256):
            yield CandidateProxy.from_row(row)

    async for chunk in probe.url_test_stream(
        _candidates(),
        config.url_test_url,
        config.url_timeout_seconds,
        config.test_attempts,
        config.url_batch_size,
    ):
        db.mark_url_results([res for _, res in chunk])

        for candidate, res in chunk:
            in_excluded_country = res.country in config.exclude_countries
            if not res.success or in_excluded_country:
                total_url_fail += 1
                reason = (
                    "excluded_country"
                    if in_excluded_country
                    else (res.reason or "url_test_failed")
                )
                dead_after_url.append((res.proxy_hash, reason))
                if len(dead_after_url) >= dead_flush_size:
                    db.mark_dead_many(dead_after_url, ttl_days=config.dead_ttl_days)
                    dead_after_url.clear()
                continue

            total_url_ok += 1
            speed_candidates.append(candidate)

    if dead_after_url:
        db.mark_dead_many(dead_after_url, ttl_days=config.dead_ttl_days)

    LOGGER.info("URL stage complete: ok=%s fail=%s", total_url_ok, total_url_fail)
    return speed_candidates


async def run_once(config: AppConfig, db: Database, probe: ProxyProbe) -> None:
    start_time = time.perf_counter()

    LOGGER.info("Initializing DB schema")
    db.init_schema()
    cleaned = db.cleanup_expired_dead()
    LOGGER.info("Expired dead proxies cleaned: %s", cleaned)

    candidates_count = await _refresh_candidates(config, db, probe)
    if candidates_count <= 0:
        LOGGER.warning("No candidates available after refresh")
        db.store_selected([])
        write_export(config.export_file, db)
        return

    LOGGER.info("Starting URL test stage. total_candidates=%s", candidates_count)
    speed_candidates = await _url_test_stage(config, db, probe)
    LOGGER.info("Selected for speed stage: %s", len(speed_candidates))

    dead_after_speed: list[tuple[str, str]] = []
    for speed_chunk in _chunk_candidates(speed_candidates, max(1, config.speed_batch_size)):
        speed_results = await probe.speed_test_batch(
            speed_chunk,
            config.speed_test_url,
            config.url_timeout_seconds,
            config.speed_timeout_seconds,
            config.test_attempts,
            config.speed_batch_size,
        )

        db.mark_speed_results(speed_results)

        for res in speed_results:
            if not res.success:
                dead_after_speed.append((res.proxy_hash, res.reason or "speed_test_failed"))
                continue
            if (res.mbps or 0.0) < config.speed_min_mb_s:
                dead_after_speed.append((res.proxy_hash, "below_speed_threshold"))

        if len(dead_after_speed) >= 500:
            db.mark_dead_many(dead_after_speed, ttl_days=max(1, config.dead_ttl_days // 2))
            dead_after_speed.clear()

    if dead_after_speed:
        db.mark_dead_many(dead_after_speed, ttl_days=max(1, config.dead_ttl_days // 2))

    final_selection = db.select_top_speed(
        limit=config.target_final_count,
        min_speed_mb_s=config.speed_min_mb_s,
    )

    LOGGER.info("Final selection size: %s", len(final_selection))
    db.store_selected(final_selection)

    end_time = time.perf_counter()

    write_export(
        config.export_file,
        db,
        {"elapsed_time": end_time - start_time, "candidates": candidates_count},
    )
    LOGGER.info("Export saved to %s", config.export_file)
    LOGGER.info("Pipeline completed.")


def _chunk_candidates(candidates: list[CandidateProxy], chunk_size: int):
    for idx in range(0, len(candidates), chunk_size):
        yield candidates[idx : idx + chunk_size]
