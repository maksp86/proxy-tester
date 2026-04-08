from __future__ import annotations

import logging
import time

from .config import AppConfig
from .db import Database
from .exporter import write_export
from .models import CandidateProxy
from .probe import ProxyProbe
from .subscriptions import collect_candidates

LOGGER = logging.getLogger(__name__)


def _take_candidates(
    candidates: list[CandidateProxy], start: int, n: int
) -> tuple[list[CandidateProxy], int]:
    return candidates[start : start + n], start + n


def _chunked_candidates(candidates: list[CandidateProxy], chunk_size: int):
    for index in range(0, len(candidates), chunk_size):
        yield candidates[index : index + chunk_size]


def _latency_value(value: float | None) -> float:
    return value if value is not None else 10**9


async def _fetch_candidates(
    config: AppConfig, db: Database, probe: ProxyProbe
) -> list[CandidateProxy]:
    seeded: dict[str, CandidateProxy] = {}
    for row in db.get_recent_all(config.target_final_count):
        seeded[row["proxy_hash"]] = CandidateProxy.from_row(row)
    LOGGER.info("Seeded from recent selected: %s", len(seeded))

    for row in db.get_recent_url_ok(config.target_final_count * 5):
        seeded[row["proxy_hash"]] = CandidateProxy.from_row(row)
    LOGGER.info("Seeded with URL cache total: %s", len(seeded))

    source_urls = list(config.subscription_urls)
    LOGGER.info("Loaded %s source URLs from config", len(source_urls))

    fresh = await collect_candidates(source_urls, db, probe.toolchain)
    LOGGER.info("Collected fresh candidates: %s", len(fresh))

    fresh_by_hash = {item.proxy_hash: item for item in fresh}
    alive_hashes = db.get_alive_hashes(list(fresh_by_hash.keys()))
    skipped_dead = len(fresh_by_hash) - len(alive_hashes)

    upsert_rows = []
    for proxy_hash in alive_hashes:
        c = fresh_by_hash[proxy_hash]
        seeded[c.proxy_hash] = c
        upsert_rows.append((c.proxy_hash, c.raw_link, c.scheme))

    db.upsert_proxies(upsert_rows)
    LOGGER.info(
        "Added fresh alive candidates: %s (skipped dead=%s)", len(seeded), skipped_dead
    )

    return list(seeded.values())


async def _url_test_stage(
    config: AppConfig, db: Database, probe: ProxyProbe, candidates: list[CandidateProxy]
) -> list[tuple[float, CandidateProxy]]:
    url_stream_chunk_size = max(config.url_batch_size * 4, config.url_batch_size)
    top_for_speed: list[tuple[float, CandidateProxy]] = []
    dead_after_url: list[tuple[str, str]] = []
    dead_flush_size = 1000
    total_url_ok = 0
    total_url_fail = 0

    for candidate_chunk in _chunked_candidates(candidates, url_stream_chunk_size):
        url_results = await probe.url_test_batch(
            candidate_chunk,
            config.url_test_url,
            config.url_timeout_seconds,
            config.test_attempts,
            config.url_batch_size,
        )
        db.mark_url_results(url_results)

        by_hash_link = {
            candidate.proxy_hash: candidate.raw_link for candidate in candidate_chunk
        }
        for res in url_results:
            if len(dead_after_url) >= dead_flush_size:
                db.mark_dead_many(dead_after_url, ttl_days=config.dead_ttl_days)
                dead_after_url.clear()

            # Filtering
            in_excluded_country = res.country in config.exclude_countries
            if not res.success or in_excluded_country:
                total_url_fail += 1
                reason = (
                    "excluded_country"
                    if in_excluded_country
                    else (res.reason or "url_test_failed")
                )
                dead_after_url.append((res.proxy_hash, reason))
                continue

            raw_link = by_hash_link.get(res.proxy_hash)
            if raw_link is None:
                continue

            total_url_ok += 1
            top_for_speed.append(
                (
                    _latency_value(res.latency_ms),
                    CandidateProxy(res.proxy_hash, raw_link, "selected"),
                )
            )

    if dead_after_url:
        db.mark_dead_many(dead_after_url, ttl_days=config.dead_ttl_days)

    LOGGER.info("URL stage complete: ok=%s fail=%s", total_url_ok, total_url_fail)

    return top_for_speed


async def run_once(config: AppConfig, db: Database, probe: ProxyProbe) -> None:
    start_time = time.perf_counter()

    LOGGER.info("Initializing DB schema")
    db.init_schema()
    cleaned = db.cleanup_expired_dead()
    LOGGER.info("Expired dead proxies cleaned: %s", cleaned)

    candidates = await _fetch_candidates(config, db, probe)

    if not candidates:
        LOGGER.warning("No candidates available after seeding/collecting")
        db.store_selected([])
        write_export(config.export_file, db)
        return

    candidates_count = len(candidates)

    LOGGER.info("Starting URL test stage. total_candidates=%s", candidates_count)

    top_for_speed = await _url_test_stage(config, db, probe, candidates)

    speed_candidates = [entry[1] for entry in sorted(top_for_speed, key=lambda x: x[0])]

    LOGGER.info("Selected for speed stage: %s", len(speed_candidates))

    final_selection: list[str] = []
    dead_after_speed: list[tuple[str, str]] = []
    total_speed_ok = 0
    last_index = 0
    while total_speed_ok < config.target_final_count and (last_index + 1) < len(
        speed_candidates
    ):
        speed_test_chunk_size = min(
            len(speed_candidates) - (last_index + 1), config.speed_batch_size
        )

        next_speed_candidates, last_index = _take_candidates(
            speed_candidates, last_index, speed_test_chunk_size * 4
        )

        speed_results = await probe.speed_test_batch(
            next_speed_candidates,
            config.speed_test_url,
            config.url_timeout_seconds,
            config.speed_timeout_seconds,
            config.test_attempts,
            config.speed_batch_size,
        )

        db.mark_speed_results(speed_results)

        for res in speed_results:
            if not res.success:
                dead_after_speed.append(
                    (res.proxy_hash, res.reason or "speed_test_failed")
                )
                continue
            if (res.mbps or 0.0) < config.speed_min_mb_s:
                dead_after_speed.append((res.proxy_hash, "below_speed_threshold"))
                continue

            total_speed_ok += 1
            final_selection.append(res.proxy_hash)

            if total_speed_ok == config.target_final_count:
                break

    db.mark_dead_many(dead_after_speed, ttl_days=config.dead_ttl_days // 2)

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
