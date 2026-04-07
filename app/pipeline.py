from __future__ import annotations

import logging
import heapq
import itertools
from dataclasses import asdict

from tqdm import tqdm

from .config import AppConfig
from .db import Database
from .exporter import write_export
from .models import CandidateProxy
from .probe import ProxyProbe
from .subscriptions import collect_candidates

LOGGER = logging.getLogger(__name__)


def _row_to_candidate(row) -> CandidateProxy:
    return CandidateProxy(proxy_hash=row["proxy_hash"], raw_link=row["raw_link"], scheme="cached")


def _collect_url_stage_rows(
    url_results,
    by_hash_link: dict[str, str],
) -> tuple[list[dict], list[tuple[str, str]]]:
    ok_for_speed: list[dict] = []
    dead_after_url: list[tuple[str, str]] = []

    for res in tqdm(url_results, desc="URL post-processing", unit="proxy"):
        if not res.success:
            dead_after_url.append((res.proxy_hash, res.reason or "url_test_failed"))
            continue

        raw_link = by_hash_link.get(res.proxy_hash)
        if raw_link is None:
            continue

        ok_for_speed.append(
            {
                "proxy_hash": res.proxy_hash,
                "raw_link": raw_link,
                "latency_ms": res.latency_ms,
                "exit_ip": res.exit_ip,
                "country": res.country,
                "city": res.city,
            }
        )

    return ok_for_speed, dead_after_url


def _chunked_candidates(candidates: list[CandidateProxy], chunk_size: int):
    for index in range(0, len(candidates), chunk_size):
        yield candidates[index:index + chunk_size]


def _latency_value(value: float | None) -> float:
    return value if value is not None else 10**9


def _add_top_latency_row(
    top_rows_heap: list[tuple[float, int, dict]],
    row: dict,
    limit: int,
    counter: itertools.count,
) -> None:
    if limit <= 0:
        return

    score = _latency_value(row.get("latency_ms"))
    heapq.heappush(top_rows_heap, (-score, next(counter), row))
    if len(top_rows_heap) > limit:
        heapq.heappop(top_rows_heap)


def _collect_speed_stage_rows(
    top_for_speed: list[dict],
    speed_by_hash: dict,
    speed_min_mb_s: float,
    target_final_count: int,
) -> tuple[list[dict], list[tuple[str, str]]]:
    final: list[dict] = []
    dead_after_speed: list[tuple[str, str]] = []

    for row in tqdm(top_for_speed, desc="Speed post-processing", unit="proxy"):
        speed_result = speed_by_hash.get(row["proxy_hash"])
        if speed_result is None:
            continue
        if not speed_result.success:
            dead_after_speed.append((speed_result.proxy_hash, speed_result.reason or "speed_test_failed"))
            continue
        if (speed_result.mbps or 0.0) < speed_min_mb_s:
            dead_after_speed.append((speed_result.proxy_hash, "below_speed_threshold"))
            continue

        accepted_row = dict(row)
        accepted_row["mbps"] = speed_result.mbps
        final.append(accepted_row)
        if len(final) >= target_final_count:
            break

    return final, dead_after_speed

async def fetch_candidates(config: AppConfig, db: Database, probe: ProxyProbe) -> list[CandidateProxy]:
    seeded: dict[str, CandidateProxy] = {}
    for row in db.get_recent_all(config.target_final_count):
        c = _row_to_candidate(row)
        seeded[c.proxy_hash] = c
    LOGGER.info("Seeded from recent selected: %s", len(seeded))

    for row in db.get_recent_url_ok(config.target_final_count * 5):
        c = _row_to_candidate(row)
        seeded[c.proxy_hash] = c
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
    LOGGER.info("Added fresh alive candidates: %s (skipped dead=%s)", len(seeded), skipped_dead)

    return list(seeded.values())

async def run_once(config: AppConfig, db: Database, probe: ProxyProbe) -> list[dict]:
    LOGGER.info("Initializing DB schema")
    db.init_schema()
    cleaned = db.cleanup_expired_dead()
    LOGGER.info("Expired dead proxies cleaned: %s", cleaned)

    candidates = await fetch_candidates(config, db, probe)

    if not candidates:
        LOGGER.warning("No candidates available after seeding/collecting")
        db.store_selected([])
        write_export(config.export_file, [])
        return []

    LOGGER.info("Starting URL test stage. total_candidates=%s", len(candidates))
    url_stream_chunk_size = max(config.url_batch_size * 4, config.url_batch_size)
    top_for_speed_heap: list[tuple[float, int, dict]] = []
    heap_counter = itertools.count()
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

        by_hash_link = {candidate.proxy_hash: candidate.raw_link for candidate in candidate_chunk}
        for res in url_results:
            if len(dead_after_url) >= dead_flush_size:
                db.mark_dead_many(dead_after_url, ttl_days=config.dead_ttl_days)
                dead_after_url.clear()

            if not res.success:
                total_url_fail += 1
                dead_after_url.append((res.proxy_hash, res.reason or "url_test_failed"))
                continue

            raw_link = by_hash_link.get(res.proxy_hash)
            if raw_link is None:
                continue

            total_url_ok += 1
            _add_top_latency_row(
                top_for_speed_heap,
                {
                    "proxy_hash": res.proxy_hash,
                    "raw_link": raw_link,
                    "latency_ms": res.latency_ms,
                    "exit_ip": res.exit_ip,
                    "country": res.country,
                    "city": res.city,
                },
                config.speed_top_n,
                heap_counter,
            )


    if dead_after_url:
        db.mark_dead_many(dead_after_url, ttl_days=config.dead_ttl_days)

    LOGGER.info("URL stage complete: ok=%s fail=%s", total_url_ok, total_url_fail)

    top_for_speed = [entry[2] for entry in sorted(top_for_speed_heap, key=lambda x: -x[0])]
    LOGGER.info("Selected for speed stage: %s", len(top_for_speed))

    speed_candidates = [
        CandidateProxy(proxy_hash=x["proxy_hash"], raw_link=x["raw_link"], scheme="selected")
        for x in top_for_speed
    ]

    speed_results = await probe.speed_test_batch(
        speed_candidates,
        config.speed_test_url,
        config.url_timeout_seconds,
        config.speed_timeout_seconds,
        config.test_attempts,
        config.speed_batch_size,
    )

    speed_by_hash = {r.proxy_hash: r for r in speed_results}
    db.mark_speed_results([asdict(item) for item in speed_results])

    final, dead_after_speed = _collect_speed_stage_rows(
        top_for_speed=top_for_speed,
        speed_by_hash=speed_by_hash,
        speed_min_mb_s=config.speed_min_mb_s,
        target_final_count=config.target_final_count,
    )

    db.mark_dead_many(dead_after_speed, ttl_days=config.dead_ttl_days)
    LOGGER.info("Final selection size: %s", len(final))
    db.store_selected(final)
    write_export(config.export_file, final)
    LOGGER.info("Export saved to %s", config.export_file)
    return final
