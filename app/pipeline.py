from __future__ import annotations

import logging
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
    by_hash: dict[str, CandidateProxy],
) -> tuple[list[dict], list[tuple[str, str]]]:
    ok_for_speed: list[dict] = []
    dead_after_url: list[tuple[str, str]] = []

    for res in tqdm(url_results, desc="URL post-processing", unit="proxy"):
        if not res.success:
            dead_after_url.append((res.proxy_hash, res.reason or "url_test_failed"))
            continue

        candidate = by_hash[res.proxy_hash]
        ok_for_speed.append(
            {
                "proxy_hash": res.proxy_hash,
                "raw_link": candidate.raw_link,
                "latency_ms": res.latency_ms,
                "exit_ip": res.exit_ip,
                "country": res.country,
                "city": res.city,
            }
        )

    return ok_for_speed, dead_after_url


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


async def run_once(config: AppConfig, db: Database, probe: ProxyProbe) -> list[dict]:
    LOGGER.info("Initializing DB schema")
    db.init_schema()
    cleaned = db.cleanup_expired_dead()
    LOGGER.info("Expired dead proxies cleaned: %s", cleaned)

    seeded: dict[str, CandidateProxy] = {}
    for row in db.get_recent_selected(config.target_final_count):
        c = _row_to_candidate(row)
        seeded[c.proxy_hash] = c
    LOGGER.info("Seeded from recent selected: %s", len(seeded))

    for row in db.get_recent_url_ok(config.target_final_count * 5):
        c = _row_to_candidate(row)
        seeded[c.proxy_hash] = c
    LOGGER.info("Seeded with URL cache total: %s", len(seeded))

    source_urls = list(config.subscription_urls)
    LOGGER.info("Loaded %s source URLs from config", len(source_urls))

    fresh = collect_candidates(source_urls, probe.toolchain)
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

    candidates = list(seeded.values())
    if not candidates:
        LOGGER.warning("No candidates available after seeding/collecting")
        db.store_selected([])
        write_export(config.export_file, [])
        return []

    LOGGER.info("Starting URL test stage. total_candidates=%s", len(candidates))
    url_results = await probe.url_test_batch(
        candidates,
        config.url_test_url,
        config.url_timeout_seconds,
        config.test_attempts,
        config.url_batch_size,
    )

    by_hash = {c.proxy_hash: c for c in candidates}

    db.mark_url_results([asdict(item) for item in url_results])
    ok_for_speed, dead_after_url = _collect_url_stage_rows(url_results, by_hash)
    db.mark_dead_many(dead_after_url, ttl_days=config.dead_ttl_days)
    LOGGER.info("URL stage complete: ok=%s fail=%s", len(ok_for_speed), len(url_results) - len(ok_for_speed))

    ok_for_speed.sort(key=lambda x: x.get("latency_ms") or 10**9)
    top_for_speed = ok_for_speed[: config.speed_top_n]
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
