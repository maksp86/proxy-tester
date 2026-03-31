from __future__ import annotations

import json
import logging
from dataclasses import asdict
from typing import Iterator, Sequence, TypeVar

from tqdm import tqdm

from .config import AppConfig
from .db import Database
from .exporter import write_export
from .models import CandidateProxy, SpeedTestResult, UrlTestResult
from .probe import ProxyProbe
from .subscriptions import collect_candidates

LOGGER = logging.getLogger(__name__)
T = TypeVar("T")


def _chunks(items: Sequence[T], n: int) -> Iterator[Sequence[T]]:
    """Yield fixed-size slices.

    Args:
        items: Source sequence.
        n: Chunk size.

    Yields:
        Consecutive chunks from `items`.
    """

    for i in range(0, len(items), n):
        yield items[i : i + n]


async def _run_url_tests(
    probe: ProxyProbe,
    candidates: list[CandidateProxy],
    urls: tuple[str, ...],
    timeout_s: float,
    attempts: int,
    batch_size: int,
) -> list[UrlTestResult]:
    """Execute URL tests by batches with detailed logs and progress bar."""

    results: list[UrlTestResult] = []
    batches = list(_chunks(candidates, batch_size))
    for index, batch in enumerate(tqdm(batches, desc="URL tests", unit="batch"), start=1):
        LOGGER.info("URL batch %s started (size=%s)", index, len(batch))
        batch_results = await probe.url_test_batch(
            list(batch),
            urls=urls,
            timeout_s=timeout_s,
            attempts=attempts,
        )
        ok_count = sum(1 for item in batch_results if item.success)
        LOGGER.info("URL batch %s finished: ok=%s fail=%s", index, ok_count, len(batch_results) - ok_count)
        results.extend(batch_results)
    return results


async def _run_speed_tests(
    probe: ProxyProbe,
    candidates: list[CandidateProxy],
    download_url: str,
    timeout_s: float,
    batch_size: int,
) -> list[SpeedTestResult]:
    """Execute speed tests by batches with detailed logs and progress bar."""

    results: list[SpeedTestResult] = []
    batches = list(_chunks(candidates, batch_size))
    for index, batch in enumerate(tqdm(batches, desc="Speed tests", unit="batch"), start=1):
        LOGGER.info("Speed batch %s started (size=%s)", index, len(batch))
        batch_results = await probe.speed_test_batch(
            list(batch),
            download_url=download_url,
            timeout_s=timeout_s,
        )
        ok_count = sum(1 for item in batch_results if item.success)
        LOGGER.info("Speed batch %s finished: ok=%s fail=%s", index, ok_count, len(batch_results) - ok_count)
        results.extend(batch_results)
    return results


def _row_to_candidate(row) -> CandidateProxy:
    """Convert DB row to `CandidateProxy`."""

    return CandidateProxy(proxy_hash=row["proxy_hash"], raw_link=row["raw_link"], scheme="cached")


async def run_once(config: AppConfig, db: Database, probe: ProxyProbe) -> list[dict]:
    """Run full pipeline once.

    Args:
        config: Runtime configuration.
        db: Persistence layer.
        probe: URL/speed probing engine.

    Returns:
        Final list of selected proxies ready for export.
    """

    LOGGER.info("Initializing DB schema")
    db.init_schema()
    cleaned = db.cleanup_expired_dead()
    LOGGER.info("Expired dead proxies cleaned: %s", cleaned)

    # 1) Seed set: recent selected + URL-success cache.
    seeded: dict[str, CandidateProxy] = {}
    for row in db.get_recent_selected(config.target_final_count):
        c = _row_to_candidate(row)
        seeded[c.proxy_hash] = c
    LOGGER.info("Seeded from recent selected: %s", len(seeded))

    for row in db.get_recent_url_ok(config.target_final_count * 5):
        c = _row_to_candidate(row)
        seeded[c.proxy_hash] = c
    LOGGER.info("Seeded with URL cache total: %s", len(seeded))

    # 2) Collect fresh candidates from config subscription URLs.
    source_urls = list(config.subscription_urls)
    LOGGER.info("Loaded %s source URLs from config", len(source_urls))

    fresh = collect_candidates(source_urls, probe.toolchain)
    LOGGER.info("Collected fresh candidates: %s", len(fresh))

    skipped_dead = 0
    for c in tqdm(fresh, desc="Filter dead/upsert", unit="proxy"):
        if db.is_dead(c.proxy_hash):
            skipped_dead += 1
            continue
        db.upsert_proxy(c.proxy_hash, c.raw_link, c.scheme)
        seeded[c.proxy_hash] = c
    LOGGER.info("Added fresh alive candidates: %s (skipped dead=%s)", len(seeded), skipped_dead)

    candidates = list(seeded.values())
    if not candidates:
        LOGGER.warning("No candidates available after seeding/collecting")
        db.store_selected([])
        write_export(config.export_file, [])
        return []

    # 3) URL test stage.
    LOGGER.info("Starting URL test stage. total_candidates=%s", len(candidates))
    url_results = await _run_url_tests(
        probe,
        candidates,
        config.url_urls,
        config.url_timeout_seconds,
        config.url_test_attempts,
        config.url_batch_size,
    )

    ok_for_speed: list[dict] = []
    by_hash = {c.proxy_hash: c for c in candidates}
    for res in tqdm(url_results, desc="Persist URL results", unit="proxy"):
        db.save_url_result(
            proxy_hash=res.proxy_hash,
            success=res.success,
            latency_ms=res.latency_ms,
            exit_ip=res.exit_ip,
            country=res.country,
            city=res.city,
            details_json=json.dumps(asdict(res), ensure_ascii=False),
        )
        if not res.success:
            db.mark_dead(res.proxy_hash, reason=res.reason or "url_test_failed", ttl_days=config.dead_ttl_days)
            continue

        c = by_hash[res.proxy_hash]
        ok_for_speed.append(
            {
                "proxy_hash": res.proxy_hash,
                "raw_link": c.raw_link,
                "latency_ms": res.latency_ms,
                "exit_ip": res.exit_ip,
                "country": res.country,
                "city": res.city,
            }
        )
    LOGGER.info("URL stage complete: ok=%s fail=%s", len(ok_for_speed), len(url_results) - len(ok_for_speed))

    ok_for_speed.sort(key=lambda x: x.get("latency_ms") or 10**9)
    top_for_speed = ok_for_speed[: config.speed_top_n]
    LOGGER.info("Selected for speed stage: %s", len(top_for_speed))

    # 4) Speed test stage for top latency subset.
    speed_candidates = [
        CandidateProxy(proxy_hash=x["proxy_hash"], raw_link=x["raw_link"], scheme="selected")
        for x in top_for_speed
    ]
    speed_results = await _run_speed_tests(
        probe,
        speed_candidates,
        config.speed_test_url,
        config.speed_timeout_seconds,
        config.speed_batch_size,
    )

    speed_by_hash = {r.proxy_hash: r for r in speed_results}
    final: list[dict] = []
    rejected_after_speed: set[str] = set()

    for row in tqdm(top_for_speed, desc="Persist speed results", unit="proxy"):
        s = speed_by_hash.get(row["proxy_hash"])
        if s is None:
            continue

        db.save_speed_result(
            proxy_hash=s.proxy_hash,
            success=s.success,
            mbps=s.mbps,
            bytes_downloaded=s.bytes_downloaded,
            details_json=json.dumps(asdict(s), ensure_ascii=False),
        )
        if not s.success:
            db.mark_dead(s.proxy_hash, reason=s.reason or "speed_test_failed", ttl_days=config.dead_ttl_days)
            rejected_after_speed.add(s.proxy_hash)
            continue
        if (s.mbps or 0.0) < config.speed_min_mb_s:
            db.mark_dead(s.proxy_hash, reason="below_speed_threshold", ttl_days=config.dead_ttl_days)
            rejected_after_speed.add(s.proxy_hash)
            continue
        row["mbps"] = s.mbps
        final.append(row)

    # 5) Backfill to target from URL-success pool.
    used = {x["proxy_hash"] for x in final}
    for row in ok_for_speed:
        if len(final) >= config.target_final_count:
            break
        if row["proxy_hash"] in used:
            continue
        if row["proxy_hash"] in rejected_after_speed:
            continue
        row = dict(row)
        row.setdefault("mbps", None)
        final.append(row)
        used.add(row["proxy_hash"])

    final = final[: config.target_final_count]
    LOGGER.info("Final selection size: %s", len(final))
    db.store_selected(final)
    write_export(config.export_file, final)
    LOGGER.info("Export saved to %s", config.export_file)
    return final
