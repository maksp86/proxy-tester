from __future__ import annotations

import asyncio
import json
from dataclasses import asdict

from .config import AppConfig
from .db import Database
from .exporter import write_export
from .models import CandidateProxy, SpeedTestResult, UrlTestResult
from .probe import ProxyProbe
from .subscriptions import collect_candidates, read_sources


def _chunks(items: list, n: int):
    for i in range(0, len(items), n):
        yield items[i : i + n]


async def _run_url_tests(
    probe: ProxyProbe,
    candidates: list[CandidateProxy],
    urls: tuple[str, ...],
    timeout_s: float,
    batch_size: int,
) -> list[UrlTestResult]:
    results: list[UrlTestResult] = []
    for batch in _chunks(candidates, batch_size):
        batch_results = await asyncio.gather(
            *[probe.url_test(c, urls=urls, timeout_s=timeout_s) for c in batch]
        )
        results.extend(batch_results)
    return results


async def _run_speed_tests(
    probe: ProxyProbe,
    candidates: list[CandidateProxy],
    download_url: str,
    timeout_s: float,
    batch_size: int,
) -> list[SpeedTestResult]:
    results: list[SpeedTestResult] = []
    for batch in _chunks(candidates, batch_size):
        batch_results = await asyncio.gather(
            *[probe.speed_test(c, download_url=download_url, timeout_s=timeout_s) for c in batch]
        )
        results.extend(batch_results)
    return results


def _row_to_candidate(row) -> CandidateProxy:
    return CandidateProxy(proxy_hash=row["proxy_hash"], raw_link=row["raw_link"], scheme="cached")


async def run_once(config: AppConfig, db: Database, probe: ProxyProbe) -> list[dict]:
    db.init_schema()
    db.cleanup_expired_dead()

    # 1) Seed set: last 25 selected + URL-success cache
    seeded: dict[str, CandidateProxy] = {}
    for row in db.get_recent_selected(config.target_final_count):
        c = _row_to_candidate(row)
        seeded[c.proxy_hash] = c
    for row in db.get_recent_url_ok(config.target_final_count * 5):
        c = _row_to_candidate(row)
        seeded[c.proxy_hash] = c

    # 2) Load sources and collect fresh candidates
    source_urls = read_sources(config.sources_file)
    fresh = collect_candidates(source_urls)
    for c in fresh:
        if db.is_dead(c.proxy_hash):
            continue
        db.upsert_proxy(c.proxy_hash, c.raw_link, c.scheme)
        seeded[c.proxy_hash] = c

    candidates = list(seeded.values())
    if not candidates:
        db.store_selected([])
        write_export(config.export_file, [])
        return []

    # 4) URL test
    url_results = await _run_url_tests(
        probe,
        candidates,
        config.url_urls,
        config.url_timeout_seconds,
        config.url_batch_size,
    )

    ok_for_speed: list[dict] = []
    by_hash = {c.proxy_hash: c for c in candidates}
    for res in url_results:
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

    ok_for_speed.sort(key=lambda x: x.get("latency_ms") or 10**9)
    top_for_speed = ok_for_speed[: config.speed_top_n]

    # 5) Speed test for top N
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
    spare: list[dict] = []

    for row in top_for_speed:
        s = speed_by_hash.get(row["proxy_hash"])
        if s is None:
            spare.append(row)
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
            continue
        if (s.mbps or 0.0) < config.speed_min_mb_s:
            db.mark_dead(s.proxy_hash, reason="below_speed_threshold", ttl_days=config.dead_ttl_days)
            continue
        row["mbps"] = s.mbps
        final.append(row)

    # Fill up to target from remaining URL-success records.
    used = {x["proxy_hash"] for x in final}
    for row in ok_for_speed:
        if len(final) >= config.target_final_count:
            break
        if row["proxy_hash"] in used:
            continue
        row = dict(row)
        row.setdefault("mbps", None)
        final.append(row)
        used.add(row["proxy_hash"])

    final = final[: config.target_final_count]
    db.store_selected(final)
    write_export(config.export_file, final)
    return final
