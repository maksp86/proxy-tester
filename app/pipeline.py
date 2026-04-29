from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio

from .batch_operations import BatchCandidateReader, BatchTestResultWriter
from .cidr import CIDRReader
from .config import AppConfig, CIDRConfig
from .db import Database
from .exporter import write_export
from .geoip import GeoIPReader
from .helpers import StopController, find_key_nonrecursive
from .models import CandidateProxy, ProxyTestResult, TestResultKind, TestResultReasons
from .proxy_tester import ProxyTester
from .subscriptions import fetch_candidates
from .xray_backend import XrayToolchain
from .xray_queue import XrayOrchestrator

LOGGER = logging.getLogger(__name__)


async def _cidr_filter_one(
    proxy: CandidateProxy,
    outbound: dict[str, Any] | None,
    cidr_reader: CIDRReader,
    result_writer: BatchTestResultWriter,
):
    if not outbound:
        return result_writer.add(
            ProxyTestResult(
                proxy_hash=proxy.proxy_hash,
                success=False,
                reason=TestResultReasons.INVALID_URI,
                kind=TestResultKind.CIDR,
            )
        )

    host = find_key_nonrecursive(outbound, "address")
    if not host:
        raise Exception("No host address found in config")

    success = await cidr_reader.filter(host)
    return result_writer.add(
        ProxyTestResult(
            proxy_hash=proxy.proxy_hash,
            success=success,
            reason=(
                TestResultReasons.OK if success else TestResultReasons.CIDR_DISCARDED
            ),
            kind=TestResultKind.CIDR,
        )
    )


async def _cidr_filter_stage(
    config: CIDRConfig,
    db: Database,
    toolchain: XrayToolchain,
    candidates_count: int,
) -> None:
    LOGGER.debug("CIDR filtering started")

    cidr_reader = CIDRReader(config)
    cidr_reader.ensure_cidr_reader()

    candidate_reader = BatchCandidateReader(db, toolchain, 100)
    result_writer = BatchTestResultWriter(db, 1000)

    tasks = set()

    async for proxy, outbound in tqdm_asyncio(
        candidate_reader, total=candidates_count, desc="CIDR-test", mininterval=2
    ):
        task = asyncio.create_task(
            _cidr_filter_one(proxy, outbound, cidr_reader, result_writer)
        )
        tasks.add(task)

        if len(tasks) >= 50:
            _, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

    if tasks:
        await tqdm_asyncio.gather(*tasks, desc="Ending CIDR-test", mininterval=2)

    await cidr_reader.close()
    result_writer.flush()


async def _url_test_stage(
    config: AppConfig, db: Database, toolchain: XrayToolchain, candidates_count: int
) -> None:
    LOGGER.debug("URL test starting")
    orchestrator = XrayOrchestrator(
        toolchain,
        config.tester.url_test.worker_count,
        config.tester.url_test.worker_tasks_count,
    )

    candidate_reader = BatchCandidateReader(db, toolchain, 100)
    result_writer = BatchTestResultWriter(db, 1000)

    geoip_reader: GeoIPReader | None = None
    if config.filter.geoip:
        geoip_reader = GeoIPReader(config.filter.geoip)
        geoip_reader.ensure_geoip_database()

    proxy_tester = ProxyTester(result_writer, orchestrator)
    await orchestrator.start()
    tasks = set()

    max_tasks = (
        config.tester.url_test.worker_count * config.tester.url_test.worker_tasks_count
    )
    async for proxy, outbound in tqdm_asyncio(
        candidate_reader, total=candidates_count, desc="URL-test", mininterval=2
    ):
        task = asyncio.create_task(
            proxy_tester.url_test_proxy(geoip_reader, proxy, outbound, config.tester)
        )
        tasks.add(task)

        if len(tasks) >= max_tasks:
            _, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

    if tasks:
        await tqdm_asyncio.gather(*tasks, desc="Ending URL-test", mininterval=2)

    result_writer.flush()
    await orchestrator.stop()


async def _speed_test_stage(
    config: AppConfig,
    db: Database,
    toolchain: XrayToolchain,
    stop_controller: StopController,
    candidates_count: int,
) -> None:
    LOGGER.debug("Speed test starting")
    orchestrator = XrayOrchestrator(
        toolchain,
        config.tester.speed_test.worker_count,
        config.tester.speed_test.worker_tasks_count,
    )

    candidate_reader = BatchCandidateReader(db, toolchain, 100)
    result_writer = BatchTestResultWriter(db, 100)

    proxy_tester = ProxyTester(result_writer, orchestrator)
    await orchestrator.start()
    tasks: set[asyncio.Task] = set()

    max_tasks = (
        config.tester.speed_test.worker_count
        * config.tester.speed_test.worker_tasks_count
    )

    pbar = tqdm(total=stop_controller.target, mininterval=2, desc="Speed test")

    async for proxy, outbound in candidate_reader:
        if (delta := stop_controller.success - pbar.n) > 0:
            pbar.update(delta)

        if stop_controller.should_stop():
            break

        task = asyncio.create_task(
            proxy_tester.speed_test_proxy(
                proxy, outbound, stop_controller, config.tester
            )
        )
        tasks.add(task)

        if len(tasks) >= max_tasks:
            _, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            if stop_controller.should_stop():
                break

    pbar.n = stop_controller.success
    pbar.refresh()
    pbar.close()

    if tasks:
        if stop_controller.should_stop():
            LOGGER.debug("Speed test target count reached")
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        else:
            await tqdm_asyncio.gather(
                *tasks, desc="Finishing speed test", mininterval=2
            )

    await orchestrator.stop()
    result_writer.flush()


async def run_once(config: AppConfig, db: Database, toolchain: XrayToolchain) -> None:
    start_time = time.perf_counter()

    LOGGER.info("Initializing DB schema")
    db.init_schema()
    cleaned = db.cleanup_expired_dead()
    LOGGER.info("Expired dead proxies cleaned: %s", cleaned)

    fresh_candidates_count = await fetch_candidates(config, db, toolchain)

    candidates_count = db.count_candidate_proxies()
    LOGGER.info(
        "Starting tests. total_candidates=%s fresh=%s",
        candidates_count,
        fresh_candidates_count,
    )

    # Tests

    if config.filter.cidr:
        LOGGER.info("Selected for cidr stage: %s", candidates_count)

        await _cidr_filter_stage(config.filter.cidr, db, toolchain, candidates_count)

        db.move_dead_proxies(config.tester.dead_ttl_days)
        candidates_count = db.count_candidate_proxies()

    LOGGER.info("Selected for url stage: %s", candidates_count)
    await _url_test_stage(config, db, toolchain, candidates_count)

    if config.filter.geoip:
        db.geoip_filter_proxies(config.filter.geoip)

    db.move_dead_proxies(config.tester.dead_ttl_days)

    speed_candidates_count = db.count_candidate_proxies()
    LOGGER.info("Selected for speed stage: %s", speed_candidates_count)

    stop_controller = StopController(config.tester.target_final_count)
    await _speed_test_stage(
        config, db, toolchain, stop_controller, speed_candidates_count
    )
    db.move_dead_proxies(config.tester.dead_ttl_days)

    final_selection_count = db.count_candidate_proxies_with_status("speed_ok")

    LOGGER.info("Final selection size: %s", final_selection_count)

    db.store_selected(config.tester.target_final_count)

    end_time = time.perf_counter()

    write_export(
        config.export_file,
        db,
        {"elapsed_time": end_time - start_time, "candidates": candidates_count},
    )
    LOGGER.info("Export saved to %s", config.export_file)
    LOGGER.info("Pipeline completed.")
