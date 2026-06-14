from __future__ import annotations

import asyncio
import logging
import time
import datetime
from typing import Any

from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio

from .batch_operations import BatchCandidateReader, BatchTestResultWriter
from .binary_toolchain import BinaryToolchain
from .cidr import CIDRReader
from .config import AppConfig, CIDRConfig, ConnectTestConfig
from .connect_tester import ConnectTester
from .db import Database
from .exporter import write_export
from .helpers import StopController, extract_address
from .models import CandidateProxy, ProxyTestResult, TestResultKind, TestResultReasons
from .proxy_tester import ProxyTester
from .subscriptions import fetch_candidates

LOGGER = logging.getLogger(__name__)


async def _cooldown(cooldown_time: float):
    if cooldown_time > 0:
        LOGGER.info("Cooldown %s", datetime.timedelta(seconds=cooldown_time))
        await asyncio.sleep(cooldown_time)


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

    host, _ = extract_address(outbound)
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
    toolchain: BinaryToolchain,
    candidates_count: int,
) -> None:
    LOGGER.debug("CIDR filtering started")

    cidr_reader = CIDRReader(config)
    cidr_reader.ensure_cidr_reader()

    candidate_reader = BatchCandidateReader(
        db, toolchain, 100, TestResultKind.CIDR)
    result_writer = BatchTestResultWriter(db, 1000)

    tasks = set()

    pbar = tqdm(
        total=candidates_count,
        mininterval=2,
        desc="CIDR test",
        unit="proxies",
        colour="MAGENTA",
    )

    while not candidate_reader.is_finished():
        res = await candidate_reader.take(1)
        if not res:
            break

        proxy, outbound = res[0]
        task = asyncio.create_task(
            _cidr_filter_one(proxy, outbound, cidr_reader, result_writer)
        )
        tasks.add(task)

        if len(tasks) >= config.concurrent_tasks:
            completed, tasks = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED
            )
            pbar.update(len(completed))

    pbar.close()

    if tasks:
        await tqdm_asyncio.gather(
            *tasks,
            desc="Ending CIDR-test",
            unit="tasks",
            mininterval=2,
            colour="MAGENTA",
        )

    await cidr_reader.close()
    result_writer.flush()


async def _connect_test_stage(
    config: ConnectTestConfig, db: Database,
    toolchain: BinaryToolchain, candidates_count: int
) -> None:
    LOGGER.debug("Connect test starting")

    candidate_reader = BatchCandidateReader(
        db, toolchain, config.concurrent_tasks * 4, kind=TestResultKind.CONNECT
    )

    result_writer = BatchTestResultWriter(db, 1000)

    proxy_tester = ConnectTester(config, result_writer)
    tasks: set[asyncio.Task] = set()

    pbar = tqdm(
        total=candidates_count,
        mininterval=2,
        desc="Connect test",
        unit="proxies",
        colour="GREEN",
    )

    while not candidate_reader.is_finished():
        res = await candidate_reader.take(1)
        if not res:
            break

        task = asyncio.create_task(proxy_tester.test_proxy(res[0]))
        tasks.add(task)

        if len(tasks) >= config.concurrent_tasks:
            completed, tasks = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED
            )
            pbar.update(len(completed))

    pbar.n = candidate_reader.position
    pbar.close()

    if tasks:
        await tqdm_asyncio.gather(
            *tasks,
            desc="Ending Connect-test",
            unit="tasks",
            mininterval=2,
            colour="GREEN",
        )

    result_writer.flush()


async def _url_test_stage(
    config: AppConfig, db: Database, toolchain: BinaryToolchain, candidates_count: int
) -> None:
    LOGGER.debug("URL test starting")

    max_tasks = (
        config.tester.url_test.worker_count * config.tester.url_test.worker_tasks_count
    )

    candidate_reader = BatchCandidateReader(
        db, toolchain, max_tasks * 4, kind=TestResultKind.URL
    )

    result_writer = BatchTestResultWriter(db, 1000)

    proxy_tester = ProxyTester(
        result_writer, config.tester, TestResultKind.URL, toolchain, config.filter.geoip
    )
    tasks: set[asyncio.Task] = set()

    pbar = tqdm(
        total=candidates_count,
        mininterval=2,
        desc="URL test",
        unit="proxies",
        colour="BLUE",
    )

    while not candidate_reader.is_finished():
        task = asyncio.create_task(
            proxy_tester.test_proxy(
                await candidate_reader.take(config.tester.url_test.worker_tasks_count)
            )
        )
        tasks.add(task)

        if len(tasks) >= config.tester.url_test.worker_count:
            _, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            pbar.n = candidate_reader.position
            pbar.refresh()

    pbar.close()

    if tasks:
        await tqdm_asyncio.gather(
            *tasks, desc="Ending URL-test", unit="tasks", mininterval=2, colour="BLUE"
        )

    result_writer.flush()


async def _speed_test_stage(
    config: AppConfig,
    db: Database,
    toolchain: BinaryToolchain,
    stop_controller: StopController,
    candidates_count: int,
) -> None:
    LOGGER.debug("URL test starting")

    max_tasks = (
        config.tester.url_test.worker_count * config.tester.url_test.worker_tasks_count
    )

    candidate_reader = BatchCandidateReader(
        db, toolchain, max_tasks * 4, TestResultKind.SPEED
    )

    result_writer = BatchTestResultWriter(db, 100)

    proxy_tester = ProxyTester(
        result_writer, config.tester, TestResultKind.SPEED, toolchain
    )
    tasks: set[asyncio.Task] = set()

    pbar = tqdm(
        total=candidates_count,
        mininterval=2,
        desc="Speed test",
        unit="proxies",
        colour="CYAN",
    )

    exhausted = False

    while not candidate_reader.is_finished():
        task = asyncio.create_task(
            proxy_tester.test_proxy(
                await candidate_reader.take(config.tester.url_test.worker_tasks_count)
            )
        )
        tasks.add(task)

        if len(tasks) >= config.tester.url_test.worker_count:
            res, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            await stop_controller.add_success(sum(r.result() for r in res))

            pbar.n = candidate_reader.position
            pbar.refresh()

            if stop_controller.should_stop():
                break
    else:
        exhausted = not stop_controller.should_stop()

    if exhausted:
        LOGGER.warning("Speed test target count not reached")

    if not exhausted:
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
                *tasks,
                desc="Finishing speed test",
                unit="tasks",
                mininterval=2,
                colour="CYAN",
            )

    result_writer.flush()


async def run_once(config: AppConfig, db: Database, toolchain: BinaryToolchain) -> None:
    start_time = time.perf_counter()

    LOGGER.info("Initializing DB schema")
    db.init_schema()
    cleaned = db.cleanup_expired_dead()

    if config.filter.deduplicate:
        duplicate_cleaned = db.cleanup_duplicate_dead_proxies()
        LOGGER.info(
            "Expired dead proxies cleaned: %s, cleaned duplicates from dead: %s",
            cleaned,
            duplicate_cleaned,
        )
    else:
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

        db.move_dead_proxies(config.dead_ttl)
        candidates_count = db.count_candidate_proxies()
        await _cooldown(config.tester.cooldown_time)

    if config.tester.connect_test:
        LOGGER.info("Selected for connect stage: %s", candidates_count)
        await _connect_test_stage(config.tester.connect_test, db,
                                  toolchain, candidates_count)

        db.move_dead_proxies(config.dead_ttl)
        candidates_count = db.count_candidate_proxies()
        await _cooldown(config.tester.cooldown_time)

    LOGGER.info("Selected for url stage: %s", candidates_count)
    await _url_test_stage(config, db, toolchain, candidates_count)
    await _cooldown(config.tester.cooldown_time)

    if config.filter.geoip:
        db.geoip_filter_proxies(config.filter.geoip)

    if config.filter.deduplicate:
        duplicates_count = db.mark_dead_duplicate_ip_proxies()
        LOGGER.info("Marked dead %s duplicates", duplicates_count)

    db.move_dead_proxies(config.dead_ttl)

    speed_candidates_count = db.count_candidate_proxies()
    LOGGER.info("Selected for speed stage: %s", speed_candidates_count)

    stop_controller = StopController(config.tester.target_final_count)
    await _speed_test_stage(
        config, db, toolchain, stop_controller, speed_candidates_count
    )
    db.move_dead_proxies(config.dead_ttl)

    final_selection_count = db.count_candidate_proxies_with_status("speed_ok")

    LOGGER.info("Final selection size: %s", final_selection_count)

    db.store_selected(config.tester.target_final_count)

    end_time = time.perf_counter()

    write_export(
        config.export_options,
        db,
        {"elapsed_time": end_time - start_time, "candidates": candidates_count},
    )
    LOGGER.info("Export saved to %s", config.export_options.file)
    LOGGER.info("Pipeline completed.")
