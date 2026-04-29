import asyncio
import logging
import socket
import time
from typing import Any

import aiohttp

from app.batch_operations import BatchTestResultWriter
from app.config import TesterConfig
from app.geoip import GeoIPReader
from app.helpers import StopController
from app.models import (
    CandidateProxy,
    ProxyTestResult,
    TestResultKind,
    TestResultReasons,
)
from app.xray_queue import XrayOrchestrator

LOGGER = logging.getLogger(__name__)


class ProxyTester:
    def __init__(
        self, batch_writer: BatchTestResultWriter, orchestrator: XrayOrchestrator
    ):
        self.orchestrator = orchestrator
        self.batch_writer = batch_writer

    async def url_test_proxy(
        self,
        geoip_reader: GeoIPReader | None,
        candidate: CandidateProxy,
        outbound: dict[str, Any] | None,
        tester_config: TesterConfig,
    ) -> None:
        if not outbound:
            return self.batch_writer.add(
                ProxyTestResult(
                    proxy_hash=candidate.proxy_hash,
                    success=False,
                    reason=TestResultReasons.INVALID_URI,
                    kind=TestResultKind.URL,
                )
            )

        slot = await self.orchestrator.acquire_slot()
        worker = self.orchestrator.get_worker(slot.worker_id)
        LOGGER.debug(
            "Started test task on %s with %s", slot.inbound_tag, candidate.proxy_hash
        )

        try:
            async with worker.with_outbound(
                outbound, candidate.proxy_hash, slot.inbound_tag
            ):
                latency_ms: float | None = None
                for _ in range(tester_config.test_attempts):
                    ok, latency_result = await _http_probe_url(
                        http_proxy_port=slot.inbound_port,
                        test_url=str(tester_config.url_test.url),
                        timeout_s=tester_config.url_test.timeout,
                    )
                    if not ok and latency_result == -1:
                        continue

                    if ok and latency_result is not None and latency_result != -1:
                        latency_ms = latency_result

                if latency_ms is None:
                    return self.batch_writer.add(
                        ProxyTestResult(
                            proxy_hash=candidate.proxy_hash,
                            success=False,
                            reason=TestResultReasons.URL_FAIL,
                            kind=TestResultKind.URL,
                        )
                    )

                exit_ip = await _resolve_exit_ip(
                    http_proxy_port=slot.inbound_port,
                    timeout_s=min(tester_config.url_test.timeout, 5.0),
                )
                if geoip_reader:
                    country, city = geoip_reader.geoip_lookup(exit_ip)
                else:
                    country, city = None, None

                return self.batch_writer.add(
                    ProxyTestResult(
                        proxy_hash=candidate.proxy_hash,
                        success=True,
                        latency_ms=latency_ms,
                        exit_ip=exit_ip,
                        country=country,
                        city=city,
                        reason=TestResultReasons.OK,
                        kind=TestResultKind.URL,
                    )
                )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            reason = TestResultReasons.URL_FAIL
            if isinstance(e, ValueError):
                reason = TestResultReasons.INVALID_URI
            return self.batch_writer.add(
                ProxyTestResult(
                    proxy_hash=candidate.proxy_hash,
                    success=False,
                    reason=reason,
                    kind=TestResultKind.URL,
                )
            )
        finally:
            LOGGER.debug(
                "Ended test task on %s with %s", slot.inbound_tag, candidate.proxy_hash
            )
            await self.orchestrator.release_slot(slot)

    async def speed_test_proxy(
        self,
        candidate: CandidateProxy,
        outbound: dict[str, Any] | None,
        stop_controller: StopController,
        tester_config: TesterConfig,
    ):
        if not outbound:
            return self.batch_writer.add(
                ProxyTestResult(
                    proxy_hash=candidate.proxy_hash,
                    success=False,
                    reason=TestResultReasons.INVALID_URI,
                    kind=TestResultKind.SPEED,
                )
            )

        slot = await self.orchestrator.acquire_slot()
        worker = self.orchestrator.get_worker(slot.worker_id)
        LOGGER.debug(
            "Started test task on %s with %s", slot.inbound_tag, candidate.proxy_hash
        )

        try:
            async with worker.with_outbound(
                outbound, candidate.proxy_hash, slot.inbound_tag
            ):
                speed_bps, size_download = None, None
                for _ in range(tester_config.test_attempts):
                    if stop_controller.should_stop():
                        return

                    speed_bps, size_download = await _http_probe_speed(
                        slot.inbound_port,
                        str(tester_config.speed_test.url),
                        tester_config.url_test.timeout,
                        tester_config.speed_test.timeout,
                    )
                    if speed_bps is None and size_download is None:
                        continue
                    if speed_bps is not None and size_download is not None:
                        break

                if speed_bps is None:
                    return self.batch_writer.add(
                        ProxyTestResult(
                            proxy_hash=candidate.proxy_hash,
                            success=False,
                            reason=TestResultReasons.SPEED_FAIL,
                            kind=TestResultKind.SPEED,
                        )
                    )

                mbps = speed_bps * 8 / (1024 * 1024)
                success = mbps > tester_config.speed_test.speed_threshold

                if success:
                    await stop_controller.add_success()

                return self.batch_writer.add(
                    ProxyTestResult(
                        proxy_hash=candidate.proxy_hash,
                        success=success,
                        mbps=mbps,
                        reason=(
                            TestResultReasons.OK
                            if success
                            else TestResultReasons.SPEED_BELOW_THRESHOLD
                        ),
                        kind=TestResultKind.SPEED,
                    )
                )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            reason = TestResultReasons.SPEED_FAIL
            if isinstance(e, ValueError):
                reason = TestResultReasons.INVALID_URI

            return self.batch_writer.add(
                ProxyTestResult(
                    proxy_hash=candidate.proxy_hash,
                    success=False,
                    reason=reason,
                    kind=TestResultKind.SPEED,
                )
            )
        finally:
            await self.orchestrator.release_slot(slot)
            LOGGER.debug(
                "Ended test task on %s with %s", slot.inbound_tag, candidate.proxy_hash
            )


async def _http_probe_url(
    http_proxy_port: int, test_url: str, timeout_s: float
) -> tuple[bool, float | None]:
    timeout = aiohttp.ClientTimeout(total=max(timeout_s, 1.0))
    start = time.perf_counter()
    try:
        async with aiohttp.ClientSession(
            timeout=timeout, proxy=f"http://127.0.0.1:{http_proxy_port}"
        ) as session:
            async with session.get(test_url) as response:
                await response.read()
        latency_ms = (time.perf_counter() - start) * 1000
        return True, latency_ms
    except asyncio.CancelledError:
        raise
    except Exception as e:
        return False, -1 if isinstance(e, TimeoutError) else None


async def _resolve_exit_ip(http_proxy_port: int, timeout_s: float) -> str | None:
    timeout = aiohttp.ClientTimeout(total=max(timeout_s, 1.0))
    try:
        connector = aiohttp.TCPConnector(family=socket.AF_INET)
        async with aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            proxy=f"http://127.0.0.1:{http_proxy_port}",
        ) as session:
            async with session.get("http://ifconfig.me/ip") as response:
                return (await response.text()).strip() or None
    except asyncio.CancelledError:
        raise
    except Exception:
        return None


async def _http_probe_speed(
    socks_port: int,
    download_url: str,
    connect_timeout_s: float,
    download_timeout_s: float,
) -> tuple[float | None, int | None]:
    connect_timeout_s = max(connect_timeout_s, 1.0)
    timeout = aiohttp.ClientTimeout(
        connect=max(connect_timeout_s, 1.0),
        sock_connect=max(connect_timeout_s, 1.0),
        sock_read=max(download_timeout_s, 1.0),
        total=connect_timeout_s * 2 + download_timeout_s,
    )
    bytes_downloaded = 0
    started = time.perf_counter()

    try:
        async with aiohttp.ClientSession(
            timeout=timeout, proxy=f"http://127.0.0.1:{socks_port}"
        ) as session:
            async with session.get(download_url) as response:
                async for chunk in response.content.iter_chunked(8 * 1024):
                    bytes_downloaded += len(chunk)
    except asyncio.CancelledError:
        raise
    except Exception:
        return None, None

    duration = time.perf_counter() - started
    if duration <= 0:
        return None, None
    return bytes_downloaded / duration, bytes_downloaded
