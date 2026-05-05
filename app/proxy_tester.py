import asyncio
import logging
import socket
import time
from typing import Any

import aiohttp
from pydantic import HttpUrl

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
        self, batch_writer: BatchTestResultWriter, orchestrator: XrayOrchestrator, config: TesterConfig,
        kind: TestResultKind
    ):
        self._orchestrator = orchestrator
        self._batch_writer = batch_writer
        self._config = config
        self._kind = kind

        if kind == TestResultKind.CIDR:
            raise ValueError("Invalid test kind")

        conn_limit = config.url_test.worker_count * config.url_test.worker_tasks_count
        timeout = aiohttp.ClientTimeout(
            total=max(self._config.url_test.timeout, 1.0))

        if kind == TestResultKind.SPEED:
            conn_limit = config.speed_test.worker_count * \
                config.speed_test.worker_tasks_count

            connect_timeout_s = max(self._config.url_test.timeout, 1.0)
            download_timeout_s = max(self._config.speed_test.timeout, 1.0)

            timeout = aiohttp.ClientTimeout(
                connect=connect_timeout_s,
                sock_connect=connect_timeout_s,
                sock_read=connect_timeout_s,
                total=connect_timeout_s * 2 + download_timeout_s,
            )

        self._connector = aiohttp.TCPConnector(
            limit=conn_limit * config.test_attempts,
            limit_per_host=config.test_attempts,
            keepalive_timeout=timeout.total,
            enable_cleanup_closed=True)

        self._session = aiohttp.ClientSession(
            connector=self._connector,
            timeout=timeout)

    async def test_proxy(self, candidate: CandidateProxy, outbound: dict[str, Any] | None, **kwargs):
        if self._kind == TestResultKind.CIDR:
            raise ValueError("Invalid test kind")
        elif self._kind == TestResultKind.URL:
            return await self._url_test_proxy(kwargs.get("geoip_reader"), candidate, outbound)
        elif self._kind == TestResultKind.SPEED:
            stop_controller = kwargs.get("stop_controller")
            if not stop_controller:
                raise KeyError("stop_controller was None")
            return await self._speed_test_proxy(candidate, outbound, stop_controller)

    async def _url_test_proxy(
        self,
        geoip_reader: GeoIPReader | None,
        candidate: CandidateProxy,
        outbound: dict[str, Any] | None
    ) -> None:
        if not outbound:
            return self._batch_writer.add(
                ProxyTestResult(
                    proxy_hash=candidate.proxy_hash,
                    success=False,
                    reason=TestResultReasons.INVALID_URI,
                    kind=TestResultKind.URL,
                )
            )

        slot = await self._orchestrator.acquire_slot()
        worker = self._orchestrator.get_worker(slot.worker_id)
        LOGGER.debug(
            "Started test task on %s with %s", slot.inbound_tag, candidate.proxy_hash
        )

        try:
            async with worker.with_outbound(
                outbound, candidate.proxy_hash, slot.inbound_tag
            ):
                latency_ms: float | None = None
                for _ in range(self._config.test_attempts):
                    ok, latency_result = await _http_probe_url(
                        session=self._session,
                        http_proxy_port=slot.inbound_port,
                        test_url=self._config.url_test.url
                    )
                    if not ok and latency_result == -1:
                        continue

                    if ok and latency_result is not None and latency_result != -1:
                        latency_ms = latency_result
                        break

                if latency_ms is None:
                    return self._batch_writer.add(
                        ProxyTestResult(
                            proxy_hash=candidate.proxy_hash,
                            success=False,
                            reason=TestResultReasons.URL_FAIL,
                            kind=TestResultKind.URL,
                        )
                    )

                exit_ip = await _resolve_exit_ip(self._session, slot.inbound_port)
                if geoip_reader:
                    country, city = geoip_reader.geoip_lookup(exit_ip)
                else:
                    country, city = None, None

                return self._batch_writer.add(
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
            return self._batch_writer.add(
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
            await self._orchestrator.release_slot(slot)

    async def _speed_test_proxy(
        self,
        candidate: CandidateProxy,
        outbound: dict[str, Any] | None,
        stop_controller: StopController
    ):
        if not outbound:
            return self._batch_writer.add(
                ProxyTestResult(
                    proxy_hash=candidate.proxy_hash,
                    success=False,
                    reason=TestResultReasons.INVALID_URI,
                    kind=TestResultKind.SPEED,
                )
            )

        slot = await self._orchestrator.acquire_slot()
        worker = self._orchestrator.get_worker(slot.worker_id)
        LOGGER.debug(
            "Started test task on %s with %s", slot.inbound_tag, candidate.proxy_hash
        )

        try:
            async with worker.with_outbound(
                outbound, candidate.proxy_hash, slot.inbound_tag
            ):
                speed_bps, size_download = None, None
                for _ in range(self._config.test_attempts):
                    if stop_controller.should_stop():
                        return

                    speed_bps, size_download = await _http_probe_speed(
                        session=self._session,
                        http_proxy_port=slot.inbound_port,
                        download_url=self._config.speed_test.url
                    )
                    if speed_bps is None and size_download is None:
                        continue
                    if speed_bps is not None and size_download is not None:
                        break

                if speed_bps is None:
                    return self._batch_writer.add(
                        ProxyTestResult(
                            proxy_hash=candidate.proxy_hash,
                            success=False,
                            reason=TestResultReasons.SPEED_FAIL,
                            kind=TestResultKind.SPEED,
                        )
                    )

                mbps = speed_bps * 8 / (1024 * 1024)
                success = mbps > self._config.speed_test.speed_threshold

                if success:
                    await stop_controller.add_success()

                return self._batch_writer.add(
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

            return self._batch_writer.add(
                ProxyTestResult(
                    proxy_hash=candidate.proxy_hash,
                    success=False,
                    reason=reason,
                    kind=TestResultKind.SPEED,
                )
            )
        finally:
            await self._orchestrator.release_slot(slot)
            LOGGER.debug(
                "Ended test task on %s with %s", slot.inbound_tag, candidate.proxy_hash
            )

    async def stop(self):
        await self._session.close()
        await self._connector.close()


async def _http_probe_url(
    session: aiohttp.ClientSession,
    http_proxy_port: int,
    test_url: HttpUrl
) -> tuple[bool, float | None]:
    start = time.perf_counter()
    try:
        async with session.get(test_url.encoded_string(), proxy=f"http://127.0.0.1:{http_proxy_port}") as response:
            await response.read()
        latency_ms = (time.perf_counter() - start) * 1000
        return True, latency_ms
    except asyncio.CancelledError:
        raise
    except Exception as e:
        return False, -1 if isinstance(e, TimeoutError) else None


async def _resolve_exit_ip(session: aiohttp.ClientSession, http_proxy_port: int) -> str | None:
    tester_urls = ("http://ipv4.text.wtfismyip.com",
                   "http://checkip.amazonaws.com",
                   "http://ifconfig.me/ip",
                   "http://ifconfig.io/ip",
                   "http://icanhazip.com",
                   "http://text.ipv4.myip.wtf")
    for tester_url in tester_urls[:4]:
        try:
            async with session.get(tester_url,
                                proxy=f"http://127.0.0.1:{http_proxy_port}",
                                timeout=aiohttp.ClientTimeout(total=1)
                                ) as response:
                return (await response.text()).strip() or None
        except asyncio.CancelledError:
            raise
        except TimeoutError as e:
            pass
        except Exception as exc:
            pass
    return None


async def _http_probe_speed(
    session: aiohttp.ClientSession,
    http_proxy_port: int,
    download_url: HttpUrl,
) -> tuple[float | None, int | None]:
    bytes_downloaded = 0
    started = time.perf_counter()
    try:
        async with session.get(download_url.encoded_string(), proxy=f"http://127.0.0.1:{http_proxy_port}") as response:
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
