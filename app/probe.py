from __future__ import annotations

import asyncio
import ipaddress
import json
import logging
import socket
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import aiohttp
import geoip2.database
from geoip2.errors import AddressNotFoundError

from .models import CandidateProxy, SpeedTestResult, UrlTestResult
from .xray_backend import XrayToolchain

LOGGER = logging.getLogger(__name__)
PORT_POOL_START = 20_000
API_PORT_POOL_START = 30_000


class ProxyProbe:
    """Proxy probing adapter built on Xray-core + ProxyConverter."""

    def __init__(
        self,
        project_root: Path | None = None,
        geoip_db_path: Path | None = None,
        worker_count: int = 10,
        tasks_per_worker: int = 100,
    ) -> None:
        self._toolchain = XrayToolchain(project_root=project_root)
        self._geoip_db_path = geoip_db_path
        self._geoip_reader: geoip2.database.Reader | None = None
        self._geoip_error: str | None = None
        self._worker_count = max(1, worker_count)
        self._tasks_per_worker = max(1, tasks_per_worker)

    @property
    def toolchain(self) -> XrayToolchain:
        return self._toolchain

    def _ensure_geoip_reader(self) -> geoip2.database.Reader | None:
        if self._geoip_reader is not None:
            return self._geoip_reader
        if self._geoip_error is not None:
            return None
        if self._geoip_db_path is None:
            self._geoip_error = "geoip_db_path_not_set"
            return None

        try:
            if not self._geoip_db_path.exists():
                self._geoip_error = f"geoip_db_not_found:{self._geoip_db_path}"
                return None
            self._geoip_reader = geoip2.database.Reader(str(self._geoip_db_path))
            return self._geoip_reader
        except Exception as exc:
            self._geoip_error = f"geoip_reader_init_failed:{exc}"
            LOGGER.exception("Failed to initialize GeoIP reader")
            return None

    def _geoip_lookup(self, ip_value: str | None) -> tuple[str | None, str | None]:
        if not ip_value:
            return None, None
        try:
            ipaddress.ip_address(ip_value)
        except ValueError:
            return None, None

        reader = self._ensure_geoip_reader()
        if reader is None:
            return None, None

        try:
            city_record = reader.city(ip_value)
        except AddressNotFoundError:
            return None, None
        except Exception:
            LOGGER.exception("GeoIP lookup failed for ip=%s", ip_value)
            return None, None

        country = city_record.country.iso_code or city_record.country.name
        city = city_record.city.name
        return country, city

    async def url_test_stream(
        self,
        candidates: AsyncIterator[CandidateProxy] | list[CandidateProxy],
        test_url: str,
        timeout_s: float,
        attempts: int = 1,
        concurrency: int = 25,
    ) -> AsyncIterator[list[tuple[CandidateProxy, UrlTestResult]]]:
        ping_url = test_url if test_url else "http://www.google.com/generate_204"
        max_workers = max(1, min(concurrency, self._worker_count))
        queue_size = max_workers * self._tasks_per_worker

        async def _iter_candidates():
            if isinstance(candidates, list):
                for candidate in candidates:
                    yield candidate
                return
            async for candidate in candidates:
                yield candidate

        async with _XrayWorkerPool(
            self._toolchain.xray_path,
            worker_count=max_workers,
            tasks_per_worker=self._tasks_per_worker,
            inbound_start_port=PORT_POOL_START,
            api_start_port=API_PORT_POOL_START,
        ) as pool:
            queue: asyncio.Queue[CandidateProxy | None] = asyncio.Queue(maxsize=queue_size)
            result_queue: asyncio.Queue[tuple[CandidateProxy, UrlTestResult] | None] = (
                asyncio.Queue()
            )

            async def _producer() -> None:
                async for candidate in _iter_candidates():
                    await queue.put(candidate)
                for _ in range(max_workers):
                    await queue.put(None)

            async def _worker_loop() -> None:
                while True:
                    candidate = await queue.get()
                    if candidate is None:
                        await result_queue.put(None)
                        return
                    result = await self._url_test_candidate(
                        candidate=candidate,
                        test_url=ping_url,
                        timeout_s=timeout_s,
                        attempts=max(1, attempts),
                        pool=pool,
                    )
                    await result_queue.put((candidate, result))

            producer = asyncio.create_task(_producer())
            workers = [asyncio.create_task(_worker_loop()) for _ in range(max_workers)]

            buffer: list[tuple[CandidateProxy, UrlTestResult]] = []
            finished_workers = 0
            while finished_workers < max_workers:
                item = await result_queue.get()
                if item is None:
                    finished_workers += 1
                    continue
                buffer.append(item)
                if len(buffer) >= 500:
                    yield buffer
                    buffer = []

            if buffer:
                yield buffer

            await producer
            await asyncio.gather(*workers)

    async def speed_test_batch(
        self,
        candidates: list[CandidateProxy],
        download_url: str,
        connect_timeout_s: float,
        download_timeout_s: float,
        attempts: int,
        concurrency: int = 25,
    ) -> list[SpeedTestResult]:
        if not candidates:
            return []

        max_workers = max(1, min(concurrency, self._worker_count, len(candidates)))
        conversion_chunk_size = max(max_workers * 4, max_workers)

        results: list[SpeedTestResult] = []
        for candidate_chunk in _chunked(candidates, conversion_chunk_size):
            links = [candidate.raw_link for candidate in candidate_chunk]
            try:
                configs_by_link = await self._toolchain.convert_links(
                    links,
                    chunk_size=conversion_chunk_size,
                )
            except Exception as exc:
                LOGGER.exception("ProxyConverter batch conversion failed")
                results.extend(
                    SpeedTestResult(
                        proxy_hash=c.proxy_hash,
                        success=False,
                        reason=f"converter_runtime_error:{exc}",
                    )
                    for c in candidate_chunk
                )
                continue

            async with _XrayWorkerPool(
                self._toolchain.xray_path,
                worker_count=max_workers,
                tasks_per_worker=self._tasks_per_worker,
                inbound_start_port=PORT_POOL_START,
                api_start_port=API_PORT_POOL_START,
            ) as pool:
                semaphore = asyncio.Semaphore(max_workers)

                async def _one(candidate: CandidateProxy) -> SpeedTestResult:
                    async with semaphore:
                        return await self._speed_test_candidate(
                            candidate,
                            configs_by_link,
                            download_url,
                            connect_timeout_s,
                            download_timeout_s,
                            attempts,
                            pool,
                        )

                chunk_results = await asyncio.gather(
                    *(_one(candidate) for candidate in candidate_chunk)
                )
                results.extend(chunk_results)
        return results

    async def _url_test_candidate(
        self,
        candidate: CandidateProxy,
        test_url: str,
        timeout_s: float,
        attempts: int,
        pool: _XrayWorkerPool,
    ) -> UrlTestResult:
        try:
            configs_by_link = await self._toolchain.convert_links(
                [candidate.raw_link],
                chunk_size=1,
            )
        except Exception as exc:
            return UrlTestResult(
                proxy_hash=candidate.proxy_hash,
                success=False,
                reason=f"converter_runtime_error:{exc}",
            )
        template_config = configs_by_link.get(candidate.raw_link)
        if template_config is None:
            return UrlTestResult(
                proxy_hash=candidate.proxy_hash,
                success=False,
                reason="invalid_proxy_uri",
            )

        outbounds = template_config.get("outbounds") if template_config else None
        if not outbounds:
            return UrlTestResult(
                proxy_hash=candidate.proxy_hash,
                success=False,
                reason="missing_outbound",
            )

        worker, outbound_tag = await pool.configure_proxy(
            outbounds[0],
            proxy_hash=candidate.proxy_hash,
        )

        latency_ms: float | None = None
        try:
            for _ in range(attempts):
                ok, latency_result = await _http_probe_url(
                    http_port=worker.inbound_port,
                    test_url=test_url,
                    timeout_s=timeout_s,
                )
                if not ok and latency_result == -1:
                    continue

                if ok and latency_result is not None and latency_result != -1:
                    latency_ms = latency_result
        finally:
            await pool.cleanup_proxy(worker, outbound_tag)

        if latency_ms is None:
            return UrlTestResult(
                proxy_hash=candidate.proxy_hash,
                success=False,
                reason="url_test_failed",
            )

        exit_ip = await _resolve_exit_ip(
            http_port=worker.inbound_port, timeout_s=min(timeout_s, 5.0)
        )
        country, city = self._geoip_lookup(exit_ip)
        return UrlTestResult(
            proxy_hash=candidate.proxy_hash,
            success=True,
            latency_ms=latency_ms,
            exit_ip=exit_ip,
            country=country,
            city=city,
            reason=outbound_tag,
        )

    async def _speed_test_candidate(
        self,
        candidate: CandidateProxy,
        configs_by_link: dict[str, dict[str, Any] | None],
        download_url: str,
        connect_timeout_s: float,
        download_timeout_s: float,
        attempts: int,
        pool: _XrayWorkerPool,
    ) -> SpeedTestResult:
        template_config = configs_by_link.get(candidate.raw_link)
        if template_config is None:
            return SpeedTestResult(
                proxy_hash=candidate.proxy_hash,
                success=False,
                reason="invalid_proxy_uri",
            )

        outbounds = template_config.get("outbounds") if template_config else None
        if not outbounds:
            return SpeedTestResult(
                proxy_hash=candidate.proxy_hash,
                success=False,
                reason="missing_outbound",
            )

        worker, outbound_tag = await pool.configure_proxy(
            outbounds[0],
            proxy_hash=candidate.proxy_hash,
        )

        speed_bps, size_download = None, None
        try:
            for _ in range(attempts):
                speed_bps, size_download = await _http_probe_speed(
                    worker.inbound_port,
                    download_url,
                    connect_timeout_s,
                    download_timeout_s,
                )
                if speed_bps is None and size_download is None:
                    continue
        finally:
            await pool.cleanup_proxy(worker, outbound_tag)

        if speed_bps is None:
            return SpeedTestResult(
                proxy_hash=candidate.proxy_hash,
                success=False,
                reason="speed_test_failed",
            )

        mbps = speed_bps * 8 / 1_000_000
        return SpeedTestResult(
            proxy_hash=candidate.proxy_hash,
            success=mbps > 0,
            mbps=mbps,
            bytes_downloaded=size_download,
        )


@dataclass(slots=True)
class _XrayWorker:
    xray_path: Path
    api_port: int
    inbound_port: int

    def __post_init__(self) -> None:
        self._proc: asyncio.subprocess.Process | None = None
        self._lock = asyncio.Lock()
        self._tasks_done = 0
        self._inbound_tag = f"inbound-{self.inbound_port}"

    @property
    def api_address(self) -> str:
        return f"127.0.0.1:{self.api_port}"

    async def start(self) -> None:
        base_config = {
            "log": {"loglevel": "warning"},
            "dns": {
                "hosts": {},
                "servers": [
                    {
                        "address": "223.5.5.5",
                        "domains": ["full:dns.alidns.com", "full:cloudflare-dns.com"],
                        "skipFallback": True,
                    },
                    "https://cloudflare-dns.com/dns-query",
                ],
                "tag": "dns-module",
            },
            "api": {
                "tag": "api",
                "services": ["HandlerService", "RoutingService"],
                "listen": self.api_address,
            },
            "routing": {"domainStrategy": "AsIs", "rules": []},
            "inbounds": [
                {
                    "tag": self._inbound_tag,
                    "port": self.inbound_port,
                    "listen": "127.0.0.1",
                    "protocol": "http",
                    "settings": {"allowTransparent": False},
                }
            ],
        }

        self._proc = await asyncio.create_subprocess_exec(
            str(self.xray_path),
            "run",
            cwd=self.xray_path.parent,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )

        if self._proc.stdin:
            self._proc.stdin.write(json.dumps(base_config).encode("utf-8"))
            await self._proc.stdin.drain()
            self._proc.stdin.close()

        await asyncio.sleep(0.2)
        if self._proc.returncode is not None:
            raise RuntimeError(f"xray worker exited immediately: {self._proc.returncode}")

    async def stop(self) -> None:
        if self._proc and self._proc.returncode is None:
            self._proc.kill()
            try:
                await asyncio.wait_for(self._proc.wait(), timeout=0.5)
            except TimeoutError:
                return

    async def recycle_if_needed(self, tasks_per_worker: int) -> None:
        if self._tasks_done < tasks_per_worker:
            return
        await self.stop()
        self._tasks_done = 0
        await self.start()

    async def configure_proxy(self, outbound_config: dict[str, Any], proxy_hash: str) -> str:
        async with self._lock:
            outbound_tag = f"outbound-{proxy_hash}"
            outbound_payload = {"outbounds": [{**outbound_config, "tag": outbound_tag}]}
            await _xray_api_call(
                self.xray_path,
                "rmo",
                self.api_address,
                {"tag": outbound_tag},
                ignore_error=True,
            )
            await _xray_api_call(self.xray_path, "ado", self.api_address, outbound_payload)

            route_payload = {
                "routing": {
                    "domainStrategy": "AsIs",
                    "rules": [
                        {
                            "type": "field",
                            "inboundTag": [self._inbound_tag],
                            "outboundTag": outbound_tag,
                        }
                    ],
                }
            }
            await _xray_api_call(
                self.xray_path,
                "adrules",
                self.api_address,
                route_payload,
                extra_args=["-append"],
            )

            self._tasks_done += 1
            return outbound_tag

    async def cleanup_proxy(self, outbound_tag: str) -> None:
        async with self._lock:
            rule_payload = {
                "routing": {
                    "rules": [
                        {
                            "type": "field",
                            "inboundTag": [self._inbound_tag],
                            "outboundTag": outbound_tag,
                        }
                    ]
                }
            }
            await _xray_api_call(
                self.xray_path,
                "rmrules",
                self.api_address,
                rule_payload,
                ignore_error=True,
            )
            await _xray_api_call(
                self.xray_path,
                "rmo",
                self.api_address,
                {"tag": outbound_tag},
                ignore_error=True,
            )


class _XrayWorkerPool:
    def __init__(
        self,
        xray_path: Path,
        worker_count: int,
        tasks_per_worker: int,
        inbound_start_port: int,
        api_start_port: int,
    ) -> None:
        self._xray_path = xray_path
        self._worker_count = worker_count
        self._tasks_per_worker = tasks_per_worker
        self._workers = [
            _XrayWorker(
                xray_path=xray_path,
                api_port=api_start_port + i,
                inbound_port=inbound_start_port + i,
            )
            for i in range(worker_count)
        ]
        self._queue: asyncio.Queue[_XrayWorker] = asyncio.Queue()

    async def __aenter__(self) -> _XrayWorkerPool:
        for worker in self._workers:
            await worker.start()
            self._queue.put_nowait(worker)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        for worker in self._workers:
            await worker.stop()

    async def configure_proxy(
        self, outbound_config: dict[str, Any], proxy_hash: str
    ) -> tuple[_XrayWorker, str]:
        worker = await self._queue.get()
        try:
            tag = await worker.configure_proxy(outbound_config, proxy_hash)
            await worker.recycle_if_needed(self._tasks_per_worker)
            return worker, tag
        finally:
            self._queue.put_nowait(worker)

    async def cleanup_proxy(self, worker: _XrayWorker, outbound_tag: str) -> None:
        await worker.cleanup_proxy(outbound_tag)


async def _xray_api_call(
    xray_path: Path,
    command: str,
    api_address: str,
    payload: dict[str, Any],
    extra_args: list[str] | None = None,
    ignore_error: bool = False,
) -> None:
    args = [
        str(xray_path),
        "api",
        command,
    ]
    if extra_args:
        args.extend(extra_args)
    args.extend(["-s", api_address])
    proc = await asyncio.create_subprocess_exec(
        *args,
        cwd=xray_path.parent,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate(json.dumps(payload).encode("utf-8"))
    if proc.returncode != 0:
        if ignore_error:
            return
        err = stderr.decode("utf-8", errors="ignore").strip()
        out = stdout.decode("utf-8", errors="ignore").strip()
        raise RuntimeError(
            f"xray api call failed ({command}) code={proc.returncode}: {err or out}"
        )


async def _http_probe_url(
    http_port: int, test_url: str, timeout_s: float
) -> tuple[bool, float | None]:
    timeout = aiohttp.ClientTimeout(total=max(timeout_s, 1.0))
    start = time.perf_counter()
    try:
        async with aiohttp.ClientSession(
            timeout=timeout, proxy=f"http://127.0.0.1:{http_port}"
        ) as session:
            async with session.get(test_url) as response:
                await response.read()
        latency_ms = (time.perf_counter() - start) * 1000
        return True, latency_ms
    except Exception as e:
        return False, -1 if isinstance(e, TimeoutError) else None


async def _resolve_exit_ip(http_port: int, timeout_s: float) -> str | None:
    timeout = aiohttp.ClientTimeout(total=max(timeout_s, 1.0))
    try:
        connector = aiohttp.TCPConnector(family=socket.AF_INET)
        async with aiohttp.ClientSession(
            timeout=timeout, connector=connector, proxy=f"http://127.0.0.1:{http_port}"
        ) as session:
            async with session.get("http://ifconfig.me/ip") as response:
                return (await response.text()).strip() or None
    except Exception:
        return None


async def _http_probe_speed(
    http_port: int,
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
            timeout=timeout, proxy=f"http://127.0.0.1:{http_port}"
        ) as session:
            async with session.get(download_url) as response:
                async for chunk in response.content.iter_chunked(8 * 1024):
                    bytes_downloaded += len(chunk)
    except Exception:
        return None, None

    duration = time.perf_counter() - started
    if duration <= 0:
        return None, None
    return bytes_downloaded / duration, bytes_downloaded


def _chunked(values: list[CandidateProxy], chunk_size: int):
    for index in range(0, len(values), chunk_size):
        yield values[index : index + chunk_size]
