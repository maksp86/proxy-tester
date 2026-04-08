from __future__ import annotations

import asyncio
import copy
import ipaddress
import json
import logging
import socket
import time
from pathlib import Path
from typing import Any

import aiohttp
import geoip2.database
import tqdm.asyncio
from geoip2.errors import AddressNotFoundError

from .models import CandidateProxy, SpeedTestResult, UrlTestResult
from .xray_backend import XrayToolchain

LOGGER = logging.getLogger(__name__)
PORT_POOL_START = 20_000


class ProxyProbe:
    """Proxy probing adapter built on Xray-core + ProxyConverter."""

    def __init__(
        self, project_root: Path | None = None, geoip_db_path: Path | None = None
    ) -> None:
        self._toolchain = XrayToolchain(project_root=project_root)
        self._geoip_db_path = geoip_db_path
        self._geoip_reader: geoip2.database.Reader | None = None
        self._geoip_error: str | None = None

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

    async def url_test_batch(
        self,
        candidates: list[CandidateProxy],
        test_url: str,
        timeout_s: float,
        attempts: int = 1,
        concurrency: int = 25,
    ) -> list[UrlTestResult]:
        if not candidates:
            return []

        ping_url = test_url if test_url else "http://www.google.com/generate_204"
        max_workers = max(1, min(concurrency, len(candidates)))
        conversion_chunk_size = max(max_workers * 4, max_workers)

        results: list[UrlTestResult] = []
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
                    UrlTestResult(
                        proxy_hash=c.proxy_hash,
                        success=False,
                        reason=f"converter_runtime_error:{exc}",
                    )
                    for c in candidate_chunk
                )
                continue

            semaphore = asyncio.Semaphore(max_workers)
            port_pool: asyncio.Queue[int] = asyncio.Queue()
            for port in range(PORT_POOL_START, PORT_POOL_START + max_workers):
                port_pool.put_nowait(port)

            async def _one(candidate: CandidateProxy) -> UrlTestResult:
                async with semaphore:
                    socks_port = await port_pool.get()
                    try:
                        return await self._url_test_candidate(
                            candidate,
                            configs_by_link,
                            ping_url,
                            timeout_s,
                            attempts=max(1, attempts),
                            socks_port=socks_port,
                        )
                    except Exception:
                        return UrlTestResult(
                            proxy_hash=candidate.proxy_hash,
                            success=False,
                            reason="xray_runtime_error",
                        )
                    finally:
                        port_pool.put_nowait(socks_port)

            chunk_results = await tqdm.asyncio.tqdm.gather(
                *(_one(candidate) for candidate in candidate_chunk)
            )
            results.extend(chunk_results)
        return results

    async def _url_test_candidate(
        self,
        candidate: CandidateProxy,
        configs_by_link: dict[str, dict[str, Any] | None],
        test_url: str,
        timeout_s: float,
        attempts: int,
        socks_port: int,
    ) -> UrlTestResult:
        template_config = configs_by_link.get(candidate.raw_link)
        if template_config is None:
            return UrlTestResult(
                proxy_hash=candidate.proxy_hash,
                success=False,
                reason="invalid_proxy_uri",
            )

        config = _override_socks_port(template_config, socks_port)
        if config is None:
            return UrlTestResult(
                proxy_hash=candidate.proxy_hash,
                success=False,
                reason="missing_socks_inbound",
            )

        async with _xray_runtime(self._toolchain.xray_path, config, socks_port):
            latency_ms: float | None = None
            for _ in range(attempts):
                ok, latency_result = await _http_probe_url(
                    socks_port=socks_port,
                    test_url=test_url,
                    timeout_s=timeout_s,
                )
                if not ok and latency_result == -1:
                    continue

                if ok and latency_result is not None and latency_result != -1:
                    latency_ms = latency_result

            if latency_ms is None:
                return UrlTestResult(
                    proxy_hash=candidate.proxy_hash,
                    success=False,
                    reason="url_test_failed",
                )

            exit_ip = await _resolve_exit_ip(
                socks_port=socks_port, timeout_s=min(timeout_s, 5.0)
            )
            country, city = self._geoip_lookup(exit_ip)
            return UrlTestResult(
                proxy_hash=candidate.proxy_hash,
                success=True,
                latency_ms=latency_ms,
                exit_ip=exit_ip,
                country=country,
                city=city,
            )

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

        max_workers = max(1, min(concurrency, len(candidates)))
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

            semaphore = asyncio.Semaphore(max_workers)
            port_pool: asyncio.Queue[int] = asyncio.Queue()
            for port in range(PORT_POOL_START, PORT_POOL_START + max_workers):
                port_pool.put_nowait(port)

            async def _one(candidate: CandidateProxy) -> SpeedTestResult:
                async with semaphore:
                    socks_port = await port_pool.get()
                    try:
                        return await self._speed_test_candidate(
                            candidate,
                            configs_by_link,
                            download_url,
                            connect_timeout_s,
                            download_timeout_s,
                            attempts,
                            socks_port,
                        )
                    finally:
                        port_pool.put_nowait(socks_port)

            chunk_results = await tqdm.asyncio.tqdm.gather(
                *(_one(candidate) for candidate in candidate_chunk)
            )
            results.extend(chunk_results)
        return results

    async def _speed_test_candidate(
        self,
        candidate: CandidateProxy,
        configs_by_link: dict[str, dict[str, Any] | None],
        download_url: str,
        connect_timeout_s: float,
        download_timeout_s: float,
        attempts: int,
        socks_port: int,
    ) -> SpeedTestResult:
        template_config = configs_by_link.get(candidate.raw_link)
        if template_config is None:
            return SpeedTestResult(
                proxy_hash=candidate.proxy_hash,
                success=False,
                reason="invalid_proxy_uri",
            )

        config = _override_socks_port(template_config, socks_port)
        if config is None:
            return SpeedTestResult(
                proxy_hash=candidate.proxy_hash,
                success=False,
                reason="missing_socks_inbound",
            )

        async with _xray_runtime(self._toolchain.xray_path, config, socks_port):
            speed_bps, size_download = None, None
            for _ in range(attempts):
                speed_bps, size_download = await _http_probe_speed(
                    socks_port, download_url, connect_timeout_s, download_timeout_s
                )
                if speed_bps is None and size_download is None:
                    continue

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


class _xray_runtime:
    def __init__(self, xray_path: Path, config: dict, socks_port: int) -> None:
        self._xray_path = xray_path
        self._config = config
        self._socks_port = socks_port
        self._proc: asyncio.subprocess.Process | None = None

    async def __aenter__(self) -> None:
        self._proc = await asyncio.create_subprocess_exec(
            str(self._xray_path),
            "run",
            cwd=self._xray_path.parent,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )

        if self._proc.stdin:
            self._proc.stdin.write(json.dumps(self._config).encode("utf-8"))
            await self._proc.stdin.drain()
            self._proc.stdin.close()

        try:
            await asyncio.wait_for(self._proc.wait(), timeout=0.5)
        except TimeoutError:
            return

        raise ValueError(
            f"""Something is wrong with config: {self._config}. Xray at {self._socks_port} is dead with code {self._proc.returncode}."""
        )

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._proc and self._proc.returncode is None:
            self._proc.kill()
            try:
                await asyncio.wait_for(self._proc.wait(), timeout=1.0)
            except TimeoutError:
                raise RuntimeError(f"Failed to kill xray at {self._socks_port}.")


async def _http_probe_url(
    socks_port: int, test_url: str, timeout_s: float
) -> tuple[bool, float | None]:
    timeout = aiohttp.ClientTimeout(total=max(timeout_s, 1.0))
    start = time.perf_counter()
    try:
        async with aiohttp.ClientSession(
            timeout=timeout, proxy=f"http://127.0.0.1:{socks_port}"
        ) as session:
            async with session.get(test_url) as response:
                await response.read()
        latency_ms = (time.perf_counter() - start) * 1000
        return True, latency_ms
    except Exception as e:
        return False, -1 if isinstance(e, TimeoutError) else None


async def _resolve_exit_ip(socks_port: int, timeout_s: float) -> str | None:
    timeout = aiohttp.ClientTimeout(total=max(timeout_s, 1.0))
    try:
        connector = aiohttp.TCPConnector(family=socket.AF_INET)
        async with aiohttp.ClientSession(
            timeout=timeout, connector=connector, proxy=f"http://127.0.0.1:{socks_port}"
        ) as session:
            async with session.get("http://ifconfig.me/ip") as response:
                return (await response.text()).strip() or None
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
    except Exception:
        return None, None

    duration = time.perf_counter() - started
    if duration <= 0:
        return None, None
    return bytes_downloaded / duration, bytes_downloaded


def _extract_socks_port(config: dict) -> int | None:
    inbounds = config.get("inbounds") or []
    for inbound in inbounds:
        protocol = (inbound.get("protocol") or "").lower()
        if protocol in {"socks", "mixed"} and inbound.get("port"):
            return int(inbound["port"])
    return None


def _override_socks_port(config: dict, socks_port: int) -> dict | None:
    cloned = copy.deepcopy(config)
    inbounds = cloned.get("inbounds") or []
    for inbound in inbounds:
        protocol = (inbound.get("protocol") or "").lower()
        if protocol in {"socks", "mixed"}:
            inbound["port"] = socks_port
            return cloned
    return None


def _chunked(values: list[CandidateProxy], chunk_size: int):
    for index in range(0, len(values), chunk_size):
        yield values[index : index + chunk_size]
