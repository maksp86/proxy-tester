from __future__ import annotations

import asyncio
import ipaddress
import json
import logging
import tempfile
import time
from pathlib import Path

import aiohttp
import geoip2.database
from geoip2.errors import AddressNotFoundError

from .models import CandidateProxy, SpeedTestResult, UrlTestResult
from .xray_backend import XrayToolchain

LOGGER = logging.getLogger(__name__)


class ProxyProbe:
    """Proxy probing adapter built on Xray-core + ProxyConverter."""

    def __init__(self, project_root: Path | None = None, geoip_db_path: Path | None = None) -> None:
        self._toolchain = XrayToolchain(project_root=project_root)
        self._geoip_db_path = geoip_db_path
        self._geoip_reader: geoip2.database.Reader | None = None
        self._geoip_error: str | None = None
        self._parallel_limit = 5

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
        urls: tuple[str, ...],
        timeout_s: float,
    ) -> list[UrlTestResult]:
        if not candidates:
            return []

        ping_url = urls[0] if urls else "http://www.google.com/generate_204"
        links = [candidate.raw_link for candidate in candidates]

        try:
            configs_by_link = self._toolchain.convert_links(links, start_port=20000)
        except Exception as exc:
            LOGGER.exception("ProxyConverter batch conversion failed")
            return [
                UrlTestResult(proxy_hash=c.proxy_hash, success=False, reason=f"converter_runtime_error:{exc}")
                for c in candidates
            ]

        semaphore = asyncio.Semaphore(self._parallel_limit)

        async def _one(candidate: CandidateProxy) -> UrlTestResult:
            async with semaphore:
                return await self._url_test_candidate(candidate, configs_by_link, ping_url, timeout_s)

        return list(await asyncio.gather(*(_one(candidate) for candidate in candidates)))

    async def _url_test_candidate(
        self,
        candidate: CandidateProxy,
        configs_by_link: dict[str, dict],
        ping_url: str,
        timeout_s: float,
    ) -> UrlTestResult:
        config = configs_by_link.get(candidate.raw_link)
        if config is None:
            return UrlTestResult(proxy_hash=candidate.proxy_hash, success=False, reason="invalid_proxy_uri")

        socks_port = _extract_socks_port(config)
        if socks_port is None:
            return UrlTestResult(proxy_hash=candidate.proxy_hash, success=False, reason="missing_socks_inbound")

        async with _xray_runtime(self._toolchain.xray_path, config, socks_port):
            ok, latency_ms = await _http_probe_url(socks_port=socks_port, test_url=ping_url, timeout_s=timeout_s)
            if not ok:
                return UrlTestResult(proxy_hash=candidate.proxy_hash, success=False, reason="url_test_failed")

            exit_ip = await _resolve_exit_ip(socks_port=socks_port, timeout_s=min(timeout_s, 5.0))
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
        timeout_s: float,
    ) -> list[SpeedTestResult]:
        if not candidates:
            return []

        links = [candidate.raw_link for candidate in candidates]
        try:
            configs_by_link = self._toolchain.convert_links(links, start_port=30000)
        except Exception as exc:
            LOGGER.exception("ProxyConverter batch conversion failed")
            return [
                SpeedTestResult(proxy_hash=c.proxy_hash, success=False, reason=f"converter_runtime_error:{exc}")
                for c in candidates
            ]

        semaphore = asyncio.Semaphore(self._parallel_limit)

        async def _one(candidate: CandidateProxy) -> SpeedTestResult:
            async with semaphore:
                return await self._speed_test_candidate(candidate, configs_by_link, download_url, timeout_s)

        return list(await asyncio.gather(*(_one(candidate) for candidate in candidates)))

    async def _speed_test_candidate(
        self,
        candidate: CandidateProxy,
        configs_by_link: dict[str, dict],
        download_url: str,
        timeout_s: float,
    ) -> SpeedTestResult:
        config = configs_by_link.get(candidate.raw_link)
        if config is None:
            return SpeedTestResult(proxy_hash=candidate.proxy_hash, success=False, reason="invalid_proxy_uri")

        socks_port = _extract_socks_port(config)
        if socks_port is None:
            return SpeedTestResult(proxy_hash=candidate.proxy_hash, success=False, reason="missing_socks_inbound")

        async with _xray_runtime(self._toolchain.xray_path, config, socks_port):
            speed_bps, size_download = await _http_probe_speed(
                socks_port=socks_port,
                download_url=download_url,
                timeout_s=timeout_s,
            )
            if speed_bps is None:
                return SpeedTestResult(proxy_hash=candidate.proxy_hash, success=False, reason="speed_test_failed")

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
        self._tmpdir: tempfile.TemporaryDirectory[str] | None = None
        self._proc: asyncio.subprocess.Process | None = None

    async def __aenter__(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory(prefix="xray-config-")
        config_path = Path(self._tmpdir.name) / "config.json"
        config_path.write_text(json.dumps(self._config, ensure_ascii=False), encoding="utf-8")

        self._proc = await asyncio.create_subprocess_exec(
            str(self._xray_path),
            "run",
            "-c",
            str(config_path),
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await _wait_for_port(self._socks_port, timeout_s=3.0)

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._proc and self._proc.returncode is None:
            self._proc.terminate()
            try:
                await asyncio.wait_for(self._proc.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                self._proc.kill()
                await self._proc.wait()

        if self._tmpdir:
            self._tmpdir.cleanup()


async def _wait_for_port(port: int, timeout_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.close()
            await writer.wait_closed()
            return
        except Exception:
            await asyncio.sleep(0.05)
    raise RuntimeError(f"xray socks port {port} did not open in {timeout_s}s")


async def _http_probe_url(socks_port: int, test_url: str, timeout_s: float) -> tuple[bool, float | None]:
    proxy = f"http://127.0.0.1:{socks_port}"
    timeout = aiohttp.ClientTimeout(total=max(timeout_s, 1.0))
    start = time.perf_counter()
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(test_url, proxy=proxy) as response:
                await response.read()
        latency_ms = (time.perf_counter() - start) * 1000
        return True, latency_ms
    except Exception:
        return False, None


async def _resolve_exit_ip(socks_port: int, timeout_s: float) -> str | None:
    proxy = f"http://127.0.0.1:{socks_port}"
    timeout = aiohttp.ClientTimeout(total=max(timeout_s, 1.0))
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get("https://api.ipify.org", proxy=proxy) as response:
                return (await response.text()).strip() or None
    except Exception:
        return None


async def _http_probe_speed(socks_port: int, download_url: str, timeout_s: float) -> tuple[float | None, int | None]:
    proxy = f"http://127.0.0.1:{socks_port}"
    timeout = aiohttp.ClientTimeout(total=max(timeout_s, 1.0))
    bytes_downloaded = 0
    started = time.perf_counter()

    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(download_url, proxy=proxy) as response:
                async for chunk in response.content.iter_chunked(64 * 1024):
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
