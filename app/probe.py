from __future__ import annotations

import asyncio
import ipaddress
from pathlib import Path

import geoip2.database
from geoip2.errors import AddressNotFoundError
from python_v2ray.config_parser import parse_uri
from python_v2ray.downloader import BinaryDownloader
from python_v2ray.tester import ConnectionTester

from .models import CandidateProxy, SpeedTestResult, UrlTestResult


class ProxyProbe:
    def __init__(self, project_root: Path | None = None, geoip_db_path: Path | None = None) -> None:
        self._project_root = project_root or Path.cwd()
        self._tester: ConnectionTester | None = None
        self._setup_error: str | None = None

        self._geoip_db_path = geoip_db_path
        self._geoip_reader: geoip2.database.Reader | None = None
        self._geoip_error: str | None = None

    def _ensure_tester(self) -> ConnectionTester:
        if self._tester is not None:
            return self._tester
        if self._setup_error is not None:
            raise RuntimeError(self._setup_error)

        try:
            downloader = BinaryDownloader(self._project_root)
            downloader.ensure_all()
            self._tester = ConnectionTester(
                vendor_path=str(downloader.vendor_path),
                core_engine_path=str(downloader.core_engine_path),
            )
            return self._tester
        except Exception as exc:
            self._setup_error = f"python_v2ray setup failed: {exc}"
            raise RuntimeError(self._setup_error) from exc

    def _ensure_geoip_reader(self) -> geoip2.database.Reader | None:
        if self._geoip_reader is not None:
            return self._geoip_reader
        if self._geoip_error is not None:
            return None
        if self._geoip_db_path is None:
            self._geoip_error = "geoip_db_path_not_set"
            return None

        db_path = self._geoip_db_path
        try:
            if not db_path.exists():
                self._geoip_error = f"geoip_db_not_found:{db_path}"
                return None
            self._geoip_reader = geoip2.database.Reader(str(db_path))
            return self._geoip_reader
        except Exception as exc:
            self._geoip_error = f"geoip_reader_init_failed:{exc}"
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
            return None, None

        country = city_record.country.iso_code or city_record.country.name
        city = city_record.city.name
        return country, city

    async def url_test(self, candidate: CandidateProxy, urls: tuple[str, ...], timeout_s: float) -> UrlTestResult:
        params = parse_uri(candidate.raw_link)
        if params is None:
            return UrlTestResult(proxy_hash=candidate.proxy_hash, success=False, reason="invalid_proxy_uri")

        ping_url = urls[0] if urls else "http://www.google.com/generate_204"

        try:
            tester = self._ensure_tester()
            payload = await asyncio.to_thread(
                tester.test_uris,
                [params],
                timeout=max(int(timeout_s), 1),
                ping_url=ping_url,
            )
        except Exception as exc:
            return UrlTestResult(proxy_hash=candidate.proxy_hash, success=False, reason=f"url_test_runtime_error:{exc}")

        if not payload:
            return UrlTestResult(proxy_hash=candidate.proxy_hash, success=False, reason="url_test_failed")

        result = payload[0]
        if not result.get("success"):
            return UrlTestResult(
                proxy_hash=candidate.proxy_hash,
                success=False,
                reason=result.get("error") or "url_test_failed",
            )

        exit_ip = result.get("ip")
        geo_country, geo_city = self._geoip_lookup(exit_ip)

        return UrlTestResult(
            proxy_hash=candidate.proxy_hash,
            success=True,
            latency_ms=result.get("latency_ms"),
            exit_ip=exit_ip,
            country=geo_country or result.get("country"),
            city=geo_city or result.get("city"),
        )

    async def speed_test(
        self,
        candidate: CandidateProxy,
        download_url: str,
        timeout_s: float,
    ) -> SpeedTestResult:
        params = parse_uri(candidate.raw_link)
        if params is None:
            return SpeedTestResult(proxy_hash=candidate.proxy_hash, success=False, reason="invalid_proxy_uri")

        try:
            tester = self._ensure_tester()
            payload = await asyncio.to_thread(
                tester.test_speed,
                [params],
                timeout=max(int(timeout_s), 1),
                download_url=download_url,
            )
        except Exception as exc:
            return SpeedTestResult(proxy_hash=candidate.proxy_hash, success=False, reason=f"speed_test_runtime_error:{exc}")

        if not payload:
            return SpeedTestResult(proxy_hash=candidate.proxy_hash, success=False, reason="speed_test_failed")

        result = payload[0]
        mbps = result.get("download_mbps")
        success = bool(result.get("success", mbps is not None and mbps > 0))
        return SpeedTestResult(
            proxy_hash=candidate.proxy_hash,
            success=success,
            mbps=mbps,
            bytes_downloaded=result.get("bytes_downloaded"),
            reason=None if success else (result.get("error") or "speed_test_failed"),
        )
