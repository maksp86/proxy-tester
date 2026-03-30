from __future__ import annotations

import asyncio
import ipaddress
import logging
from pathlib import Path

import geoip2.database
from geoip2.errors import AddressNotFoundError
from python_v2ray.config_parser import parse_uri
from python_v2ray.downloader import BinaryDownloader
from python_v2ray.tester import ConnectionTester

from .models import CandidateProxy, SpeedTestResult, UrlTestResult

LOGGER = logging.getLogger(__name__)


class ProxyProbe:
    """Proxy probing adapter over `python_v2ray` with safe orchestration.

    The upstream `ConnectionTester` starts local proxy cores on a static base port.
    Running multiple tester calls concurrently causes port collisions. This class
    serializes tester invocations and exposes batch APIs that map raw tester payloads
    back to domain-level result objects.
    """

    def __init__(self, project_root: Path | None = None, geoip_db_path: Path | None = None) -> None:
        """Initialize probe.

        Args:
            project_root: Root where python_v2ray vendor binaries are stored.
            geoip_db_path: Path to local GeoIP `.mmdb`.
        """

        self._project_root = project_root or Path.cwd()
        self._tester: ConnectionTester | None = None
        self._setup_error: str | None = None
        self._tester_lock = asyncio.Lock()

        self._geoip_db_path = geoip_db_path
        self._geoip_reader: geoip2.database.Reader | None = None
        self._geoip_error: str | None = None

    def _ensure_tester(self) -> ConnectionTester:
        """Create and cache `ConnectionTester`.

        Returns:
            Ready `ConnectionTester` instance.

        Raises:
            RuntimeError: If tester bootstrap fails.
        """

        if self._tester is not None:
            return self._tester
        if self._setup_error is not None:
            raise RuntimeError(self._setup_error)

        try:
            LOGGER.info("Preparing python_v2ray binaries in %s", self._project_root)
            downloader = BinaryDownloader(self._project_root)
            downloader.ensure_all()
            self._tester = ConnectionTester(
                vendor_path=str(downloader.vendor_path),
                core_engine_path=str(downloader.core_engine_path),
            )
            LOGGER.info("ConnectionTester initialized successfully")
            return self._tester
        except Exception as exc:
            self._setup_error = f"python_v2ray setup failed: {exc}"
            LOGGER.exception("Failed to initialize ConnectionTester")
            raise RuntimeError(self._setup_error) from exc

    def _ensure_geoip_reader(self) -> geoip2.database.Reader | None:
        """Create and cache GeoIP reader.

        Returns:
            GeoIP reader instance or `None` when unavailable.
        """

        if self._geoip_reader is not None:
            return self._geoip_reader
        if self._geoip_error is not None:
            return None
        if self._geoip_db_path is None:
            self._geoip_error = "geoip_db_path_not_set"
            LOGGER.debug("GeoIP disabled: %s", self._geoip_error)
            return None

        db_path = self._geoip_db_path
        try:
            if not db_path.exists():
                self._geoip_error = f"geoip_db_not_found:{db_path}"
                LOGGER.warning("GeoIP DB not found: %s", db_path)
                return None
            self._geoip_reader = geoip2.database.Reader(str(db_path))
            LOGGER.info("GeoIP DB loaded from %s", db_path)
            return self._geoip_reader
        except Exception as exc:
            self._geoip_error = f"geoip_reader_init_failed:{exc}"
            LOGGER.exception("Failed to initialize GeoIP reader")
            return None

    def _geoip_lookup(self, ip_value: str | None) -> tuple[str | None, str | None]:
        """Resolve country/city for an IP.

        Args:
            ip_value: Public IP address from proxy test.

        Returns:
            Tuple `(country, city)` where values may be `None`.
        """

        if not ip_value:
            return None, None
        try:
            ipaddress.ip_address(ip_value)
        except ValueError:
            LOGGER.debug("Skipping GeoIP lookup for non-IP value: %s", ip_value)
            return None, None

        reader = self._ensure_geoip_reader()
        if reader is None:
            return None, None

        try:
            city_record = reader.city(ip_value)
        except AddressNotFoundError:
            LOGGER.debug("IP not found in GeoIP DB: %s", ip_value)
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
        """Run URL reachability test for a batch of candidates.

        Args:
            candidates: Proxy candidates.
            urls: Healthcheck URLs; first URL is used as ping target.
            timeout_s: Timeout for tester execution.

        Returns:
            List of per-candidate URL test results (same order as input).
        """

        if not candidates:
            return []

        ping_url = urls[0] if urls else "http://www.google.com/generate_204"
        parsed_jobs: list[tuple[CandidateProxy, object]] = []
        results: list[UrlTestResult] = []

        for candidate in candidates:
            params = parse_uri(candidate.raw_link)
            if params is None:
                results.append(
                    UrlTestResult(
                        proxy_hash=candidate.proxy_hash,
                        success=False,
                        reason="invalid_proxy_uri",
                    )
                )
                continue
            parsed_jobs.append((candidate, params))

        if not parsed_jobs:
            return results

        LOGGER.debug(
            "Starting URL test batch: size=%s ping_url=%s timeout=%s",
            len(parsed_jobs),
            ping_url,
            timeout_s,
        )

        async with self._tester_lock:
            try:
                tester = self._ensure_tester()
                payload = await asyncio.to_thread(
                    tester.test_uris,
                    [params for _, params in parsed_jobs],
                    timeout=max(int(timeout_s), 1),
                    ping_url=ping_url,
                )
            except Exception as exc:
                LOGGER.exception("URL test batch failed")
                for candidate, _ in parsed_jobs:
                    results.append(
                        UrlTestResult(
                            proxy_hash=candidate.proxy_hash,
                            success=False,
                            reason=f"url_test_runtime_error:{exc}",
                        )
                    )
                return results

        by_tag = {item.get("tag"): item for item in payload or []}
        for candidate, params in parsed_jobs:
            result = by_tag.get(params.display_tag)
            if not result or not result.get("success"):
                results.append(
                    UrlTestResult(
                        proxy_hash=candidate.proxy_hash,
                        success=False,
                        reason=(result or {}).get("error") or "url_test_failed",
                    )
                )
                continue

            exit_ip = result.get("ip")
            geo_country, geo_city = self._geoip_lookup(exit_ip)
            results.append(
                UrlTestResult(
                    proxy_hash=candidate.proxy_hash,
                    success=True,
                    latency_ms=result.get("latency_ms"),
                    exit_ip=exit_ip,
                    country=geo_country or result.get("country"),
                    city=geo_city or result.get("city"),
                )
            )
        LOGGER.debug("URL test batch finished: total=%s", len(results))
        return results

    async def speed_test_batch(
        self,
        candidates: list[CandidateProxy],
        download_url: str,
        timeout_s: float,
    ) -> list[SpeedTestResult]:
        """Run download speed tests for a batch of candidates.

        Args:
            candidates: Proxy candidates.
            download_url: URL used for download benchmark.
            timeout_s: Timeout for tester execution.

        Returns:
            List of per-candidate speed results.
        """

        if not candidates:
            return []

        parsed_jobs: list[tuple[CandidateProxy, object]] = []
        results: list[SpeedTestResult] = []

        for candidate in candidates:
            params = parse_uri(candidate.raw_link)
            if params is None:
                results.append(
                    SpeedTestResult(
                        proxy_hash=candidate.proxy_hash,
                        success=False,
                        reason="invalid_proxy_uri",
                    )
                )
                continue
            parsed_jobs.append((candidate, params))

        if not parsed_jobs:
            return results

        LOGGER.debug(
            "Starting speed test batch: size=%s url=%s timeout=%s",
            len(parsed_jobs),
            download_url,
            timeout_s,
        )

        async with self._tester_lock:
            try:
                tester = self._ensure_tester()
                payload = await asyncio.to_thread(
                    tester.test_speed,
                    [params for _, params in parsed_jobs],
                    timeout=max(int(timeout_s), 1),
                    download_url=download_url,
                )
            except Exception as exc:
                LOGGER.exception("Speed test batch failed")
                for candidate, _ in parsed_jobs:
                    results.append(
                        SpeedTestResult(
                            proxy_hash=candidate.proxy_hash,
                            success=False,
                            reason=f"speed_test_runtime_error:{exc}",
                        )
                    )
                return results

        by_tag = {item.get("tag"): item for item in payload or []}
        for candidate, params in parsed_jobs:
            result = by_tag.get(params.display_tag)
            if not result:
                results.append(
                    SpeedTestResult(
                        proxy_hash=candidate.proxy_hash,
                        success=False,
                        reason="speed_test_failed",
                    )
                )
                continue

            mbps = result.get("download_mbps")
            success = bool(result.get("success", mbps is not None and mbps > 0))
            results.append(
                SpeedTestResult(
                    proxy_hash=candidate.proxy_hash,
                    success=success,
                    mbps=mbps,
                    bytes_downloaded=result.get("bytes_downloaded"),
                    reason=None if success else (result.get("error") or "speed_test_failed"),
                )
            )

        LOGGER.debug("Speed test batch finished: total=%s", len(results))
        return results
