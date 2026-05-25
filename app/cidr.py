from __future__ import annotations

import asyncio
import ipaddress
import logging
import socket
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from time import monotonic, perf_counter
from typing import List, Optional, Set, Tuple

import dns.asyncresolver
import dns.exception

from app.config import CIDRConfig

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class _DNSCacheItem:
    expires_at: float
    addresses: Tuple[str, ...]


class CIDRReader:
    def __init__(self, config: CIDRConfig) -> None:
        self.config = config

        self._path: Optional[Path] = None
        self._networks: Optional[
            List[ipaddress.IPv4Network | ipaddress.IPv6Network]
        ] = None

        self._resolver_lock = asyncio.Lock()
        self._dns_lock = asyncio.Lock()

        self._resolver: Optional[dns.asyncresolver.Resolver] = None
        self._dns_cache: dict[str, _DNSCacheItem] = {}
        self._dns_inflight: dict[str, asyncio.Task[Tuple[str, ...]]] = {}
        self._dns_semaphore = asyncio.Semaphore(50)

    def ensure_cidr_reader(self) -> None:
        path = Path(self.config.path)

        if not path.exists():
            if self.config.url is None:
                raise FileNotFoundError(f"CIDR file not found: {path}")

            path.parent.mkdir(parents=True, exist_ok=True)
            LOGGER.info("Downloading CIDR file from %s to %s", self.config.url, path)
            urllib.request.urlretrieve(str(self.config.url), str(path))
            LOGGER.info("CIDR file downloaded to %s", path)

        self._path = path
        self._networks = self._parse_networks(path)

    def _parse_networks(
        self, path: Path
    ) -> List[ipaddress.IPv4Network | ipaddress.IPv6Network]:
        networks: List[ipaddress.IPv4Network | ipaddress.IPv6Network] = []

        with path.open("r", encoding="utf-8") as f:
            for line_no, raw_line in enumerate(f, start=1):
                line = raw_line.strip()
                if not line or line.startswith("#"):
                    continue
                try:
                    networks.append(ipaddress.ip_network(line, strict=False))
                except ValueError as exc:
                    LOGGER.warning(
                        "Skipping invalid CIDR on line %d in %s: %s",
                        line_no,
                        path,
                        exc,
                    )

        return networks

    async def filter(self, host: str) -> bool:
        start = perf_counter()
        try:
            if self._networks is None or self._path is None:
                self.ensure_cidr_reader()

            if not self._networks:
                return False

            addresses = await self._resolve_host_addresses(host)
            elapsed_resolve = perf_counter() - start

            if not addresses:
                return False

            matched = any(
                ipaddress.ip_address(addr) in network
                for addr in addresses
                for network in self._networks
            )
            elapsed = perf_counter() - start
            if elapsed > 1:
                LOGGER.debug(
                    "filter %s took %.3fs (resolve stage: %.3fs)",
                    host,
                    elapsed,
                    elapsed_resolve,
                )

            return not matched if self.config.method == "exclude" else matched

        except Exception as exc:
            LOGGER.warning("Failed to check host %s: %s", host, exc)
            return False

    async def close(self) -> None:
        if self._resolver is not None:
            self._resolver = None

        self._dns_cache.clear()

        for task in self._dns_inflight.values():
            task.cancel()
        self._dns_inflight.clear()

    async def _ensure_resolver(self) -> dns.asyncresolver.Resolver:
        if self._resolver is not None:
            return self._resolver

        async with self._resolver_lock:
            if self._resolver is not None:
                return self._resolver

            resolver = dns.asyncresolver.Resolver()

            all_nameservers = []
            for ns_list in self.config.dns_nameservers_pool or [[]]:
                all_nameservers.extend(ns_list)
            if all_nameservers:
                resolver.nameservers = all_nameservers

            self._resolver = resolver
            return resolver

    async def _resolve_host_addresses(self, host: str) -> Tuple[str, ...]:
        host_key = host.strip().rstrip(".").lower()

        try:
            ip = ipaddress.ip_address(host_key)
            return (str(ip),)
        except ValueError:
            pass

        now = monotonic()

        async with self._dns_lock:
            cached = self._dns_cache.get(host_key)
            if cached is not None and cached.expires_at > now:
                return cached.addresses

            task = self._dns_inflight.get(host_key)
            if task is None:
                task = asyncio.create_task(
                    self._resolve_host_addresses_uncached(host_key)
                )
                self._dns_inflight[host_key] = task

        try:
            return await task
        finally:
            async with self._dns_lock:
                if self._dns_inflight.get(host_key) is task and task.done():
                    self._dns_inflight.pop(host_key, None)

    async def _resolve_host_addresses_uncached(self, host_key: str) -> Tuple[str, ...]:
        async with self._dns_semaphore:
            resolver = await self._ensure_resolver()
            try:
                result = await resolver.resolve_name(host_key, family=socket.AF_UNSPEC)
            except (dns.exception.DNSException, Exception) as exc:
                LOGGER.debug("DNS lookup failed for %s: %s", host_key, exc)
                return ()

            addresses: List[str] = []
            seen: Set[str] = set()

            for addr in result.addresses():
                if addr not in seen:
                    seen.add(addr)
                    addresses.append(addr)

            if not addresses:
                return ()

            resolved = tuple(addresses)
            expires_at = monotonic() + max(0, int(self.config.dns_cache_ttl))

            async with self._dns_lock:
                self._dns_cache[host_key] = _DNSCacheItem(
                    expires_at=expires_at,
                    addresses=resolved,
                )

            return resolved
