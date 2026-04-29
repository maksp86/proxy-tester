from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import logging
import socket
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from time import monotonic

import aiodns

from app.config import CIDRConfig

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class _DNSCacheItem:
    expires_at: float
    addresses: tuple[str, ...]


class CIDRReader:
    def __init__(self, config: CIDRConfig) -> None:
        self.config = config

        self._path: Path | None = None
        self._networks: list[ipaddress.IPv4Network | ipaddress.IPv6Network] | None = (
            None
        )

        self._resolver_lock = asyncio.Lock()
        self._networks_lock = asyncio.Lock()
        self._dns_lock = asyncio.Lock()

        self._resolvers: list[aiodns.DNSResolver] | None = None
        self._dns_cache: dict[str, _DNSCacheItem] = {}
        self._dns_inflight: dict[str, asyncio.Task[tuple[str, ...]]] = {}

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
    ) -> list[ipaddress.IPv4Network | ipaddress.IPv6Network]:
        networks: list[ipaddress.IPv4Network | ipaddress.IPv6Network] = []

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
        try:
            if self._networks is None or self._path is None:
                self.ensure_cidr_reader()

            if not self._networks:
                return False

            addresses = await self._resolve_host_addresses(host)
            if not addresses:
                return False

            matched = any(
                ipaddress.ip_address(addr) in network
                for addr in addresses
                for network in self._networks
            )

            return not matched if self.config.method == "exclude" else matched

        except Exception as exc:
            LOGGER.warning("Failed to check host %s: %s", host, exc)
            return False

    async def close(self) -> None:
        resolvers = self._resolvers
        self._resolvers = None

        self._dns_cache.clear()

        for task in self._dns_inflight.values():
            task.cancel()
        self._dns_inflight.clear()

        if not resolvers:
            return

        for resolver in resolvers:
            await resolver.close()

    async def _ensure_resolvers(self) -> list[aiodns.DNSResolver]:
        if self._resolvers is not None:
            return self._resolvers

        async with self._resolver_lock:
            if self._resolvers is not None:
                return self._resolvers

            loop = asyncio.get_running_loop()
            pool = self.config.dns_nameservers_pool or [[]]

            resolvers: list[aiodns.DNSResolver] = []
            for nameservers in pool:
                if nameservers:
                    resolvers.append(
                        aiodns.DNSResolver(loop=loop, nameservers=nameservers)
                    )
                else:
                    resolvers.append(aiodns.DNSResolver(loop=loop))

            self._resolvers = resolvers
            return resolvers

    async def _get_resolver(self, host_key: str) -> aiodns.DNSResolver:
        resolvers = await self._ensure_resolvers()
        if len(resolvers) == 1:
            return resolvers[0]

        idx = int.from_bytes(
            hashlib.blake2s(host_key.encode("utf-8"), digest_size=2).digest(),
            "big",
        ) % len(resolvers)
        return resolvers[idx]

    async def _resolve_host_addresses(self, host: str) -> tuple[str, ...]:
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

    async def _resolve_host_addresses_uncached(self, host_key: str) -> tuple[str, ...]:
        resolver = await self._get_resolver(host_key)

        try:
            result = await resolver.getaddrinfo(
                host_key,
                family=socket.AF_UNSPEC,
                port=None,
                proto=0,
                type=0,
                flags=0,
            )
        except aiodns.error.DNSError as exc:
            LOGGER.debug("DNS lookup failed for %s: %s", host_key, exc)
            return ()

        addresses: list[str] = []
        seen: set[str] = set()

        for node in result.nodes:
            addr = node.addr[0]
            addr_str = addr.decode() if isinstance(addr, bytes) else str(addr)
            if addr_str not in seen:
                seen.add(addr_str)
                addresses.append(addr_str)

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
