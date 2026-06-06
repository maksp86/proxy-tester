from __future__ import annotations

import asyncio
import ipaddress
import logging
import socket
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from typing import List, Optional, Set, Tuple

import dns.asyncresolver
import dns.exception
import dns.resolver  # Добавили импорт для перехвата NXDOMAIN

from app.config import CIDRConfig
from app.helpers import FastNetworkSearch

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class _DNSCacheItem:
    matched_addresses: Tuple[str, ...]
    result: bool


class CIDRReader:
    def __init__(self, config: CIDRConfig) -> None:
        self.config = config

        self._network_search = FastNetworkSearch()

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

        self._network_search.ensure_networks(path)

    async def filter(self, host: str) -> bool:
        start = perf_counter()
        try:
            host_key = host.strip().rstrip(".").lower()

            async with self._dns_lock:
                if host_key in self._dns_cache:
                    return self._dns_cache[host_key].result

            try:
                addresses = await self._resolve_host_addresses(host_key)
                elapsed_resolve = perf_counter() - start

                matched_addresses = tuple(
                    addr
                    for addr in addresses
                    if self._network_search.find_network(addr)
                )
                matched = len(matched_addresses) > 0
                result = not matched if self.config.method == "exclude" else matched

            except dns.resolver.NXDOMAIN:
                elapsed_resolve = perf_counter() - start
                LOGGER.debug(
                    "Host %s returned NXDOMAIN. Caching result as False.", host_key
                )
                matched_addresses = ()
                result = False

            elapsed = perf_counter() - start
            if elapsed > 1:
                LOGGER.debug(
                    "filter %s (key %s) took %.3fs (resolve stage: %.3fs)",
                    host,
                    host_key,
                    elapsed,
                    elapsed_resolve,
                )

            async with self._dns_lock:
                self._dns_cache[host_key] = _DNSCacheItem(
                    matched_addresses=matched_addresses, result=result
                )

            return result

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

    async def _resolve_host_addresses(self, host_key: str) -> Tuple[str, ...]:
        try:
            ip = ipaddress.ip_address(host_key)
            return (str(ip),)
        except ValueError:
            pass

        async with self._dns_lock:
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
            except dns.resolver.NXDOMAIN:
                raise
            except (dns.exception.DNSException, Exception) as exc:
                LOGGER.debug("DNS lookup failed for %s: %s", host_key, exc)
                return ()

            addresses: List[str] = []
            seen: Set[str] = set()

            for addr in result.addresses():
                if addr not in seen:
                    seen.add(addr)
                    addresses.append(addr)

            return tuple(addresses)
