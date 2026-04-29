import asyncio
from collections import deque
from threading import Lock
from typing import Any, AsyncIterator, Deque

from app.db import Database
from app.models import CandidateProxy, ProxyTestResult
from app.xray_backend import XrayToolchain


class BatchTestResultWriter:
    def __init__(self, db: Database, batch_size: int):
        self._db = db
        self.batch_size = batch_size
        self.buffer: list[ProxyTestResult] = []
        self.lock = Lock()

    def add(self, result: ProxyTestResult):
        with self.lock:
            self.buffer.append(result)

            if len(self.buffer) >= self.batch_size:
                self.flush()

    def flush(self):
        if not self.buffer:
            return

        batch = self.buffer
        self.buffer = []

        self._db.mark_results(batch)


class BatchCandidateReader:
    def __init__(
        self, db: Database, toolchain: XrayToolchain, prepare_batch_size: int
    ) -> None:
        self._db = db
        self._toolchain = toolchain
        self._prepare_batch_size = prepare_batch_size

        self._buffer: Deque[tuple[CandidateProxy, dict[str, Any] | None]] = deque()
        self._lock = asyncio.Lock()
        self._last_proxy_hash: str | None = None
        self._finished = False

    async def _fill_buffer(self) -> None:
        if self._finished:
            return

        proxies = self._db.fetch_candidate_proxies_batch(
            self._prepare_batch_size,
            after_proxy_hash=self._last_proxy_hash,
        )

        if not proxies:
            self._finished = True
            return

        self._last_proxy_hash = proxies[-1].proxy_hash

        links = [p.raw_link for p in proxies]
        converted = await self._toolchain.convert_links(links)

        for proxy in proxies:
            outbound = converted.get(proxy.raw_link, None)

            self._buffer.append((proxy, outbound))

    async def _ensure_buffer(self) -> None:
        if self._buffer or self._finished:
            return

        async with self._lock:
            if not self._buffer and not self._finished:
                await self._fill_buffer()

    def __aiter__(self) -> AsyncIterator[tuple[CandidateProxy, dict[str, Any] | None]]:
        return self

    async def __anext__(self) -> tuple[CandidateProxy, dict[str, Any] | None]:
        await self._ensure_buffer()

        if not self._buffer:
            raise StopAsyncIteration

        return self._buffer.popleft()
