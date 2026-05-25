import asyncio
from collections import deque
from threading import Lock
from typing import Any, Deque, Iterable

from app.binary_toolchain import BinaryToolchain
from app.db import Database
from app.models import CandidateProxy, ProxyTestResult


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

    def add_many(self, result: Iterable[ProxyTestResult]):
        with self.lock:
            self.buffer.extend(result)

            if len(self.buffer) >= self.batch_size:
                self.flush()

    def flush(self):
        if not self.buffer:
            return

        batch = self.buffer

        self._db.mark_results(batch)
        self.buffer.clear()
        self.buffer = []


class BatchCandidateReader:
    def __init__(
        self, db: Database, toolchain: BinaryToolchain, prepare_batch_size: int
    ) -> None:
        self._db = db
        self._toolchain = toolchain
        self._prepare_batch_size = prepare_batch_size

        self._buffer: Deque[tuple[CandidateProxy, dict[str, Any] | None]] = deque()
        self._lock = asyncio.Lock()
        self._last_proxy_hash: str | None = None
        self._finished = False
        self.position = 0

    def is_finished(self) -> bool:
        return self._finished

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

        self._buffer.extend((proxy, converted.get(proxy.raw_link)) for proxy in proxies)

    async def _ensure_buffer(self) -> None:
        if self._buffer or self._finished:
            return

        async with self._lock:
            if not self._buffer and not self._finished:
                await self._fill_buffer()

    async def take(self, n: int) -> list[tuple[CandidateProxy, dict[str, Any] | None]]:
        output: list[tuple[CandidateProxy, dict[str, Any] | None]] = []

        while n > 0:
            await self._ensure_buffer()
            if not self._buffer:
                break

            take_now = min(n, len(self._buffer))
            for _ in range(take_now):
                async with self._lock:
                    output.append(self._buffer.popleft())
                    self.position += 1
            n -= take_now

        return output
