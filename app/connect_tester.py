from __future__ import annotations

import asyncio
import logging
import socket
from typing import Any

from app.batch_operations import BatchTestResultWriter
from app.config import TesterConfig
from app.helpers import extract_address
from app.models import (
    CandidateProxy,
    ProxyTestResult,
    TestResultKind,
    TestResultReasons,
)

LOGGER = logging.getLogger(__name__)


class ConnectTester:
    def __init__(
        self, config: TesterConfig, batch_writer: BatchTestResultWriter
    ) -> None:
        self._batch_writer = batch_writer
        self._config = config

    async def _test_tcp(
        self, host: str, port: int, timeout: float
    ) -> tuple[bool, TestResultReasons]:
        try:
            _, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port), timeout=timeout
            )
            writer.close()
            await writer.wait_closed()
            return True, TestResultReasons.OK
        except asyncio.TimeoutError:
            return False, TestResultReasons.LATENCY_EXCEEDED
        except Exception:
            return False, TestResultReasons.CONNECT_FAILED

    async def _test_udp(
        self, host: str, port: int, timeout: float
    ) -> tuple[bool, TestResultReasons]:
        loop = asyncio.get_running_loop()
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setblocking(False)

        try:
            await loop.sock_connect(sock, (host, port))
            await loop.sock_sendall(sock, b"\x00")

            try:
                await asyncio.wait_for(
                    loop.sock_recv(sock, 1), timeout=min(0.5, timeout)
                )
            except asyncio.TimeoutError:
                return True, TestResultReasons.OK

            return True, TestResultReasons.OK
        except ConnectionRefusedError:
            return False, TestResultReasons.CONNECT_FAILED
        except asyncio.TimeoutError:
            return False, TestResultReasons.LATENCY_EXCEEDED
        except Exception:
            return False, TestResultReasons.CONNECT_FAILED
        finally:
            sock.close()

    async def test_proxy(
        self, data: tuple[CandidateProxy, dict[str, Any] | None]
    ) -> bool:
        candidate, outbound = data

        if not outbound:
            self._batch_writer.add(
                ProxyTestResult(
                    proxy_hash=candidate.proxy_hash,
                    success=False,
                    reason=TestResultReasons.INVALID_OUTBOUND,
                    kind=TestResultKind.CONNECT,
                )
            )
            return False

        host, port = extract_address(outbound)
        if not host or not port:
            self._batch_writer.add(
                ProxyTestResult(
                    proxy_hash=candidate.proxy_hash,
                    success=False,
                    reason=TestResultReasons.INVALID_OUTBOUND,
                    kind=TestResultKind.CONNECT,
                )
            )
            return False

        protocol = str(outbound.get("protocol", "")).lower()

        timeout = self._config.url_test.timeout

        test_func = (
            self._test_udp
            if protocol in ("hysteria", "hysteria2", "tuic")
            else self._test_tcp
        )

        last_reason = TestResultReasons.CONNECT_FAILED

        for attempt in range(self._config.test_attempts):
            success, reason = await test_func(host, port, timeout)
            last_reason = reason

            if success:
                self._batch_writer.add(
                    ProxyTestResult(
                        proxy_hash=candidate.proxy_hash,
                        success=True,
                        reason=TestResultReasons.OK,
                        kind=TestResultKind.CONNECT,
                    )
                )
                return True

            if attempt < self._config.test_attempts - 1:
                await asyncio.sleep(0.2)

        self._batch_writer.add(
            ProxyTestResult(
                proxy_hash=candidate.proxy_hash,
                success=False,
                reason=last_reason,
                kind=TestResultKind.CONNECT,
            )
        )
        return False
