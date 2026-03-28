from __future__ import annotations

import asyncio
import random

from .models import CandidateProxy, SpeedTestResult, UrlTestResult


class ProxyProbe:
    """
    Adapter point for real proxy checks.

    Replace this class with integration against libXray/python_v2ray + urltest/speedtest
    runtime. The current implementation is a lightweight stub so orchestration can run.
    """

    async def url_test(self, candidate: CandidateProxy, urls: tuple[str, ...], timeout_s: float) -> UrlTestResult:
        await asyncio.sleep(0)
        # TODO: replace with real URL test through xray runtime
        ok = random.random() > 0.35
        if not ok:
            return UrlTestResult(proxy_hash=candidate.proxy_hash, success=False, reason="url_test_failed")

        latency = round(random.uniform(80, 900), 2)
        return UrlTestResult(
            proxy_hash=candidate.proxy_hash,
            success=True,
            latency_ms=latency,
            exit_ip="203.0.113.10",
            country="US",
            city="New York",
        )

    async def speed_test(
        self,
        candidate: CandidateProxy,
        download_url: str,
        timeout_s: float,
    ) -> SpeedTestResult:
        await asyncio.sleep(0)
        # TODO: replace with real speed test through selected proxy route
        mbps = round(random.uniform(0.2, 8.0), 3)
        ok = mbps >= 1.0
        return SpeedTestResult(
            proxy_hash=candidate.proxy_hash,
            success=ok,
            mbps=mbps,
            bytes_downloaded=10 * 1024 * 1024 if ok else int(2 * 1024 * 1024),
            reason=None if ok else "low_bandwidth",
        )
