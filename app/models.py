from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class CandidateProxy:
    proxy_hash: str
    raw_link: str
    scheme: str


@dataclass(slots=True)
class UrlTestResult:
    proxy_hash: str
    success: bool
    latency_ms: float | None = None
    exit_ip: str | None = None
    country: str | None = None
    city: str | None = None
    reason: str | None = None


@dataclass(slots=True)
class SpeedTestResult:
    proxy_hash: str
    success: bool
    mbps: float | None = None
    bytes_downloaded: int | None = None
    reason: str | None = None
