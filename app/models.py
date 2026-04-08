from __future__ import annotations

import sqlite3
from dataclasses import dataclass


@dataclass(slots=True)
class CandidateProxy:
    def __init__(self, proxy_hash: str, raw_link: str, scheme: str) -> None:
        self.proxy_hash = proxy_hash
        self.raw_link = raw_link
        self.scheme = scheme

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> CandidateProxy:
        return cls(
            proxy_hash=row["proxy_hash"],
            raw_link=row["raw_link"],
            scheme=row["scheme"] if "scheme" in row else "selected",
        )

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
class Subscripton:
    link: str
    last_data_hash: str


@dataclass(slots=True)
class SpeedTestResult:
    proxy_hash: str
    success: bool
    mbps: float | None = None
    bytes_downloaded: int | None = None
    reason: str | None = None
