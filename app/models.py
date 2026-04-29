from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from enum import Enum


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

    def to_row(self) -> tuple[str, str, str]:
        return (self.proxy_hash, self.raw_link, self.scheme)

    proxy_hash: str
    raw_link: str
    scheme: str


class TestResultReasons(Enum):
    OK = "ok"
    UNKNOWN = "unknown"
    INVALID_URI = "invalid_proxy_uri"
    URL_FAIL = "url_test_failed"
    SPEED_FAIL = "speed_test_failed"
    SPEED_BELOW_THRESHOLD = "speed_below_threshold"
    CIDR_DISCARDED = "cidr_discarded"


class TestResultKind(str, Enum):
    URL = "url"
    SPEED = "speed"
    CIDR = "cidr"


@dataclass(slots=True)
class ProxyTestResult:
    proxy_hash: str
    kind: TestResultKind
    success: bool
    reason: TestResultReasons = TestResultReasons.UNKNOWN
    latency_ms: float | None = None
    exit_ip: str | None = None
    country: str | None = None
    city: str | None = None
    mbps: float | None = None


@dataclass(slots=True)
class Subscripton:
    link: str
    last_data_hash: str
