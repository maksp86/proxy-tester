from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from enum import Enum
from typing import Any


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
    INVALID_OUTBOUND = "invalid_outbound"
    URL_FAIL = "url_test_failed"
    LATENCY_EXCEEDED = "latency_exceeded"
    SPEED_FAIL = "speed_test_failed"
    SPEED_BELOW_THRESHOLD = "speed_below_threshold"
    CIDR_DISCARDED = "cidr_discarded"
    CONNECT_FAILED = "connect_failed"

    @classmethod
    def from_str(cls, value: str | None) -> TestResultReasons:
        if value is None:
            return cls.UNKNOWN
        try:
            return cls(value)
        except ValueError:
            return cls.UNKNOWN


class TestResultKind(str, Enum):
    URL = "url"
    SPEED = "speed"
    CIDR = "cidr"
    CONNECT = "connect"


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

    @classmethod
    def from_dict(
        cls, tag: str, kind: TestResultKind, data: dict[str, Any]
    ) -> ProxyTestResult:
        reason_str = data.get("reason", "test_failed")
        if reason_str == "test_failed":
            reason_str = f"{kind.value}_{reason_str}"

        reason = TestResultReasons.from_str(reason_str)
        if reason.value == "unknown":
            pass
        return cls(
            proxy_hash=tag,
            kind=kind,
            success=data.get("result", False),
            reason=reason,
            latency_ms=data.get("latency", None),
            exit_ip=data.get("exit-ip", None),
            country=data.get("country", None),
            city=data.get("city", None),
            mbps=data.get("speed", None),
        )


@dataclass(slots=True)
class Subscripton:
    link: str
    last_data_hash: str
