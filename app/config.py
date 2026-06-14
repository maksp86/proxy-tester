from __future__ import annotations

import re
from pathlib import Path
from typing import Any, List, Literal, Tuple

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    HttpUrl,
    field_validator,
    model_validator,
)

_TIME_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(ms|s|m)?\s*$", re.IGNORECASE)


def parse_time(value: Any, is_timeout: bool = True) -> float:
    if is_timeout and isinstance(value, (int, float)):
        if value <= 0:
            raise ValueError("timeout must be > 0")
        return float(value)

    if isinstance(value, str):
        m = _TIME_RE.match(value)
        if not m:
            raise ValueError("time must look like 10, 10s, 500ms or 2m")

        num = float(m.group(1))
        if num <= 0 and is_timeout:
            raise ValueError("timeout must be > 0")

        unit = (m.group(2) or "s").lower()
        if unit == "ms":
            return num / 1000.0
        if unit == "m":
            return num * 60.0
        return num

    raise TypeError("time must be a number or a string")


def _non_empty_path(v: Any) -> Any:
    if isinstance(v, str) and not v.strip():
        raise ValueError("path must not be empty")
    return v


def _speed_test_url() -> HttpUrl:
    return HttpUrl("http://cachefly.cachefly.net/10mb.test")


def _url_test_url() -> HttpUrl:
    return HttpUrl("https://www.gstatic.com/generate_204")


class SpeedTestConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    url: HttpUrl = Field(default_factory=_speed_test_url)
    worker_count: int = 5
    worker_tasks_count: int = 5
    speed_threshold: float = 1
    timeout: float = 10.0

    @field_validator("timeout", mode="before")
    @classmethod
    def _parse_timeout(cls, v: Any) -> float:
        return parse_time(v)

    @field_validator("worker_count", "worker_tasks_count")
    @classmethod
    def _positive_int(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("must be > 0")
        return v


class UrlTestConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    url: HttpUrl = Field(default_factory=_url_test_url)
    worker_count: int = 5
    worker_tasks_count: int = 10
    timeout: float = 2.0

    @field_validator("timeout", mode="before")
    @classmethod
    def _parse_timeout(cls, v: Any) -> float:
        return parse_time(v)

    @field_validator("worker_count", "worker_tasks_count")
    @classmethod
    def _positive_int(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("must be > 0")
        return v


class ConnectTestConfig(BaseModel):
    timeout: float = 2.0
    concurrent_tasks: int = 50
    attempts: int = 3


class TesterConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    speed_test: SpeedTestConfig = Field(default_factory=SpeedTestConfig)
    url_test: UrlTestConfig = Field(default_factory=UrlTestConfig)
    connect_test: ConnectTestConfig | None = None
    target_final_count: int = 25
    test_attempts: int = 3
    cooldown_time: float = 10.0

    @field_validator("cooldown_time", mode="before")
    @classmethod
    def _parse_cooldown_time(cls, v: Any) -> float:
        return parse_time(v, False)

    @field_validator("target_final_count", "test_attempts")
    @classmethod
    def _positive_int(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("must be > 0")
        return v


class GeoIPConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    url: HttpUrl | None = None
    path: Path = Path("GeoLite2-City.mmdb")
    method: Literal["exclude", "include"] = "exclude"
    countries: list[str] = Field(default_factory=list)

    @field_validator("path", mode="before")
    @classmethod
    def _path_not_empty(cls, v: Any) -> Any:
        return _non_empty_path(v)

    @field_validator("countries", mode="before")
    @classmethod
    def _normalize_countries(cls, v: Any) -> list[str]:
        if v is None:
            return []
        if not isinstance(v, list):
            return v
        return [str(x).strip().upper() for x in v if str(x).strip()]


class ProxyFilterConfig(BaseModel):
    excluded_protocols: List[str] = Field(default_factory=list)
    excluded_ports: List[Tuple[int, int]] = Field(default_factory=list)

    @field_validator("excluded_ports", mode="before")
    @classmethod
    def parse_ports(cls, value):
        if isinstance(value, list):
            return value

        if not isinstance(value, str):
            raise TypeError("excluded_ports must be a string")

        ranges = []

        for part in value.split(","):
            part = part.strip()

            if "-" in part:
                start_str, end_str = part.split("-", 1)

                start = int(start_str)
                end = int(end_str)

                if start < 1 or end > 65535 or start > end:
                    raise ValueError(f"Invalid port range: {part}")

                ranges.append((start, end))
            else:
                port = int(part)

                if port < 1 or port > 65535:
                    raise ValueError(f"Invalid port: {port}")

                ranges.append((port, port))

        return ranges


class CIDRConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    url: HttpUrl | None = None
    path: Path = Path("cidr.txt")
    method: Literal["exclude", "include"] = "exclude"

    concurrent_tasks: int = 50

    # Each item is one resolver, and each resolver may contain one or more nameservers.
    # Example:
    # [
    #   ["1.1.1.1", "1.0.0.1"],
    #   ["8.8.8.8", "8.8.4.4"],
    # ]
    dns_nameservers_pool: list[list[str]] = [["8.8.8.8"]]
    dns_cache_ttl: int = 60

    @field_validator("path", mode="before")
    @classmethod
    def _path_not_empty(cls, v: Any) -> Any:
        if v is None:
            return None
        return _non_empty_path(v)

    @model_validator(mode="after")
    def _require_url_or_path(self) -> "CIDRConfig":
        if self.url is None and self.path is None:
            raise ValueError("cidr must contain at least one of: url or path")
        return self


class ExportConfig(BaseModel):
    file: Path = Path("result.txt")
    title: str = "proxy-tester"
    update_interval: int = 24

    separate_headers_file: Path | None = None

    web_page_url: HttpUrl | None = None
    support_url: HttpUrl | None = None


class FilterConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    deduplicate: bool = True

    geoip: GeoIPConfig | None = None
    cidr: CIDRConfig | None = None
    proxy: ProxyFilterConfig = Field(default_factory=ProxyFilterConfig)

    @field_validator("geoip", "cidr", mode="before")
    @classmethod
    def _empty_dict_to_none(cls, v: Any) -> Any:
        return None if v == {} else v


class DeadTTLConfig(BaseModel):
    cidr_discarded: int = 30
    connect_failed: int = 30
    latency_exceeded: int = 30
    other: int = 30
    speed_test_failed: int = 30
    speed_below_threshold: int = 30
    url_test_failed: int = 30
    discarded_filtering: int = 30


class AppConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    db_path: Path = Path("proxy_pool.sqlite3")
    subscription_urls: list[HttpUrl] = Field(min_length=1)
    fetch_proxy: HttpUrl | None = None

    tester: TesterConfig = Field(default_factory=TesterConfig)
    filter: FilterConfig = Field(default_factory=FilterConfig)

    dead_ttl: DeadTTLConfig = Field(default_factory=DeadTTLConfig)

    export_options: ExportConfig = Field(default_factory=ExportConfig)

    @field_validator("db_path", mode="before")
    @classmethod
    def _path_not_empty(cls, v: Any) -> Any:
        return _non_empty_path(v)

    @classmethod
    def from_json_file(cls, file_path: str | Path) -> "AppConfig":
        return cls.model_validate_json(Path(file_path).read_text(encoding="utf-8"))

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "AppConfig":
        return cls.model_validate(data)
