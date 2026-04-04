from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class AppConfig:
    """Application runtime settings loaded from JSON.

    Attributes:
        db_path: SQLite database file path.
        export_file: Output file path for selected proxies.
        subscription_urls: Subscription endpoints used for candidate collection.
        geoip_db_path: Local GeoIP database path (`.mmdb`).
        geoip_db_url: Optional URL for downloading `geoip_db_path` at startup.
        url_batch_size: Maximum concurrent URL tests (rolling window).
        url_timeout_seconds: Timeout for URL reachability checks.
        url_test_attempts: Number of URL-test attempts per proxy (best latency wins).
        url_urls: Candidate healthcheck URLs; first URL is used as probe target.
        speed_top_n: Number of lowest-latency proxies to include in speed stage.
        speed_batch_size: Maximum concurrent speed tests (rolling window).
        speed_timeout_seconds: Timeout for speed checks.
        speed_min_mb_s: Minimum accepted download speed in MB/s.
        speed_test_url: URL used for download speed checks.
        target_final_count: Target number of exported proxies.
        dead_ttl_days: How long failed proxies remain in dead-list.
    """

    db_path: Path = Path("proxy_pool.sqlite3")
    export_file: Path = Path("result.txt")
    subscription_urls: tuple[str, ...] = ()

    # Local GeoIP settings (file-only lookups, no network API calls during resolution)
    geoip_db_path: Path = Path("geoip/GeoLite2-City.mmdb")
    geoip_db_url: str | None = None

    # URL test settings
    url_batch_size: int = 25
    url_timeout_seconds: float = 1.0
    test_attempts: int = 1
    url_test_url: str = "https://www.gstatic.com/generate_204"

    # Speed test settings
    speed_top_n: int = 100
    speed_batch_size: int = 25
    speed_timeout_seconds: float = 10.0
    speed_min_mb_s: float = 1.0
    speed_test_url: str = "https://cachefly.cachefly.net/10mb.test"

    # Output settings
    target_final_count: int = 25

    # Dead list TTL
    dead_ttl_days: int = 30


DEFAULT_CONFIG = AppConfig()
_PATH_FIELDS = {"db_path", "export_file", "geoip_db_path"}
_TUPLE_FIELDS = {"subscription_urls"}


def _coerce_config_value(key: str, value: Any) -> Any:
    """Convert raw JSON config value into the type expected by AppConfig."""

    if key in _PATH_FIELDS and value is not None:
        return Path(str(value))
    if key in _TUPLE_FIELDS and value is not None:
        if isinstance(value, (list, tuple)):
            return tuple(str(item) for item in value)
        return (str(value),)
    return value


def _read_config_payload(config_path: Path) -> dict[str, Any]:
    """Read JSON config payload from disk.

    Raises:
        ValueError: If file extension is not `.json` or payload root is not an object.
    """

    suffix = config_path.suffix.lower()
    if suffix != ".json":
        raise ValueError(f"Unsupported config format '{suffix}'. Use .json")

    payload = json.loads(config_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Config root must be an object/dictionary")
    return payload


def load_config(config_path: Path | None) -> AppConfig:
    """Load JSON configuration and merge it over defaults.

    Args:
        config_path: Path to JSON config file. If `None`, returns defaults.
    """

    if config_path is None:
        logging.debug("No --config provided. Using DEFAULT_CONFIG.")
        return DEFAULT_CONFIG

    resolved = config_path.expanduser().resolve()
    logging.info("Loading configuration from %s", resolved)
    payload = _read_config_payload(resolved)

    data = asdict(DEFAULT_CONFIG)
    for key, value in payload.items():
        if key not in data:
            logging.warning("Ignoring unknown config key: %s", key)
            continue
        data[key] = _coerce_config_value(key, value)

    config = AppConfig(**data)
    logging.debug("Configuration loaded: %s", config)
    return config
