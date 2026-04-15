from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class AppConfig:
    """Application runtime settings loaded from JSON."""

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

    # Shared Xray worker pool settings
    xray_worker_count: int = 10
    xray_tasks_per_worker: int = 100

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

    exclude_countries: tuple[str, ...] = ()


DEFAULT_CONFIG = AppConfig()
_PATH_FIELDS = {"db_path", "export_file", "geoip_db_path"}
_TUPLE_FIELDS = {"subscription_urls", "exclude_countries"}


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
    """Read JSON config payload from disk."""

    suffix = config_path.suffix.lower()
    if suffix != ".json":
        raise ValueError(f"Unsupported config format '{suffix}'. Use .json")

    payload = json.loads(config_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Config root must be an object/dictionary")
    return payload


def load_config(config_path: Path | None) -> AppConfig:
    """Load JSON configuration and merge it over defaults."""

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
