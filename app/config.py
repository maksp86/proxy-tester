from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class AppConfig:
    db_path: Path = Path("proxy_pool.sqlite3")
    sources_file: Path = Path("sources.txt")
    export_file: Path = Path("result.txt")
    # URL test settings
    url_batch_size: int = 20
    url_timeout_seconds: float = 1.0
    url_urls: tuple[str, ...] = (
        "https://www.gstatic.com/generate_204",
        "https://www.youtube.com/generate_204",
        "https://x.com",
    )

    # Speed test settings
    speed_top_n: int = 100
    speed_batch_size: int = 5
    speed_timeout_seconds: float = 10.0
    speed_min_mb_s: float = 1.0
    speed_test_url: str = (
        "https://github.com/jamesward/play-load-tests/raw/refs/heads/master/public/10mb.txt"
    )

    # Output settings
    target_final_count: int = 25

    # Dead list TTL
    dead_ttl_days: int = 30


DEFAULT_CONFIG = AppConfig()
