from __future__ import annotations

import argparse
import asyncio
import logging
import urllib.request
from pathlib import Path

from app.config import load_config
from app.db import Database
from app.pipeline import run_once
from app.probe import ProxyProbe


def setup_logging(verbose: bool) -> None:
    """Configure global logger.

    Args:
        verbose: If `True`, enables DEBUG logs; INFO otherwise.
    """

    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    logging.debug("Logging initialized. verbose=%s", verbose)


def ensure_geoip_database(geoip_db_path: Path, geoip_db_url: str | None) -> None:
    """Ensure local GeoIP database exists.

    Args:
        geoip_db_path: Local destination path for `.mmdb`.
        geoip_db_url: Optional download URL. If absent, no download is attempted.
    """

    if not geoip_db_url:
        logging.debug("geoip_db_url is not set. Skipping GeoIP download.")
        return

    logging.info("Downloading GeoIP DB from %s to %s", geoip_db_url, geoip_db_path)
    geoip_db_path.parent.mkdir(parents=True, exist_ok=True)
    urllib.request.urlretrieve(geoip_db_url, geoip_db_path)
    logging.info("GeoIP DB download complete: %s", geoip_db_path)


async def _amain(verbose: bool, config_path: Path | None) -> None:
    """Entrypoint for async pipeline execution.

    Args:
        verbose: Enables verbose logs when `True`.
        config_path: Optional path to JSON config file.
    """

    setup_logging(verbose)
    cfg = load_config(config_path)
    logging.info("Using config: %s", cfg)

    ensure_geoip_database(cfg.geoip_db_path, cfg.geoip_db_url)

    db = Database(cfg.db_path)
    probe = ProxyProbe(
        geoip_db_path=cfg.geoip_db_path,
        worker_count=cfg.xray_worker_count,
        tasks_per_worker=cfg.xray_tasks_per_worker,
    )
    await run_once(cfg, db, probe)


def main() -> None:
    """CLI wrapper."""

    parser = argparse.ArgumentParser(description="Nightly proxy list tester")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config.json"),
        help="Path to .json config file (merged over defaults). Default: ./config.json",
    )
    args = parser.parse_args()
    asyncio.run(_amain(args.verbose, args.config))


if __name__ == "__main__":
    main()
