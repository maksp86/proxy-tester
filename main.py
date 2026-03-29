from __future__ import annotations

import argparse
import asyncio
import logging
import urllib.request

from app.config import DEFAULT_CONFIG
from app.db import Database
from app.pipeline import run_once
from app.probe import ProxyProbe


def setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")


def ensure_geoip_database(geoip_db_path, geoip_db_url: str | None) -> None:
    if not geoip_db_url:
        return
    geoip_db_path.parent.mkdir(parents=True, exist_ok=True)
    urllib.request.urlretrieve(geoip_db_url, geoip_db_path)


async def _amain(verbose: bool) -> None:
    setup_logging(verbose)
    cfg = DEFAULT_CONFIG
    ensure_geoip_database(cfg.geoip_db_path, cfg.geoip_db_url)

    db = Database(cfg.db_path)
    probe = ProxyProbe(geoip_db_path=cfg.geoip_db_path)
    selected = await run_once(cfg, db, probe)
    logging.info("Done. Selected %s proxies.", len(selected))


def main() -> None:
    parser = argparse.ArgumentParser(description="Nightly proxy list tester")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    args = parser.parse_args()
    asyncio.run(_amain(args.verbose))


if __name__ == "__main__":
    main()
