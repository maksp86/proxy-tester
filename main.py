from __future__ import annotations

import argparse
import asyncio
import logging

from app.config import DEFAULT_CONFIG
from app.db import Database
from app.pipeline import run_once
from app.probe import ProxyProbe


def setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")


async def _amain(verbose: bool) -> None:
    setup_logging(verbose)
    cfg = DEFAULT_CONFIG
    db = Database(cfg.db_path)
    probe = ProxyProbe()
    selected = await run_once(cfg, db, probe)
    logging.info("Done. Selected %s proxies.", len(selected))


def main() -> None:
    parser = argparse.ArgumentParser(description="Nightly proxy list tester")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    args = parser.parse_args()
    asyncio.run(_amain(args.verbose))


if __name__ == "__main__":
    main()
