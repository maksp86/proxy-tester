from __future__ import annotations

import argparse
import asyncio
import logging
from pathlib import Path

from app.config import AppConfig
from app.db import Database
from app.pipeline import run_once
from app.xray_backend import XrayToolchain


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


async def _amain(verbose: bool, config_path: Path) -> None:
    """Entrypoint for async pipeline execution.

    Args:
        verbose: Enables verbose logs when `True`.
        config_path: Optional path to JSON config file.
    """

    setup_logging(verbose)
    cfg = AppConfig.from_json_file(config_path)

    db = Database(cfg.db_path)
    toolchain = XrayToolchain()

    logging.debug("Ensuring toolchain exists..")
    toolchain.ensure_converter()
    toolchain.ensure_xray()
    logging.debug("Ensuring toolchain exists.. Done")

    await run_once(cfg, db, toolchain)


def main() -> None:
    """CLI wrapper."""

    parser = argparse.ArgumentParser(description="Proxy subscriptions tester")
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
