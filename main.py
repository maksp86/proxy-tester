from __future__ import annotations

import argparse
import asyncio
import logging
import urllib.request
from pathlib import Path

import colorlog

from app.binary_toolchain import BinaryToolchain
from app.config import AppConfig
from app.db import Database
from app.pipeline import run_once


def setup_logging(verbose: bool) -> None:
    """Configure global logger.

    Args:
        verbose: If `True`, enables DEBUG logs; INFO otherwise.
    """

    level = logging.DEBUG if verbose else logging.INFO

    handler = colorlog.StreamHandler()
    handler.setFormatter(
        colorlog.ColoredFormatter(
            "%(log_color)s%(asctime)s %(levelname)s [%(name)s] %(message)s",
            log_colors={
                "DEBUG": "white",
                "INFO": "green",
                "WARNING": "yellow",
                "ERROR": "red",
                "CRITICAL": "red,bg_white",
            },
        )
    )

    logging.basicConfig(
        level=level,
        handlers=[handler],
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

    if cfg.fetch_proxy:
        proxy = urllib.request.ProxyHandler(
            {
                "http": cfg.fetch_proxy.encoded_string(),
                "https": cfg.fetch_proxy.encoded_string(),
            }
        )
        opener = urllib.request.build_opener(proxy)
        urllib.request.install_opener(opener)

    db = Database(cfg.db_path)
    toolchain = BinaryToolchain()

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
