from __future__ import annotations

import hashlib
import logging

from .db import Database
from .xray_backend import XrayToolchain, fetch_subscription_links

LOGGER = logging.getLogger(__name__)


def normalize_link(link: str) -> str:
    clean = link.strip()
    if not clean:
        return clean
    return clean.split("#", 1)[0].strip()


def hash_link(link: str) -> str:
    return hashlib.sha256(link.encode("utf-8")).hexdigest()


async def ingest_subscriptions(
    source_urls: list[str],
    db: Database,
    toolchain: XrayToolchain,
) -> int:
    """Fetch subscriptions and upsert live candidates after each source."""

    total_upserted = 0
    for url in source_urls:
        upserted = await _process_source(url, db, toolchain)
        total_upserted += upserted

    LOGGER.info("Total candidates upserted from subscriptions: %s", total_upserted)
    return total_upserted


async def _process_source(url: str, db: Database, toolchain: XrayToolchain) -> int:
    LOGGER.info("Fetching subscription: %s", url)

    try:
        links, subscription = await fetch_subscription_links(url, timeout=10)
    except Exception:
        LOGGER.exception("Failed to fetch subscription: %s", url)
        return 0

    db_subscription = db.get_subscription(url)
    if (
        db_subscription is not None
        and db_subscription.last_data_hash == subscription.last_data_hash
    ):
        LOGGER.debug("Subscription %s did not change since last time, skipping", url)
        return 0

    db.upsert_subscription(subscription)

    if not links:
        return 0

    cleaned: list[str] = []
    seen_links: set[str] = set()
    for link in links:
        clean = normalize_link(link)
        if not clean or clean in seen_links:
            continue
        seen_links.add(clean)
        cleaned.append(clean)

    if not cleaned:
        return 0

    try:
        parsed_configs = await toolchain.convert_links(cleaned)
    except Exception:
        LOGGER.exception("Failed to parse links with ProxyConverter: %s", url)
        return 0

    upsert_rows: list[tuple[str, str, str]] = []
    for link in cleaned:
        parsed_config = parsed_configs.get(link)
        if parsed_config is None:
            continue

        outbounds = parsed_config.get("outbounds") or []
        if not outbounds:
            continue

        protocol = (outbounds[0] or {}).get("protocol")
        if not protocol:
            continue

        upsert_rows.append((hash_link(link), link, str(protocol)))

    db.upsert_proxies(upsert_rows)
    LOGGER.info("Subscription %s upserted candidates: %s", url, len(upsert_rows))
    return len(upsert_rows)
