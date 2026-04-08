import asyncio
import hashlib
import logging

import tqdm.asyncio

from .db import Database
from .models import CandidateProxy
from .xray_backend import XrayToolchain, fetch_subscription_links

LOGGER = logging.getLogger(__name__)


def normalize_link(link: str) -> str:
    """Normalize a raw proxy link before parsing/hash."""

    clean = link.strip()
    if not clean:
        return clean
    # Subscription lines often contain human-readable labels after #fragment.
    clean = clean.split("#", 1)[0].strip()
    return clean


def hash_link(link: str) -> str:
    """Create stable SHA256 identifier for a proxy link."""
    return hashlib.sha256(link.encode("utf-8")).hexdigest()


async def collect_candidates(
    source_urls: list[str],
    db: Database,
    toolchain: XrayToolchain,
) -> list[CandidateProxy]:
    """Fetch and parse candidates from all configured subscriptions."""

    semaphore = asyncio.Semaphore(5)
    seen: set[str] = set()

    batches = await tqdm.asyncio.tqdm.gather(
        *(_process_source(url, db, toolchain, semaphore) for url in source_urls),
        desc="Fetch subscriptions",
        unit="source",
    )

    out: list[CandidateProxy] = []
    total_links = 0

    for candidates, batch_total in batches:
        total_links += batch_total

        for candidate in candidates:
            if candidate.proxy_hash in seen:
                continue
            seen.add(candidate.proxy_hash)
            out.append(candidate)

    LOGGER.info(
        "Collected deduplicated candidates: %s, total: %s",
        len(out),
        total_links,
    )
    return out


async def _process_source(
    url: str, db: Database, toolchain: XrayToolchain, semaphore: asyncio.Semaphore
) -> tuple[list[CandidateProxy], int]:
    async with semaphore:
        LOGGER.info("Fetching subscription: %s", url)

        try:
            links, subscription = await fetch_subscription_links(url, timeout=10)
        except Exception:
            LOGGER.exception("Failed to fetch subscription: %s", url)
            return [], 0

        db_subscription = db.get_subscription(url)
        if (
            db_subscription is not None
            and db_subscription.last_data_hash == subscription.last_data_hash
        ):
            LOGGER.debug(
                "Subscription %s did not change since last time, skipping", url
            )
            return [], 0

        db.upsert_subscription(subscription)

        LOGGER.debug("Fetched %s links from %s", len(links), url)
        if not links:
            return [], 0

        clean_links: list[str] = []
        total_links = 0

        for link in links:
            clean = normalize_link(link)
            if not clean:
                continue
            total_links += 1
            clean_links.append(clean)

        if not clean_links:
            return [], total_links

        try:
            parsed_configs = await toolchain.convert_links(clean_links)
        except Exception:
            LOGGER.exception("Failed to parse links with ProxyConverter: %s", url)
            return [], total_links

        candidates: list[CandidateProxy] = []
        for link in clean_links:
            parsed_config = parsed_configs.get(link)
            if parsed_config is None:
                continue

            digest = hash_link(link)
            scheme = parsed_config["outbounds"][0]["protocol"]
            candidates.append(
                CandidateProxy(
                    proxy_hash=digest,
                    raw_link=link,
                    scheme=scheme,
                )
            )

        return candidates, total_links
