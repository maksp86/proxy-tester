from __future__ import annotations

import hashlib
import logging

from tqdm import tqdm

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

    normalized = normalize_link(link)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def collect_candidates(source_urls: list[str], db: Database, toolchain: XrayToolchain) -> list[CandidateProxy]:
    """Fetch and parse candidates from all configured subscriptions."""

    out: list[CandidateProxy] = []
    seen: set[str] = set()

    for url in tqdm(source_urls, desc="Fetch subscriptions", unit="source"):
        LOGGER.info("Fetching subscription: %s", url)
        try:
            links, subscription = fetch_subscription_links(url, timeout=10)
        except Exception:
            LOGGER.exception("Failed to fetch subscription: %s", url)
            continue

        db_subscription = db.get_subscription(url)
        if db_subscription is not None and db_subscription.last_data_hash == subscription.last_data_hash:
            LOGGER.debug("Subscription %s did not changed since last time, skipping", url)
            continue
        db.upsert_subscription(subscription)

        LOGGER.debug("Fetched %s links from %s", len(links), url)
        if not links:
            continue

        try:
            parsed_configs = toolchain.convert_links(links, start_port=1000)
        except Exception:
            LOGGER.exception(
                "Failed to parse links with ProxyConverter: %s", url)
            continue

        for link in links:
            clean = normalize_link(link)
            if not clean:
                continue
            if parsed_configs.get(clean) is None:
                continue

            digest = hash_link(clean)
            if digest in seen:
                continue

            scheme = clean.split(
                "://", 1)[0].lower() if "://" in clean else "unknown"
            seen.add(digest)
            out.append(CandidateProxy(proxy_hash=digest,
                       raw_link=clean, scheme=scheme))

    LOGGER.info("Collected deduplicated candidates: %s", len(out))
    return out
