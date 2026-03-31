from __future__ import annotations

import hashlib
import logging

from tqdm import tqdm

from .models import CandidateProxy
from .xray_backend import XrayToolchain, fetch_subscription_links

LOGGER = logging.getLogger(__name__)


def normalize_link(link: str) -> str:
    """Normalize a raw proxy link before parsing/hash."""

    return link.strip()


def hash_link(link: str) -> str:
    """Create stable SHA256 identifier for a proxy link."""

    normalized = normalize_link(link)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def collect_candidates(source_urls: list[str], toolchain: XrayToolchain) -> list[CandidateProxy]:
    """Fetch and parse candidates from all configured subscriptions."""

    out: list[CandidateProxy] = []
    seen: set[str] = set()

    for url in tqdm(source_urls, desc="Fetch subscriptions", unit="source"):
        LOGGER.info("Fetching subscription: %s", url)
        try:
            links = fetch_subscription_links(url, timeout=10)
        except Exception:
            LOGGER.exception("Failed to fetch subscription: %s", url)
            continue

        LOGGER.debug("Fetched %s links from %s", len(links), url)
        if not links:
            continue

        try:
            parsed_configs = toolchain.convert_links(links, start_port=10808)
        except Exception:
            LOGGER.exception("Failed to parse links with ProxyConverter: %s", url)
            continue

        for link in links:
            clean = normalize_link(link)
            if parsed_configs.get(clean) is None:
                continue

            digest = hash_link(clean)
            if digest in seen:
                continue

            scheme = clean.split("://", 1)[0].lower() if "://" in clean else "unknown"
            seen.add(digest)
            out.append(CandidateProxy(proxy_hash=digest, raw_link=clean, scheme=scheme))

    LOGGER.info("Collected deduplicated candidates: %s", len(out))
    return out
