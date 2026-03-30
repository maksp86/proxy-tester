from __future__ import annotations

import hashlib
import logging

from tqdm import tqdm

from python_v2ray.config_parser import fetch_from_subscription, parse_uri

from .models import CandidateProxy

LOGGER = logging.getLogger(__name__)


def normalize_link(link: str) -> str:
    """Normalize a raw proxy link before parsing/hash."""

    return link.strip()


def hash_link(link: str) -> str:
    """Create stable SHA256 identifier for a proxy link."""

    normalized = normalize_link(link)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def collect_candidates(source_urls: list[str]) -> list[CandidateProxy]:
    """Fetch and parse candidates from all configured subscriptions.

    Args:
        source_urls: Subscription endpoints.

    Returns:
        Deduplicated list of valid proxy candidates.
    """

    out: list[CandidateProxy] = []
    seen: set[str] = set()

    for url in tqdm(source_urls, desc="Fetch subscriptions", unit="source"):
        LOGGER.info("Fetching subscription: %s", url)
        try:
            links = fetch_from_subscription(url, timeout=10)
        except Exception:
            LOGGER.exception("Failed to fetch subscription: %s", url)
            continue

        LOGGER.debug("Fetched %s links from %s", len(links), url)
        for link in links:
            clean = normalize_link(link)
            parsed = parse_uri(clean)
            if parsed is None:
                continue

            digest = hash_link(clean)
            if digest in seen:
                continue

            seen.add(digest)
            out.append(
                CandidateProxy(
                    proxy_hash=digest,
                    raw_link=clean,
                    scheme=parsed.protocol,
                )
            )

    LOGGER.info("Collected deduplicated candidates: %s", len(out))
    return out
