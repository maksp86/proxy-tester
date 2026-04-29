import asyncio
import hashlib
import logging

import tqdm.asyncio
from pydantic import HttpUrl

from .config import AppConfig
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


async def fetch_candidates(
    config: AppConfig, db: Database, toolchain: XrayToolchain
) -> int:
    """Обновляет таблицу proxies свежими прокси из подписок (только живые)."""
    source_urls = list(config.subscription_urls)
    LOGGER.info("Loaded %s source URLs from config", len(source_urls))

    added = await collect_candidates(source_urls, db, toolchain)

    LOGGER.info("Added fresh alive candidates from subscriptions: %s", added)
    return added


async def collect_candidates(
    source_urls: list[HttpUrl], db: Database, toolchain: XrayToolchain
) -> int:
    """Собирает прокси из всех подписок → temp-таблица → merge в proxies.
    Возвращает только количество добавленных/обновлённых живых прокси.
    """
    semaphore = asyncio.Semaphore(5)

    db.prepare_fresh_candidates_table()

    batches = await tqdm.asyncio.tqdm.gather(
        *(_process_source(str(url), db, toolchain, semaphore)
          for url in source_urls),
        desc="Fetch subscriptions",
        unit="source",
        mininterval=1
    )

    total_links = 0
    total_parsed = 0

    for batch_data, batch_total in batches:
        total_links += batch_total
        if not batch_data:
            continue
        total_parsed += len(batch_data)
        db.insert_fresh_candidates_batch(batch_data)

    num_fresh = db.count_fresh_candidates()

    LOGGER.info(
        "Collected unique fresh candidates: %s (parsed: %s, total links: %s)",
        num_fresh,
        total_parsed,
        total_links,
    )

    if num_fresh == 0:
        db.cleanup_fresh_candidates_table()
        return 0

    added = db.merge_fresh_candidates_to_proxies()

    LOGGER.info(
        "Added fresh alive candidates: %s (skipped dead=%s)",
        added,
        num_fresh - added,
    )

    db.cleanup_fresh_candidates_table()
    return added


async def _process_source(
    url: str, db: Database, toolchain: XrayToolchain, semaphore: asyncio.Semaphore
) -> tuple[list[CandidateProxy], int]:
    async with semaphore:
        LOGGER.debug("Fetching subscription: %s", url)

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
            LOGGER.exception(
                "Failed to parse links with ProxyConverter: %s", url)
            return [], total_links

        candidates: list[CandidateProxy] = []
        for link in clean_links:
            parsed_outbound = parsed_configs.get(link)
            if parsed_outbound is None:
                continue

            digest = hash_link(link)
            scheme = parsed_outbound["protocol"]
            candidates.append(
                CandidateProxy(
                    proxy_hash=digest,
                    raw_link=link,
                    scheme=scheme,
                )
            )

        return candidates, total_links
