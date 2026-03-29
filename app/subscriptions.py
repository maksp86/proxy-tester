from __future__ import annotations

import hashlib
from pathlib import Path

from python_v2ray.config_parser import fetch_from_subscription, parse_uri

from .models import CandidateProxy


def read_sources(sources_file: Path) -> list[str]:
    if not sources_file.exists():
        return []
    sources: list[str] = []
    for raw in sources_file.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        sources.append(line)
    return sources


def normalize_link(link: str) -> str:
    return link.strip()


def hash_link(link: str) -> str:
    normalized = normalize_link(link)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def collect_candidates(source_urls: list[str]) -> list[CandidateProxy]:
    out: list[CandidateProxy] = []
    seen: set[str] = set()

    for url in source_urls:
        links = fetch_from_subscription(url, timeout=10)
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
    return out
