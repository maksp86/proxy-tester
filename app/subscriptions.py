from __future__ import annotations

import base64
import hashlib
import urllib.request
from pathlib import Path
from urllib.parse import urlparse

from .models import CandidateProxy

SUPPORTED_SCHEMES = {"vmess", "vless", "trojan", "hysteria", "hysteria2", "ss", "socks"}


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


def _download_text(url: str, timeout: float = 10.0) -> str:
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="replace")


def _decode_subscription_blob(blob: str) -> str:
    compact = "".join(blob.strip().split())
    try:
        padded = compact + "=" * (-len(compact) % 4)
        decoded = base64.urlsafe_b64decode(padded)
        text = decoded.decode("utf-8")
    except Exception:
        return blob
    if any(part in text for part in ("://", "\n")):
        return text
    return blob


def _strip_comment(link: str) -> str:
    return link.split("#", 1)[0].strip()


def normalize_link(link: str) -> str:
    return _strip_comment(link)


def hash_link(link: str) -> str:
    normalized = normalize_link(link)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def parse_candidates_from_text(text: str) -> list[str]:
    decoded = _decode_subscription_blob(text)
    return [line.strip() for line in decoded.splitlines() if line.strip()]


def collect_candidates(source_urls: list[str]) -> list[CandidateProxy]:
    out: list[CandidateProxy] = []
    seen: set[str] = set()

    for url in source_urls:
        try:
            body = _download_text(url)
        except Exception:
            continue

        for link in parse_candidates_from_text(body):
            clean = normalize_link(link)
            parsed = urlparse(clean)
            scheme = parsed.scheme.lower()
            if scheme not in SUPPORTED_SCHEMES:
                continue
            digest = hash_link(clean)
            if digest in seen:
                continue
            seen.add(digest)
            out.append(CandidateProxy(proxy_hash=digest, raw_link=clean, scheme=scheme))
    return out
