from __future__ import annotations

import urllib.parse
from pathlib import Path

from .db import Database


def strip_existing_comment(link: str) -> str:
    return link.split("#", 1)[0].strip()


def format_comment(
    *,
    exit_ip: str | None,
    city: str | None,
    country: str | None,
    latency_ms: float | None,
    mbps: float | None,
) -> str:
    parts: list[str] = []
    if exit_ip:
        parts.append(f"IP={exit_ip}")
    geo = ",".join([x for x in [city, country] if x])
    if geo:
        parts.append(f"Geo={geo}")
    if latency_ms is not None:
        parts.append(f"URL={latency_ms:.0f}ms")
    if mbps is not None:
        parts.append(f"Speed={mbps:.2f}MB/s")
    return urllib.parse.quote("|".join(parts))


def render_link_with_comment(raw_link: str, comment: str) -> str:
    clean = strip_existing_comment(raw_link)
    if not comment:
        return clean
    return f"{clean}#{comment}"


def write_export(path: Path, db: Database) -> None:
    lines: list[str] = []
    for item in db.get_recent_selected():
        comment = format_comment(
            exit_ip=item["exit_ip"],
            city=item["city"],
            country=item["country"],
            latency_ms=item["latency_ms"],
            mbps=item["mbps"],
        )
        lines.append(render_link_with_comment(item["raw_link"], comment))

    path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
