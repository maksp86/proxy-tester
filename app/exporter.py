from __future__ import annotations

import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import flag

from .config import ExportConfig
from .db import Database


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


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
        parts.append(f"Geo={geo} {flag.flag(country) if country else ""}")
    if latency_ms is not None:
        parts.append(f"URL={latency_ms:.0f}ms")
    if mbps is not None:
        parts.append(f"Speed={mbps:.2f}MB/s")
    return urllib.parse.quote(" | ".join(parts))


def render_link_with_comment(raw_link: str, comment: str) -> str:
    clean = strip_existing_comment(raw_link)
    if not comment:
        return clean
    return f"{clean}#{comment}"


def write_export(config: ExportConfig, db: Database, info: dict[str, Any] | None = None) -> None:
    lines: list[str] = []
    for item in db.get_selected():
        comment = format_comment(
            exit_ip=item["exit_ip"],
            city=item["city"],
            country=item["country"],
            latency_ms=item["latency_ms"],
            mbps=item["mbps"],
        )
        lines.append(render_link_with_comment(item["raw_link"], comment))

    export = f"#profile-title: {config.title}\n"
    export += f"#profile-update-interval: {config.update_interval}\n"
    
    if config.web_page_url:
        export += f"#profile-web-page-url: {config.web_page_url}\n"

    export += f"# Exported at {utc_now().isoformat()}\n"
    export += f"# Count: {len(lines)}\n"

    if info is not None:
        export += "# Stats: \n"

        hours = int(info.get("elapsed_time", 0) // 3600)
        minutes = int((info.get("elapsed_time", 0) % 3600) // 60)
        seconds = int(info.get("elapsed_time", 0) % 60)
        export += f"# Time elapsed: {hours:02d}:{minutes:02d}:{seconds:02d}\n"

        export += f"# Candidates processed: {info.get("candidates")}\n"

    export += "\n".join(lines) + ("\n" if lines else "")
    config.file.write_text(export, encoding="utf-8")
