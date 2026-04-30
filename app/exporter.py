from __future__ import annotations

import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import base64

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

def _make_headers(config: ExportConfig, count: int, info: dict[str, Any] | None = None) -> dict[str, Any]:
    headers: dict[str, Any] = dict()
    headers["profile-title"] = "base64:" + \
        base64.b64encode(config.title.encode()).decode("ascii")
    headers["profile-update-interval"] = config.update_interval

    if config.web_page_url:
        headers["profile-web-page-url"] = config.web_page_url

    if config.support_url:
        headers["support-url"] = config.support_url

    announce_text = f"Exported at {datetime.now().strftime('%H:%M:%S %d.%m.%Y')}\n"
    announce_text += f"Count: {count}\n"
    
    if info is not None:
        announce_text += f"Candidates processed: {info.get("candidates")}\n"

    headers["announce"] = "base64:" + \
        base64.b64encode(announce_text.encode()).decode("ascii")
    
    return headers

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

    export = "#" + "-" * 50 + "proxy-tester" + "-" * 50 + "\n"
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

    headers = _make_headers(config, len(lines), info)

    if not config.separate_headers_file:
        headers_text = ""
        for item in headers:
            headers_text += f"#{item}: {headers[item]}\n"
        export = headers_text + export
    else:
        headers_text = ""
        for item in headers:
            headers_text += f"add_header {item} \"{headers[item]}\";\n"
        config.separate_headers_file.write_text(headers_text, encoding="utf-8")

    config.file.write_text(export, encoding="utf-8")
