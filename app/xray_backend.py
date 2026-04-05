from __future__ import annotations

import base64
import datetime
import hashlib
import json
import logging
import platform
import re
import stat
import subprocess
import tempfile
import urllib.error
import urllib.request
import zipfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from .models import Subscripton

LOGGER = logging.getLogger(__name__)
TCP_MAX_PORT = 65_535
TCP_MIN_PORT = 1000


class XrayToolchain:
    """Manages ProxyConverter and Xray-core binaries for local tests."""

    def __init__(self, project_root: Path | None = None) -> None:
        self._project_root = project_root or Path.cwd()
        self._bin_dir = self._project_root / ".bin"
        xray_binary_name = "xray.exe" if _is_windows() else "xray"
        converter_binary_name = "ProxyConverter.exe" if _is_windows() else "ProxyConverter"
        self._xray_path = self._bin_dir / "xray" / xray_binary_name
        self._converter_path = self._bin_dir / "proxyconverter" / converter_binary_name

    @property
    def xray_path(self) -> Path:
        self.ensure_xray()
        return self._xray_path

    @property
    def converter_path(self) -> Path:
        self.ensure_converter()
        return self._converter_path

    def ensure_xray(self) -> None:
        if self._xray_path.exists():
            return

        assets = _github_release_assets("XTLS", "Xray-core")
        archive_name = _select_xray_asset(assets)
        download_url = assets[archive_name]

        target_dir = self._xray_path.parent
        target_dir.mkdir(parents=True, exist_ok=True)
        archive_path = target_dir / archive_name
        _download_file(download_url, archive_path)
        _extract_zip(archive_path, target_dir)
        archive_path.unlink(missing_ok=True)

        if not self._xray_path.exists():
            raise RuntimeError(
                f"Xray binary not found after extraction: {self._xray_path}")
        _mark_executable(self._xray_path)

    def ensure_converter(self) -> None:
        if self._converter_path.exists():
            return

        assets = _github_release_assets("maksp86", "ProxyConverter")
        binary_name = _select_converter_asset(assets)
        download_url = assets[binary_name]

        target_dir = self._converter_path.parent
        target_dir.mkdir(parents=True, exist_ok=True)
        _download_file(download_url, self._converter_path)
        _mark_executable(self._converter_path)

    def convert_links(self, links: list[str], start_port: int = 10808) -> dict[str, dict[str, Any] | None]:
        if not links:
            return {}

        self.ensure_converter()
        merged: dict[str, dict[str, Any] | None] = {}
        link_batches = list(_port_limited_chunks(links, start_port))
        if len(link_batches) > 1:
            LOGGER.info(
                "Converting links in %s batches due to TCP port range: links=%s start_port=%s",
                len(link_batches),
                len(links),
                start_port,
            )

        for batch in link_batches:
            batch_data = self._convert_links_batch(
                batch, start_port=start_port)
            merged.update(batch_data)
        return merged

    def _convert_links_batch(
        self,
        links: list[str],
        start_port: int,
    ) -> dict[str, dict[str, Any] | None]:
        with tempfile.TemporaryDirectory(prefix="proxyconverter-") as tmpdir:
            tmp = Path(tmpdir)
            input_path = tmp / "links.json"
            input_path.write_text(json.dumps(
                links, ensure_ascii=False), encoding="utf-8")

            cmd = [
                str(self._converter_path),
                "--input-json",
                str(input_path),
                "--change-ports",
                "--start-port",
                str(start_port),
            ]
            process = subprocess.run(
                cmd, capture_output=True, text=True, check=False)
            if process.returncode != 0:
                raise RuntimeError(
                    "ProxyConverter failed with code "
                    f"{process.returncode}: {process.stderr.strip() or process.stdout.strip()}"
                )

            payload = process.stdout.strip()
            if not payload:
                raise RuntimeError("ProxyConverter returned empty output")

            data = json.loads(payload)
            if not isinstance(data, dict):
                raise RuntimeError(
                    "ProxyConverter output has unexpected format")
            return data


def fetch_subscription_links(url: str, timeout: float = 10.0) -> tuple[list[str], Subscripton]:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "proxy-tester/1.0",
            "Accept": "text/plain,application/json,*/*",
        },
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        raw = response.read()

    subsctiption_hash = hashlib.sha256(raw).hexdigest()

    subscripton = Subscripton(url,
                              subsctiption_hash)
    content = raw.decode("utf-8", errors="ignore")
    lines = _split_links(content)
    if lines:
        return lines, subscripton

    decoded = _try_decode_base64(content)
    if decoded:
        lines = _split_links(decoded)
    return lines, subscripton


def _split_links(payload: str) -> list[str]:
    out: list[str] = []
    for item in payload.splitlines():
        clean = item.strip()
        if not clean:
            continue
        if "://" not in clean:
            continue
        out.append(clean)
    return out


def _try_decode_base64(value: str) -> str | None:
    compact = "".join(value.split())
    if not compact:
        return None

    padded = compact + "=" * ((4 - len(compact) % 4) % 4)
    for decode in (base64.b64decode, base64.urlsafe_b64decode):
        try:
            return decode(padded).decode("utf-8", errors="ignore")
        except Exception:
            continue
    return None


def _github_release_assets(owner: str, repo: str) -> dict[str, str]:
    api_url = f"https://api.github.com/repos/{owner}/{repo}/releases/latest"
    request = urllib.request.Request(
        api_url, headers={"Accept": "application/vnd.github+json"})
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            release = json.loads(
                response.read().decode("utf-8", errors="ignore"))
    except urllib.error.URLError as exc:
        raise RuntimeError(
            f"Unable to fetch latest release for {owner}/{repo}: {exc}") from exc

    assets = release.get("assets") or []
    mapping: dict[str, str] = {}
    for asset in assets:
        name = asset.get("name")
        url = asset.get("browser_download_url")
        if name and url:
            mapping[name] = url
    if not mapping:
        raise RuntimeError(
            f"No downloadable assets found for {owner}/{repo} latest release")
    return mapping


def _select_xray_asset(assets: dict[str, str]) -> str:
    system = platform.system().lower()
    machine = platform.machine().lower()
    if system == "windows":
        if machine in {"x86_64", "amd64"}:
            wanted = "Xray-windows-64.zip"
        elif machine in {"aarch64", "arm64"}:
            wanted = "Xray-windows-arm64-v8a.zip"
        else:
            raise RuntimeError(
                f"Unsupported architecture for Xray-core on Windows: {machine}")
    elif system in {"linux", "darwin"}:
        if machine in {"x86_64", "amd64"}:
            wanted = "Xray-linux-64.zip"
        elif machine in {"aarch64", "arm64"}:
            wanted = "Xray-linux-arm64-v8a.zip"
        else:
            raise RuntimeError(
                f"Unsupported architecture for Xray-core: {machine}")
    else:
        raise RuntimeError(
            f"Unsupported operating system for Xray-core: {system}")

    if wanted not in assets:
        raise RuntimeError(f"Xray asset {wanted} not found in latest release")
    return wanted


def _select_converter_asset(assets: dict[str, str]) -> str:
    system = platform.system().lower()
    machine = platform.machine().lower()
    if system == "windows":
        if machine in {"x86_64", "amd64"}:
            wanted = _select_asset_by_pattern(
                assets, r"^ProxyConverter-win-x64(?:\.exe)?$")
        elif machine in {"aarch64", "arm64"}:
            wanted = _select_asset_by_pattern(
                assets, r"^ProxyConverter-win-arm64(?:\.exe)?$")
        else:
            raise RuntimeError(
                f"Unsupported architecture for ProxyConverter on Windows: {machine}")
        if wanted:
            return wanted
        raise RuntimeError(
            "ProxyConverter Windows asset not found in latest release")

    if machine in {"x86_64", "amd64"}:
        wanted = "ProxyConverter-linux-x64"
    elif machine in {"aarch64", "arm64"}:
        wanted = "ProxyConverter-linux-arm64"
    else:
        raise RuntimeError(
            f"Unsupported architecture for ProxyConverter: {machine}")

    if wanted not in assets:
        raise RuntimeError(
            f"ProxyConverter asset {wanted} not found in latest release")
    return wanted


def _select_asset_by_pattern(assets: dict[str, str], pattern: str) -> str | None:
    compiled = re.compile(pattern, flags=re.IGNORECASE)
    for asset_name in assets:
        if compiled.match(asset_name):
            return asset_name
    return None


def _port_limited_chunks(links: list[str], start_port: int) -> Iterator[list[str]]:
    if start_port < TCP_MIN_PORT or start_port > TCP_MAX_PORT:
        raise RuntimeError(
            f"start_port={start_port} is outside valid range {TCP_MIN_PORT}-{TCP_MAX_PORT}")

    available_ports = TCP_MAX_PORT - start_port + 1
    for index in range(0, len(links), available_ports):
        yield links[index:index + available_ports]


def _download_file(url: str, destination: Path) -> None:
    LOGGER.info("Downloading %s -> %s", url, destination)
    with urllib.request.urlopen(url, timeout=60) as response:
        destination.write_bytes(response.read())


def _extract_zip(archive_path: Path, destination: Path) -> None:
    with zipfile.ZipFile(archive_path, "r") as zf:
        zf.extractall(destination)


def _mark_executable(path: Path) -> None:
    if _is_windows():
        return
    mode = path.stat().st_mode
    path.chmod(mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def _is_windows() -> bool:
    return platform.system().lower() == "windows"
