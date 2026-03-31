from __future__ import annotations

import base64
import json
import logging
import platform
import stat
import subprocess
import tempfile
import urllib.error
import urllib.request
import zipfile
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger(__name__)


class XrayToolchain:
    """Manages ProxyConverter and Xray-core binaries for local tests."""

    def __init__(self, project_root: Path | None = None) -> None:
        self._project_root = project_root or Path.cwd()
        self._bin_dir = self._project_root / ".bin"
        self._xray_path = self._bin_dir / "xray" / "xray"
        self._converter_path = self._bin_dir / "proxyconverter" / "ProxyConverter"

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
            raise RuntimeError(f"Xray binary not found after extraction: {self._xray_path}")
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

        with tempfile.TemporaryDirectory(prefix="proxyconverter-") as tmpdir:
            tmp = Path(tmpdir)
            input_path = tmp / "links.json"
            input_path.write_text(json.dumps(links, ensure_ascii=False), encoding="utf-8")

            cmd = [
                str(self._converter_path),
                "--input-json",
                str(input_path),
                "--change-ports",
                "--start-port",
                str(start_port),
            ]
            process = subprocess.run(cmd, capture_output=True, text=True, check=False)
            if process.returncode != 0:
                raise RuntimeError(
                    f"ProxyConverter failed with code {process.returncode}: {process.stderr.strip() or process.stdout.strip()}"
                )

            payload = process.stdout.strip()
            if not payload:
                raise RuntimeError("ProxyConverter returned empty output")

            data = json.loads(payload)
            if not isinstance(data, dict):
                raise RuntimeError("ProxyConverter output has unexpected format")
            return data


def fetch_subscription_links(url: str, timeout: float = 10.0) -> list[str]:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "proxy-tester/1.0",
            "Accept": "text/plain,application/json,*/*",
        },
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        raw = response.read().decode("utf-8", errors="ignore")

    lines = _split_links(raw)
    if lines:
        return lines

    decoded = _try_decode_base64(raw)
    if decoded:
        lines = _split_links(decoded)
    return lines


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
    request = urllib.request.Request(api_url, headers={"Accept": "application/vnd.github+json"})
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            release = json.loads(response.read().decode("utf-8", errors="ignore"))
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Unable to fetch latest release for {owner}/{repo}: {exc}") from exc

    assets = release.get("assets") or []
    mapping: dict[str, str] = {}
    for asset in assets:
        name = asset.get("name")
        url = asset.get("browser_download_url")
        if name and url:
            mapping[name] = url
    if not mapping:
        raise RuntimeError(f"No downloadable assets found for {owner}/{repo} latest release")
    return mapping


def _select_xray_asset(assets: dict[str, str]) -> str:
    machine = platform.machine().lower()
    if machine in {"x86_64", "amd64"}:
        wanted = "Xray-linux-64.zip"
    elif machine in {"aarch64", "arm64"}:
        wanted = "Xray-linux-arm64-v8a.zip"
    else:
        raise RuntimeError(f"Unsupported architecture for Xray-core: {machine}")

    if wanted not in assets:
        raise RuntimeError(f"Xray asset {wanted} not found in latest release")
    return wanted


def _select_converter_asset(assets: dict[str, str]) -> str:
    machine = platform.machine().lower()
    if machine in {"x86_64", "amd64"}:
        wanted = "ProxyConverter-linux-x64"
    elif machine in {"aarch64", "arm64"}:
        wanted = "ProxyConverter-linux-arm64"
    else:
        raise RuntimeError(f"Unsupported architecture for ProxyConverter: {machine}")

    if wanted not in assets:
        raise RuntimeError(f"ProxyConverter asset {wanted} not found in latest release")
    return wanted


def _download_file(url: str, destination: Path) -> None:
    LOGGER.info("Downloading %s -> %s", url, destination)
    with urllib.request.urlopen(url, timeout=60) as response:
        destination.write_bytes(response.read())


def _extract_zip(archive_path: Path, destination: Path) -> None:
    with zipfile.ZipFile(archive_path, "r") as zf:
        zf.extractall(destination)


def _mark_executable(path: Path) -> None:
    mode = path.stat().st_mode
    path.chmod(mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
