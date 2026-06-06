import asyncio
import bisect
import ipaddress
import logging
import re
from pathlib import Path
from typing import Any, List, Tuple

LOGGER = logging.getLogger(__name__)


def find_key_nonrecursive(adict, key):
    stack = [adict]
    while stack:
        d = stack.pop()
        if key in d:
            return d[key]
        for _, v in d.items():
            if isinstance(v, dict):
                stack.append(v)
            elif isinstance(v, list):
                for x in v:
                    if isinstance(x, dict):
                        stack.append(x)
    return None


def is_safe_xhttp_config(outbound: dict[str, Any]) -> bool:
    stream = outbound.get("streamSettings", {})
    network = stream.get("network")

    if network not in ("xhttp", "splithttp", "httpupgrade"):
        return True

    xhttp = stream.get("xhttpSettings") or stream.get("splithttpSettings") or {}
    tls = stream.get("tlsSettings") or {}
    reality = stream.get("realitySettings") or {}

    server_name = tls.get("serverName", "") or reality.get("serverName", "")
    host = xhttp.get("host", "")
    path = xhttp.get("path", "")

    non_ascii = re.compile(r"[^\x00-\x7F]")
    if (
        non_ascii.search(server_name)
        or non_ascii.search(host)
        or non_ascii.search(path)
    ):
        return False

    if "@" in server_name or "@" in host or "@" in path:
        return False

    if len(server_name) > 250 or len(host) > 250 or len(path) > 250:
        return False

    return True


def is_port_excluded(excluded_ports: List[Tuple[int, int]], port: int) -> bool:
    if port < 1 or port > 65535:
        raise ValueError("Port must be in range 1..65535")

    return any(start <= port <= end for start, end in excluded_ports)


def extract_address(outbound: dict[str, Any]) -> tuple[str, int] | tuple[None, None]:
    try:
        settings = outbound.get("settings", {})
        if "vnext" in settings and settings["vnext"]:
            server = settings["vnext"][0]
            return str(server["address"]), int(server["port"])

        if "servers" in settings and settings["servers"]:
            server = settings["servers"][0]
            return str(server["address"]), int(server["port"])

        if "address" in settings and "port" in settings:
            return str(settings["address"]), int(settings["port"])

        if "address" in outbound and "port" in outbound:
            return str(outbound["address"]), int(outbound["port"])
    except (KeyError, ValueError, TypeError) as e:
        LOGGER.warning(f"Failed to parse address from outbound config: {e}")
    return None, None


class StopController:
    def __init__(self, target_success: int):
        self.target = target_success
        self.success = 0
        self.lock = asyncio.Lock()
        self.stop_event = asyncio.Event()

    async def add_success(self, count: int = 1):
        async with self.lock:
            self.success += count
            if self.success >= self.target:
                self.stop_event.set()

    def should_stop(self) -> bool:
        return self.stop_event.is_set()


class FastNetworkSearch:
    def __init__(self):
        self._ranges_v4: List[Tuple[int, int]] = []
        self._ranges_v6: List[Tuple[int, int]] = []

    def ensure_networks(self, path: Path):
        raw_v4 = []
        raw_v6 = []
        with path.open("r", encoding="utf-8") as f:
            for line_no, raw_line in enumerate(f, start=1):
                line = raw_line.strip()
                if not line or line.startswith("#"):
                    continue
                try:
                    net_obj = ipaddress.ip_network(line)
                    start_int = int(net_obj.network_address)
                    end_int = int(net_obj.broadcast_address)
                    if net_obj.version == 4:
                        raw_v4.append((start_int, end_int))
                    else:
                        raw_v6.append((start_int, end_int))
                except ValueError as exc:
                    LOGGER.warning(
                        "Skipping invalid CIDR on line %d in %s: %s",
                        line_no,
                        path,
                        exc,
                    )

        self._ranges_v4 = self._merge_ranges(raw_v4)
        self._ranges_v6 = self._merge_ranges(raw_v6)

    def _merge_ranges(self, ranges: List[Tuple[int, int]]) -> List[Tuple[int, int]]:
        if not ranges:
            return []

        ranges.sort(key=lambda x: (x[0], -x[1]))

        merged = []
        curr_start, curr_end = ranges[0]
        for start, end in ranges[1:]:
            if start <= curr_end:
                if end > curr_end:
                    curr_end = end
            else:
                merged.append((curr_start, curr_end))
                curr_start, curr_end = start, end
        merged.append((curr_start, curr_end))
        return merged

    def find_network(self, ip_address: str) -> bool:
        if not self._ranges_v4 and not self._ranges_v6:
            raise IndexError("Networks is empty")

        addr_obj = ipaddress.ip_address(ip_address)
        ip_int = int(addr_obj)
        ranges = self._ranges_v4 if addr_obj.version == 4 else self._ranges_v6

        if not ranges:
            return False

        idx = bisect.bisect_right(ranges, (ip_int, float("inf")))

        if idx > 0:
            start_int, end_int = ranges[idx - 1]
            if start_int <= ip_int <= end_int:
                return True
        return False
