import asyncio
import re
from typing import Any


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

    xhttp = stream.get("xhttpSettings") or stream.get(
        "splithttpSettings") or {}
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


class StopController:
    def __init__(self, target_success: int):
        self.target = target_success
        self.success = 0
        self.lock = asyncio.Lock()
        self.stop_event = asyncio.Event()

    async def add_success(self):
        async with self.lock:
            self.success += 1
            if self.success >= self.target:
                self.stop_event.set()

    def should_stop(self) -> bool:
        return self.stop_event.is_set()
