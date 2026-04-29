import asyncio
import copy
import json
import logging
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

from app.helpers import is_safe_xhttp_config
from app.xray_backend import XrayToolchain

LOGGER = logging.getLogger(__name__)

BASE_CONFIG: dict[str, Any] = {
    "log": {"loglevel": "warning"},
    "dns": {
        "hosts": {},
        "servers": [
            {
                "address": "223.5.5.5",
                "domains": ["full:dns.alidns.com", "full:cloudflare-dns.com"],
                "skipFallback": True,
            },
            "https://cloudflare-dns.com/dns-query",
        ],
        "tag": "dns-module",
    },
    "api": {
        "tag": "api",
        "services": ["HandlerService", "RoutingService"],
        "listen": "",
    },
    "routing": {"domainStrategy": "AsIs", "rules": []},
    "inbounds": [],
}

BASE_INBOUND: dict[str, Any] = {
    "tag": "",
    "port": 0,
    "listen": "127.0.0.1",
    "protocol": "http",
    "settings": {"allowTransparent": False},
}

BASE_RULE: dict[str, Any] = {
    "routing": {
        "domainStrategy": "AsIs",
        "rules": [
            {"ruleTag": "", "type": "field", "inboundTag": [], "outboundTag": ""}
        ],
    }
}

START_API_PORT = 10000
START_INBOUND_PORT = 20000


@dataclass
class XraySlot:
    worker_id: int
    inbound_tag: str
    inbound_port: int


class XrayInstance:
    def __init__(
        self, toolchain: XrayToolchain, worker_id: int, inbounds: list[tuple[str, int]]
    ) -> None:
        self._worker_id = worker_id
        self._api_port = START_API_PORT + worker_id
        self._xray_path = toolchain._xray_path
        self._inbounds = inbounds
        self._proc: asyncio.subprocess.Process | None = None
        self._lock = asyncio.Lock()
        self._prev_rules: deque[dict] = deque(maxlen=5)
        self._prev_outbounds: deque[dict] = deque(maxlen=5)

    def _make_init_config(self) -> dict[str, Any]:
        config = copy.deepcopy(BASE_CONFIG)
        config["api"]["listen"] = f"127.0.0.1:{self._api_port}"

        for inb_tag, inb_port in self._inbounds:
            inbound_config = copy.deepcopy(BASE_INBOUND)
            inbound_config["tag"] = inb_tag
            inbound_config["port"] = inb_port

            config["inbounds"].append(inbound_config)
        return config

    async def _xray_api_call(
        self,
        command: str,
        payload: dict[str, Any] | None = None,
        extra_args: list[str] | None = None,
        ignore_error: bool = False,
    ) -> None:
        args = [
            str(self._xray_path),
            "api",
            command,
        ]
        args.extend(["-s", f"127.0.0.1:{self._api_port}"])
        if extra_args:
            args.extend(extra_args)
        proc = await asyncio.create_subprocess_exec(
            *args,
            cwd=self._xray_path.parent,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate(
            json.dumps(payload).encode("utf-8") if payload else None
        )

        if proc.returncode != 0:
            if ignore_error:
                return
            err = stderr.decode("utf-8", errors="ignore").strip()
            out = stdout.decode("utf-8", errors="ignore").strip()
            raise RuntimeError(
                f"xray api call failed ({command}) code={proc.returncode}: {err or out}"
            )

    async def start(self):
        self._proc = await asyncio.create_subprocess_exec(
            str(self._xray_path),
            "run",
            cwd=self._xray_path.parent,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        config = self._make_init_config()

        if self._proc.stdin:
            self._proc.stdin.write(json.dumps(config).encode("utf-8"))
            await self._proc.stdin.drain()
            self._proc.stdin.close()

        try:
            await asyncio.wait_for(self._proc.wait(), timeout=0.5)
        except TimeoutError:
            LOGGER.info("Started Xray instance at %s", self._api_port)
            return

        i, e = await self._proc.communicate()
        raise ValueError(
            f"Xray at {self._api_port} is dead with code {self._proc.returncode}."
        )

    async def stop(self) -> None:
        if self._proc and self._proc.returncode is None:
            self._proc.kill()
            try:
                await asyncio.wait_for(self._proc.wait(), timeout=10.0)
            except TimeoutError:
                if self._proc.returncode is None:
                    raise RuntimeError(f"Failed to kill xray at {self._api_port}.")
            LOGGER.info("Stopped Xray instance at %s", self._api_port)

    @asynccontextmanager
    async def with_outbound(
        self, outbound: dict[str, Any], proxy_hash: str, inbound_tag: str
    ):
        if not is_safe_xhttp_config(outbound):
            raise ValueError(
                f"Config {outbound} at {inbound_tag} is not safe. Will ignore it."
            )
        try:
            try:
                async with self._lock:
                    outbound["tag"] = f"proxy-{proxy_hash}"
                    await self._xray_api_call("ado", {"outbounds": [outbound]})

                    rule = copy.deepcopy(BASE_RULE)
                    rule["routing"]["rules"][0]["ruleTag"] = f"rule-{proxy_hash}"
                    rule["routing"]["rules"][0]["inboundTag"].append(inbound_tag)
                    rule["routing"]["rules"][0]["outboundTag"] = outbound["tag"]

                    await self._xray_api_call("adrules", rule, ["-append"])
                    self._prev_rules.append(rule)
                    self._prev_outbounds.append(outbound)
            except Exception as e:
                if self._proc and self._proc.returncode:
                    stdout, stderr = await self._proc.communicate()
                    raise ProcessLookupError()
                raise ValueError(
                    f"Something is wrong with config at {inbound_tag}. Xray api did not liked it."
                )
            yield self
        finally:
            async with self._lock:
                await self._xray_api_call("rmrules", None, [f"rule-{proxy_hash}"])
                await self._xray_api_call("rmo", None, [f"proxy-{proxy_hash}"])


class XrayOrchestrator:
    def __init__(
        self, toolchain: XrayToolchain, num_workers: int, tasks_per_worker: int
    ):
        self._toolchain = toolchain
        self._slots: asyncio.Queue[XraySlot] = asyncio.Queue()
        self._workers: list[XrayInstance] = []
        self._num_workers = num_workers
        self._tasks_per_worker = tasks_per_worker

    async def start(self):
        for wid in range(self._num_workers):
            inbounds = [
                (
                    f"inbound-{wid}-{i}",
                    START_INBOUND_PORT + wid * self._tasks_per_worker + i,
                )
                for i in range(self._tasks_per_worker)
            ]

            worker = XrayInstance(self._toolchain, wid, inbounds)
            await worker.start()

            self._workers.append(worker)

            for tag, port in inbounds:
                await self._slots.put(XraySlot(wid, tag, port))

    async def stop(self):
        await asyncio.gather(*(w.stop() for w in self._workers))

    async def acquire_slot(self) -> XraySlot:
        return await self._slots.get()

    async def release_slot(self, slot: XraySlot):
        await self._slots.put(slot)

    def get_worker(self, worker_id: int) -> XrayInstance:
        return self._workers[worker_id]
