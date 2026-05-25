import asyncio
import json
import logging
import urllib.request
from pathlib import Path
from typing import Any

from pydantic import HttpUrl

from app.batch_operations import BatchTestResultWriter
from app.binary_toolchain import BinaryToolchain
from app.config import GeoIPConfig, TesterConfig
from app.models import (
    CandidateProxy,
    ProxyTestResult,
    TestResultKind,
    TestResultReasons,
)

LOGGER = logging.getLogger(__name__)

IP_RESOLVER_URLS = (
    "http://ipv4.text.wtfismyip.com",
    "http://checkip.amazonaws.com",
    "http://ifconfig.me/ip",
    "http://ifconfig.io/ip",
    "http://icanhazip.com",
    "http://text.ipv4.myip.wtf",
)


class ProxyTester:
    def __init__(
        self,
        batch_writer: BatchTestResultWriter,
        config: TesterConfig,
        kind: TestResultKind,
        toolchain: BinaryToolchain,
        geoip_config: GeoIPConfig | None = None,
    ):
        self._toolchain = toolchain
        self._batch_writer = batch_writer
        self._config = config
        self._kind = kind
        self._tester_args: list[str] = []

        worker_count = 1

        if kind == TestResultKind.CIDR:
            raise ValueError("Invalid test kind")

        self._tester_args.append(f"--test-type={kind.value}")
        self._tester_args.append(f"--retries={config.test_attempts}")
        self._tester_args.append(
            f"--connect-timeout={int(config.url_test.timeout * 1000)}"
        )

        if geoip_config and geoip_config.path and geoip_config.path.exists():
            ensure_geoip_database(geoip_config.path, geoip_config.url)
            self._tester_args.append(
                f"--geoip2-db-path={geoip_config.path.resolve()}")

        if kind == TestResultKind.SPEED:
            worker_count = config.speed_test.worker_count
            self._tester_args.append(f"--url={str(config.speed_test.url)}")
            self._tester_args.append(
                f"--parallelism={config.speed_test.worker_tasks_count}"
            )
            self._tester_args.append(
                f"--download-timeout={int(config.speed_test.timeout * 1000)}"
            )
            self._tester_args.append(
                f"--min-speed-mbps={config.speed_test.speed_threshold}"
            )
        else:
            worker_count = config.url_test.worker_count
            self._tester_args.append(f"--url={str(config.url_test.url)}")
            self._tester_args.append(
                f"--parallelism={config.url_test.worker_tasks_count}"
            )
            self._tester_args.append(
                f"--max-latency={int(config.url_test.timeout * 1000)}"
            )
            self._tester_args.append(
                f"--exit-ip-url={",".join(IP_RESOLVER_URLS[:3])}")

        self._semaphore = asyncio.Semaphore(worker_count)

    async def test_proxy(
        self, data: list[tuple[CandidateProxy, dict[str, Any] | None]]
    ) -> int:
        if self._kind == TestResultKind.CIDR:
            raise ValueError("Invalid test kind")

        async with self._semaphore:
            tester_payload = []
            for candidate, outbound in data:
                if not outbound:
                    self._batch_writer.add(
                        ProxyTestResult(
                            proxy_hash=candidate.proxy_hash,
                            success=False,
                            reason=TestResultReasons.INVALID_URI,
                            kind=self._kind,
                        )
                    )
                    continue
                outbound["tag"] = candidate.proxy_hash
                tester_payload.append(outbound)

            tester_out = await self.run_tester(tester_payload)
            tester_payload.clear()

            if not tester_out:
                raise RuntimeError(f"Tester output was {tester_out}")

            success_count = sum(
                1 if tester_out[outbound_tag]["result"] else 0
                for outbound_tag in tester_out
            )

            self._batch_writer.add_many(
                ProxyTestResult.from_dict(
                    outbound_tag, self._kind, tester_out[outbound_tag]
                )
                for outbound_tag in tester_out
            )
            tester_out.clear()
            return success_count

    async def run_tester(
        self, tester_payload: list[dict[str, Any]]
    ) -> dict[str, Any] | None:
        proc = await asyncio.create_subprocess_exec(
            str(self._toolchain.xray_path),
            *self._tester_args,
            cwd=self._toolchain.xray_path.parent,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        LOGGER.debug("Started tester on batch of %s proxies",
                     len(tester_payload))

        try:
            o, e = await proc.communicate(json.dumps(tester_payload).encode())

            if proc.returncode != 0:
                print(" ".join(self._tester_args))
                raise RuntimeError(
                    f"Tester exited with code {proc.returncode}. Error was: '{e.decode()}'"
                )

            result = None
            for line in o.decode().split("\n"):
                line = line.strip()
                if line.startswith("{"):
                    result = json.loads(line)
                    break

            return result
        except asyncio.CancelledError:
            if proc.returncode is None:
                proc.kill()
                await proc.wait()

        return None


def ensure_geoip_database(geoip_path: Path, geoip_url: HttpUrl | None) -> None:
    """Ensure local GeoIP database exists."""

    if not geoip_url:
        LOGGER.debug("geoip_db_url is not set. Skipping GeoIP download.")
        return

    LOGGER.info(
        "Downloading GeoIP DB from %s to %s",
        geoip_url,
        geoip_path,
    )
    geoip_path.parent.mkdir(parents=True, exist_ok=True)
    urllib.request.urlretrieve(str(geoip_url), geoip_path)
    LOGGER.info("GeoIP DB download complete: %s", geoip_path)
