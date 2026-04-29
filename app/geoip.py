import ipaddress
import logging
import urllib.request

import geoip2.database
from geoip2.errors import AddressNotFoundError

from app.config import GeoIPConfig

LOGGER = logging.getLogger(__name__)


class GeoIPReader:
    def __init__(self, config: GeoIPConfig) -> None:
        self._config = config
        self._geoip_reader: geoip2.database.Reader | None = None
        self._geoip_error: str | None = None

    def ensure_geoip_database(self) -> None:
        """Ensure local GeoIP database exists.

        Args:
            geoip_db_path: Local destination path for `.mmdb`.
            geoip_db_url: Optional download URL. If absent, no download is attempted.
        """

        if not self._config.url:
            LOGGER.debug("geoip_db_url is not set. Skipping GeoIP download.")
            return

        LOGGER.info(
            "Downloading GeoIP DB from %s to %s",
            self._config.url,
            self._config.path,
        )
        self._config.path.parent.mkdir(parents=True, exist_ok=True)
        urllib.request.urlretrieve(str(self._config.url), self._config.path)
        LOGGER.info("GeoIP DB download complete: %s", self._config.path)

    def _ensure_geoip_reader(self) -> geoip2.database.Reader | None:
        if self._geoip_reader is not None:
            return self._geoip_reader
        if self._geoip_error is not None:
            return None
        if self._config.path is None:
            self._geoip_error = "geoip_db_path_not_set"
            return None

        try:
            if not self._config.path.exists():
                self._geoip_error = f"geoip_db_not_found:{self._config.path}"
                return None
            self._geoip_reader = geoip2.database.Reader(self._config.path)
            return self._geoip_reader
        except Exception as exc:
            self._geoip_error = f"geoip_reader_init_failed:{exc}"
            LOGGER.exception("Failed to initialize GeoIP reader")
            return None

    def geoip_lookup(self, ip_value: str | None) -> tuple[str | None, str | None]:
        if not ip_value:
            return None, None
        try:
            ipaddress.ip_address(ip_value)
        except ValueError:
            return None, None

        reader = self._ensure_geoip_reader()
        if reader is None:
            return None, None

        try:
            city_record = reader.city(ip_value)
        except AddressNotFoundError:
            return None, None
        except Exception:
            LOGGER.exception("GeoIP lookup failed for ip=%s", ip_value)
            return None, None

        country = city_record.country.iso_code or city_record.country.name
        city = city_record.city.name
        return country, city
