from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .models import SpeedTestResult, Subscripton, UrlTestResult


@dataclass
class ProxyRecord:
    proxy_hash: str
    raw_link: str
    scheme: str
    last_status: str


def utc_now() -> datetime:
    return datetime.now(timezone.utc)  # noqa: UP017 (runtime uses Python 3.10)


class Database:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._conn = sqlite3.connect(
            self.db_path,
            check_same_thread=False,
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA foreign_keys=ON;")
        self._conn.execute("PRAGMA synchronous=NORMAL;")
        self._conn.execute("PRAGMA temp_store=MEMORY;")
        self._conn.execute("PRAGMA busy_timeout=5000;")

        self._write_lock = threading.Lock()

    @contextmanager
    def connect(self):
        try:
            yield self._conn
        except Exception:
            self._conn.rollback()
            raise
        else:
            self._conn.commit()

    def init_schema(self) -> None:
        with self._write_lock:
            with self.connect() as conn:
                conn.executescript("""
                    CREATE TABLE IF NOT EXISTS proxies (
                        proxy_hash TEXT PRIMARY KEY,
                        raw_link TEXT NOT NULL,
                        scheme TEXT NOT NULL,
                        first_seen_at TEXT NOT NULL,
                        last_seen_at TEXT NOT NULL,
                        last_checked_at TEXT,
                        last_status TEXT NOT NULL DEFAULT 'unknown',
                        latency_ms REAL,
                        mbps REAL,
                        exit_ip TEXT,
                        country TEXT,
                        city TEXT
                    ) WITHOUT ROWID;

                    CREATE TABLE IF NOT EXISTS dead_proxies (
                        proxy_hash TEXT PRIMARY KEY,
                        raw_link TEXT NOT NULL,
                        scheme TEXT NOT NULL,
                        reason TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        expires_at TEXT NOT NULL
                    ) WITHOUT ROWID;

                    CREATE INDEX IF NOT EXISTS idx_dead_proxy_expires
                    ON dead_proxies(expires_at);

                    CREATE TABLE IF NOT EXISTS selected_proxies (
                        proxy_hash TEXT PRIMARY KEY,
                        selected_at TEXT NOT NULL,
                        raw_link TEXT NOT NULL,
                        latency_ms REAL,
                        mbps REAL,
                        exit_ip TEXT,
                        country TEXT,
                        city TEXT
                    ) WITHOUT ROWID;

                    CREATE TABLE IF NOT EXISTS subscriptions (
                        link TEXT PRIMARY KEY,
                        last_data_hash TEXT NOT NULL
                    ) WITHOUT ROWID;
                    """)

    def cleanup_expired_dead(self) -> int:
        now_iso = utc_now().isoformat()
        with self._write_lock:
            with self.connect() as conn:
                cur = conn.execute(
                    "DELETE FROM dead_proxies WHERE expires_at < ?",
                    (now_iso,),
                )
                return cur.rowcount

    def upsert_proxy(self, proxy_hash: str, raw_link: str, scheme: str) -> None:
        self.upsert_proxies([(proxy_hash, raw_link, scheme)])

    def upsert_proxies(self, rows: list[tuple[str, str, str]]) -> None:
        if not rows:
            return
        now_iso = utc_now().isoformat()
        with self._write_lock:
            with self.connect() as conn:
                conn.executemany(
                    """
                    INSERT INTO proxies(proxy_hash, raw_link, scheme, first_seen_at, last_seen_at, last_status)
                    VALUES (?, ?, ?, ?, ?, 'unknown')
                    ON CONFLICT(proxy_hash) DO UPDATE SET
                        raw_link = excluded.raw_link,
                        scheme = excluded.scheme,
                        last_seen_at = excluded.last_seen_at
                    """,
                    [(h, link, sch, now_iso, now_iso) for h, link, sch in rows],
                )

    def is_dead(self, proxy_hash: str) -> bool:
        now_iso = utc_now().isoformat()
        with self.connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM dead_proxies WHERE proxy_hash = ? AND expires_at > ?",
                (proxy_hash, now_iso),
            ).fetchone()
            return row is not None

    def get_alive_hashes(self, proxy_hashes: list[str]) -> set[str]:
        if not proxy_hashes:
            return set()

        unique_hashes = list(dict.fromkeys(proxy_hashes))
        now_iso = utc_now().isoformat()

        with self.connect() as conn:
            conn.execute("DROP TABLE IF EXISTS temp_hashes")
            conn.execute("CREATE TEMP TABLE temp_hashes (proxy_hash TEXT)")

            conn.executemany(
                "INSERT INTO temp_hashes (proxy_hash) VALUES (?)",
                [(proxy_hash,) for proxy_hash in unique_hashes],
            )

            rows = conn.execute(
                """
                SELECT d.proxy_hash
                FROM dead_proxies d
                INNER JOIN temp_hashes t ON d.proxy_hash = t.proxy_hash
                WHERE d.expires_at > ?
                """,
                (now_iso,),
            ).fetchall()

            dead_hashes = {row["proxy_hash"] for row in rows}

        return {
            proxy_hash for proxy_hash in unique_hashes if proxy_hash not in dead_hashes
        }

    def mark_dead(self, proxy_hash: str, reason: str, ttl_days: int) -> None:
        self.mark_dead_many([(proxy_hash, reason)], ttl_days=ttl_days)

    def mark_dead_many(self, rows: list[tuple[str, str]], ttl_days: int) -> None:
        if not rows:
            return
        now = utc_now()
        created_at = now.isoformat()
        expires_at = (now + timedelta(days=ttl_days)).isoformat()
        hashes = [proxy_hash for proxy_hash, _ in rows]
        placeholders = ",".join("?" for _ in hashes)

        with self._write_lock:
            with self.connect() as conn:
                existing = conn.execute(
                    f"SELECT proxy_hash, raw_link, scheme FROM proxies WHERE proxy_hash IN ({placeholders})",
                    hashes,
                ).fetchall()
                by_hash = {
                    row["proxy_hash"]: (row["raw_link"], row["scheme"])
                    for row in existing
                }

                conn.executemany(
                    """
                    INSERT INTO dead_proxies(proxy_hash, raw_link, scheme, reason, created_at, expires_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(proxy_hash) DO UPDATE SET
                        raw_link = excluded.raw_link,
                        scheme = excluded.scheme,
                        reason = excluded.reason,
                        created_at = excluded.created_at,
                        expires_at = excluded.expires_at
                    """,
                    [
                        (
                            proxy_hash,
                            raw_link,
                            scheme,
                            reason,
                            created_at,
                            expires_at,
                        )
                        for proxy_hash, reason in rows
                        for raw_link, scheme in [
                            self._get_proxy_identity(by_hash, proxy_hash)
                        ]
                    ],
                )
                conn.executemany(
                    "DELETE FROM proxies WHERE proxy_hash = ?",
                    [(proxy_hash,) for proxy_hash in hashes],
                )

    @staticmethod
    def _get_proxy_identity(
        by_hash: dict[str, tuple[str, str]],
        proxy_hash: str,
    ) -> tuple[str, str]:
        return by_hash.get(proxy_hash, ("", "unknown"))

    def mark_url_results(self, rows: list[UrlTestResult]) -> None:
        if not rows:
            return
        now_iso = utc_now().isoformat()
        with self._write_lock:
            with self.connect() as conn:
                conn.executemany(
                    """
                    UPDATE proxies
                       SET last_checked_at = ?,
                           last_status = ?,
                           latency_ms = ?,
                           exit_ip = ?,
                           country = ?,
                           city = ?,
                           mbps = NULL
                     WHERE proxy_hash = ?
                    """,
                    [
                        (
                            now_iso,
                            "url_ok" if row.success else "dead",
                            row.latency_ms,
                            row.exit_ip,
                            row.country,
                            row.city,
                            row.proxy_hash,
                        )
                        for row in rows
                    ],
                )

    def mark_speed_results(self, results: list[SpeedTestResult]) -> None:
        if not results:
            return
        now_iso = utc_now().isoformat()
        with self._write_lock:
            with self.connect() as conn:
                conn.executemany(
                    """
                    UPDATE proxies
                       SET last_checked_at = ?,
                           last_status = ?,
                           mbps = ?
                     WHERE proxy_hash = ?
                    """,
                    [
                        (
                            now_iso,
                            "speed_ok" if res.success else "dead",
                            res.mbps,
                            res.proxy_hash,
                        )
                        for res in results
                    ],
                )

    def get_recent_selected(self) -> list[sqlite3.Row]:
        with self.connect() as conn:
            return conn.execute("""
                SELECT s.*
                FROM selected_proxies s
                ORDER BY s.selected_at DESC
                """).fetchall()

    def get_recent_url_ok(self, limit: int) -> list[sqlite3.Row]:
        with self.connect() as conn:
            return conn.execute(
                """
                SELECT
                  p.proxy_hash,
                  p.raw_link,
                  p.latency_ms,
                  p.exit_ip,
                  p.country,
                  p.city
                FROM proxies p
                WHERE p.last_status IN ('url_ok', 'speed_ok')
                ORDER BY p.latency_ms ASC, p.last_checked_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

    def get_recent_all(self, limit: int) -> list[sqlite3.Row]:
        with self.connect() as conn:
            return conn.execute(
                """
                SELECT
                  p.proxy_hash,
                  p.raw_link,
                  p.scheme,
                  p.latency_ms,
                  p.exit_ip,
                  p.country,
                  p.city
                FROM proxies p
                ORDER BY p.latency_ms ASC, p.last_checked_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

    def stream_proxies(self, fetch_size: int = 256):
        fetch_size = max(1, fetch_size)
        with self.connect() as conn:
            cursor = conn.execute(
                """
                SELECT
                  p.proxy_hash,
                  p.raw_link,
                  p.scheme,
                  p.latency_ms,
                  p.exit_ip,
                  p.country,
                  p.city
                FROM proxies p
                ORDER BY p.last_checked_at DESC, p.proxy_hash ASC
                """
            )
            while True:
                rows = cursor.fetchmany(fetch_size)
                if not rows:
                    return
                yield from rows

    def store_selected(self, selected: list[str]) -> None:
        if not selected:
            return
        now_iso = utc_now().isoformat()
        with self._write_lock:
            with self.connect() as conn:
                conn.execute("DELETE FROM selected_proxies")

                placeholders = ",".join("?" for _ in selected)

                query = f"""
                    INSERT INTO selected_proxies
                        (proxy_hash, selected_at, raw_link, latency_ms, mbps, exit_ip, country, city)
                    SELECT proxy_hash,
                        ? AS selected_at,
                        raw_link,
                        latency_ms,
                        mbps,
                        exit_ip,
                        country,
                        city
                    FROM proxies
                    WHERE proxy_hash IN ({placeholders})
                """

                params = [now_iso] + selected
                conn.execute(query, params)

    def count_proxies(self) -> int:
        with self.connect() as conn:
            row = conn.execute("SELECT COUNT(*) AS cnt FROM proxies").fetchone()
            return int(row["cnt"] if row is not None else 0)

    def select_top_speed(self, limit: int, min_speed_mb_s: float) -> list[str]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT p.proxy_hash
                FROM proxies p
                WHERE p.last_status = 'speed_ok'
                  AND p.mbps IS NOT NULL
                  AND p.mbps >= ?
                ORDER BY p.mbps DESC, p.latency_ms ASC, p.last_checked_at DESC
                LIMIT ?
                """,
                (min_speed_mb_s, limit),
            ).fetchall()
            return [str(row["proxy_hash"]) for row in rows]

    def get_subscription(self, link: str) -> Subscripton | None:
        if not link:
            return None
        with self._write_lock:
            with self.connect() as conn:
                row = conn.execute(
                    """
                    SELECT link, last_data_hash FROM subscriptions
                    WHERE link = ?
                    """,
                    (link,),
                ).fetchone()
                if row is not None:
                    return Subscripton(row["link"], row["last_data_hash"])
        return None

    def upsert_subscription(self, subscription: Subscripton) -> None:
        if not subscription:
            return
        with self._write_lock:
            with self.connect() as conn:
                conn.execute(
                    """
                    INSERT INTO subscriptions(link, last_data_hash)
                    VALUES (?, ?)
                    ON CONFLICT(link) DO UPDATE SET
                        link = excluded.link,
                        last_data_hash = excluded.last_data_hash
                    """,
                    (subscription.link, subscription.last_data_hash),
                )
