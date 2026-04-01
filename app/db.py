from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path


@dataclass
class ProxyRecord:
    proxy_hash: str
    raw_link: str
    scheme: str
    last_status: str


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


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

        # Блокировка ТОЛЬКО для записи (чтение остаётся конкурентным — SQLite + WAL это позволяет)
        self._write_lock = threading.Lock()

    @contextmanager
    def connect(self):
        """Возвращает существующее соединение.
        Коммит происходит только при успешном завершении блока,
        при любом исключении — откат.
        """
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
                conn.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS proxies (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        proxy_hash TEXT NOT NULL UNIQUE,
                        raw_link TEXT NOT NULL,
                        scheme TEXT NOT NULL,
                        first_seen_at TEXT NOT NULL,
                        last_seen_at TEXT NOT NULL,
                        last_status TEXT NOT NULL DEFAULT 'unknown'
                    );

                    CREATE TABLE IF NOT EXISTS url_test_results (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        proxy_hash TEXT NOT NULL,
                        checked_at TEXT NOT NULL,
                        success INTEGER NOT NULL,
                        latency_ms REAL,
                        exit_ip TEXT,
                        country TEXT,
                        city TEXT,
                        details_json TEXT,
                        FOREIGN KEY(proxy_hash) REFERENCES proxies(proxy_hash)
                    );

                    CREATE INDEX IF NOT EXISTS idx_url_results_hash_time
                    ON url_test_results(proxy_hash, checked_at DESC);

                    CREATE TABLE IF NOT EXISTS speed_test_results (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        proxy_hash TEXT NOT NULL,
                        checked_at TEXT NOT NULL,
                        success INTEGER NOT NULL,
                        mbps REAL,
                        bytes_downloaded INTEGER,
                        details_json TEXT,
                        FOREIGN KEY(proxy_hash) REFERENCES proxies(proxy_hash)
                    );

                    CREATE TABLE IF NOT EXISTS dead_proxies (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        proxy_hash TEXT NOT NULL UNIQUE,
                        reason TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        expires_at TEXT NOT NULL
                    );

                    CREATE INDEX IF NOT EXISTS idx_dead_proxy_expires
                    ON dead_proxies(expires_at);

                    CREATE TABLE IF NOT EXISTS selected_proxies (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        selected_at TEXT NOT NULL,
                        proxy_hash TEXT NOT NULL,
                        raw_link TEXT NOT NULL,
                        latency_ms REAL,
                        mbps REAL,
                        exit_ip TEXT,
                        country TEXT,
                        city TEXT
                    );
                    """
                )

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
        now_iso = utc_now().isoformat()
        with self._write_lock:
            with self.connect() as conn:
                conn.execute(
                    """
                    INSERT INTO proxies(proxy_hash, raw_link, scheme, first_seen_at, last_seen_at, last_status)
                    VALUES (?, ?, ?, ?, ?, 'unknown')
                    ON CONFLICT(proxy_hash) DO UPDATE SET
                        raw_link = excluded.raw_link,
                        scheme = excluded.scheme,
                        last_seen_at = excluded.last_seen_at
                    """,
                    (proxy_hash, raw_link, scheme, now_iso, now_iso),
                )

    def is_dead(self, proxy_hash: str) -> bool:
        # Чтение — без блокировки (SQLite + WAL позволяет конкурентные чтения)
        now_iso = utc_now().isoformat()
        with self.connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM dead_proxies WHERE proxy_hash = ? AND expires_at > ?",
                (proxy_hash, now_iso),
            ).fetchone()
            return row is not None

    def mark_dead(self, proxy_hash: str, reason: str, ttl_days: int) -> None:
        now = utc_now()
        with self._write_lock:
            with self.connect() as conn:
                conn.execute(
                    """
                    INSERT INTO dead_proxies(proxy_hash, reason, created_at, expires_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(proxy_hash) DO UPDATE SET
                        reason = excluded.reason,
                        created_at = excluded.created_at,
                        expires_at = excluded.expires_at
                    """,
                    (
                        proxy_hash,
                        reason,
                        now.isoformat(),
                        (now + timedelta(days=ttl_days)).isoformat(),
                    ),
                )
                conn.execute(
                    "UPDATE proxies SET last_status = 'dead' WHERE proxy_hash = ?",
                    (proxy_hash,),
                )

    def save_url_result(
        self,
        proxy_hash: str,
        success: bool,
        latency_ms: float | None,
        exit_ip: str | None,
        country: str | None,
        city: str | None,
        details_json: str | None,
    ) -> None:
        now_iso = utc_now().isoformat()
        with self._write_lock:
            with self.connect() as conn:
                conn.execute(
                    """
                    INSERT INTO url_test_results(proxy_hash, checked_at, success, latency_ms, exit_ip, country, city, details_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        proxy_hash,
                        now_iso,
                        int(success),
                        latency_ms,
                        exit_ip,
                        country,
                        city,
                        details_json,
                    ),
                )
                conn.execute(
                    "UPDATE proxies SET last_status = ? WHERE proxy_hash = ?",
                    ("url_ok" if success else "dead", proxy_hash),
                )

    def save_speed_result(
        self,
        proxy_hash: str,
        success: bool,
        mbps: float | None,
        bytes_downloaded: int | None,
        details_json: str | None,
    ) -> None:
        now_iso = utc_now().isoformat()
        with self._write_lock:
            with self.connect() as conn:
                conn.execute(
                    """
                    INSERT INTO speed_test_results(proxy_hash, checked_at, success, mbps, bytes_downloaded, details_json)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (proxy_hash, now_iso, int(success),
                     mbps, bytes_downloaded, details_json),
                )
                if success:
                    conn.execute(
                        "UPDATE proxies SET last_status = 'speed_ok' WHERE proxy_hash = ?",
                        (proxy_hash,),
                    )

    def get_recent_selected(self, limit: int) -> list[sqlite3.Row]:
        # Чтение — без блокировки
        with self.connect() as conn:
            return conn.execute(
                """
                SELECT s.*
                FROM selected_proxies s
                ORDER BY s.selected_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

    def get_recent_url_ok(self, limit: int) -> list[sqlite3.Row]:
        # Чтение — без блокировки
        with self.connect() as conn:
            return conn.execute(
                """
                WITH latest AS (
                    SELECT
                      u.proxy_hash,
                      MAX(u.checked_at) AS checked_at
                    FROM url_test_results u
                    GROUP BY u.proxy_hash
                )
                SELECT
                  p.proxy_hash,
                  p.raw_link,
                  u.latency_ms,
                  u.exit_ip,
                  u.country,
                  u.city
                FROM latest l
                JOIN url_test_results u
                  ON u.proxy_hash = l.proxy_hash
                 AND u.checked_at = l.checked_at
                JOIN proxies p ON p.proxy_hash = u.proxy_hash
                WHERE u.success = 1
                ORDER BY u.latency_ms ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

    def store_selected(self, rows: list[dict]) -> None:
        now_iso = utc_now().isoformat()
        with self._write_lock:
            with self.connect() as conn:
                conn.execute("DELETE FROM selected_proxies")
                conn.executemany(
                    """
                    INSERT INTO selected_proxies(
                        selected_at, proxy_hash, raw_link, latency_ms, mbps, exit_ip, country, city
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            now_iso,
                            r["proxy_hash"],
                            r["raw_link"],
                            r.get("latency_ms"),
                            r.get("mbps"),
                            r.get("exit_ip"),
                            r.get("country"),
                            r.get("city"),
                        )
                        for r in rows
                    ],
                )
