from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Literal

from .config import GeoIPConfig, DeadTTLConfig
from .models import CandidateProxy, ProxyTestResult, Subscripton


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
                        first_seen_at INTEGER NOT NULL,
                        last_seen_at INTEGER NOT NULL,
                        last_checked_at INTEGER,
                        last_status TEXT NOT NULL DEFAULT 'unknown',
                        reason TEXT NOT NULL DEFAULT 'unknown',
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
                        created_at INTEGER NOT NULL,
                        expires_at INTEGER NOT NULL
                    ) WITHOUT ROWID;

                    CREATE INDEX IF NOT EXISTS idx_dead_proxy_expires
                    ON dead_proxies(expires_at);
          
                    CREATE INDEX IF NOT EXISTS idx_proxies_latency_hash
                    ON proxies(latency_ms, proxy_hash) WHERE latency_ms IS NOT NULL;

                    CREATE TABLE IF NOT EXISTS selected_proxies (
                        proxy_hash TEXT PRIMARY KEY,
                        selected_at INTEGER NOT NULL,
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
        with self._write_lock:
            with self.connect() as conn:
                cur = conn.execute(
                    "DELETE FROM dead_proxies WHERE expires_at < strftime('%s','now')"
                )
                return cur.rowcount

    def mark_results(self, results: list[ProxyTestResult]) -> None:
        if not results:
            return

        sql = """
        UPDATE proxies
        SET last_checked_at = strftime('%s','now'),
            last_status = ?,
            latency_ms = COALESCE(?, latency_ms),
            exit_ip    = COALESCE(?, exit_ip),
            country    = COALESCE(?, country),
            city       = COALESCE(?, city),
            reason     = ?,
            mbps       = COALESCE(?, mbps)
        WHERE proxy_hash = ?
        """
        params = [
            (
                "dead" if not r.success else (r.kind.value + "_ok"),
                r.latency_ms,
                r.exit_ip,
                r.country,
                r.city,
                r.reason.value,
                r.mbps,
                r.proxy_hash,
            )
            for r in results
        ]

        with self._write_lock:
            with self.connect() as conn:
                conn.executemany(sql, params)

    def get_selected(self) -> list[sqlite3.Row]:
        with self.connect() as conn:
            return conn.execute(
                "SELECT * FROM selected_proxies ORDER BY mbps DESC, latency_ms ASC"
            ).fetchall()

    def store_selected(self, target_final_count: int) -> None:
        if target_final_count <= 0:
            return

        with self._write_lock:
            with self.connect() as conn:
                conn.execute("DELETE FROM selected_proxies")

                query = """
                    INSERT INTO selected_proxies
                        (proxy_hash, selected_at, raw_link, latency_ms, mbps, exit_ip, country, city)
                    SELECT
                        proxy_hash,
                        strftime('%s','now'),
                        raw_link,
                        latency_ms,
                        mbps,
                        exit_ip,
                        country,
                        city
                    FROM proxies
                    WHERE last_status = 'speed_ok'
                    ORDER BY mbps DESC NULLS LAST
                    LIMIT ?
                """

                conn.execute(query, (target_final_count,))

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

    def prepare_fresh_candidates_table(self) -> None:
        with self._write_lock:
            with self.connect() as conn:
                conn.execute("DROP TABLE IF EXISTS temp_fresh_candidates;")
                conn.execute("""
                        CREATE TEMP TABLE temp_fresh_candidates (
                            proxy_hash TEXT PRIMARY KEY,
                            raw_link   TEXT NOT NULL,
                            scheme     TEXT NOT NULL
                        ) WITHOUT ROWID;
                    """)

    def insert_fresh_candidates_batch(self, data: list[CandidateProxy]) -> None:
        if not data:
            return
        with self._write_lock:
            with self.connect() as conn:
                conn.executemany(
                    "INSERT OR IGNORE INTO temp_fresh_candidates VALUES (?, ?, ?)",
                    [i.to_row() for i in data],
                )

    def merge_fresh_candidates_to_proxies(self) -> int:
        with self._write_lock:
            with self.connect() as conn:
                conn.execute("""
                    INSERT INTO proxies (
                        proxy_hash, raw_link, scheme,
                        first_seen_at, last_seen_at,
                        last_checked_at, last_status, reason,
                        latency_ms, mbps, exit_ip, country, city
                    )
                    SELECT
                        t.proxy_hash,
                        t.raw_link,
                        t.scheme,
                        COALESCE(p.first_seen_at, strftime('%s','now')) AS first_seen_at,
                        strftime('%s','now')                            AS last_seen_at,
                        p.last_checked_at,
                        COALESCE(p.last_status, 'unknown')           AS last_status,
                        COALESCE(p.reason, 'unknown')                AS reason,
                        p.latency_ms,
                        p.mbps,
                        p.exit_ip,
                        p.country,
                        p.city
                    FROM temp_fresh_candidates t
                    LEFT JOIN proxies p ON p.proxy_hash = t.proxy_hash
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM dead_proxies d
                        WHERE d.proxy_hash = t.proxy_hash
                    )
                    ON CONFLICT (proxy_hash) DO UPDATE SET
                        last_seen_at = strftime('%s','now'),
                        raw_link     = excluded.raw_link,
                        scheme       = excluded.scheme;
                """)

                row = conn.execute("SELECT changes()").fetchone()
                return row[0] if row else 0

    def count_fresh_candidates(self) -> int:
        with self._write_lock:
            with self.connect() as conn:
                num_fresh = conn.execute(
                    "SELECT COUNT(*) FROM temp_fresh_candidates"
                ).fetchone()[0]
                return num_fresh

    def cleanup_fresh_candidates_table(self) -> None:
        with self._write_lock:
            with self.connect() as conn:
                conn.execute("DROP TABLE IF EXISTS temp_fresh_candidates;")

    def fetch_candidate_proxies_batch(
        self,
        limit: int,
        after_proxy_hash: str | None = None,
        after_latency_ms: float | None = None,
        order_by: Literal["proxy_hash", "latency"] = "proxy_hash",
    ) -> tuple[list[CandidateProxy], sqlite3.Row | None]:
        with self._write_lock:
            with self.connect() as conn:
                if order_by == "latency":
                    if after_latency_ms is None or after_proxy_hash is None:
                        cursor = conn.execute(
                            """
                            SELECT *
                            FROM proxies
                            WHERE latency_ms IS NOT NULL
                            ORDER BY latency_ms ASC, proxy_hash ASC
                            LIMIT ?
                            """,
                            (limit,),
                        )
                    else:
                        cursor = conn.execute(
                            """
                            SELECT *
                            FROM proxies
                            WHERE latency_ms IS NOT NULL
                            AND (
                                latency_ms > ?
                                OR (latency_ms = ? AND proxy_hash > ?)
                            )
                            ORDER BY latency_ms ASC, proxy_hash ASC
                            LIMIT ?
                            """,
                            (
                                after_latency_ms,
                                after_latency_ms,
                                after_proxy_hash,
                                limit,
                            ),
                        )
                else:
                    if after_proxy_hash is None:
                        cursor = conn.execute(
                            """
                            SELECT *
                            FROM proxies
                            ORDER BY proxy_hash ASC
                            LIMIT ?
                            """,
                            (limit,),
                        )
                    else:
                        cursor = conn.execute(
                            """
                            SELECT *
                            FROM proxies
                            WHERE proxy_hash > ?
                            ORDER BY proxy_hash ASC
                            LIMIT ?
                            """,
                            (after_proxy_hash, limit),
                        )

                proxies = cursor.fetchall()
                return (
                    [CandidateProxy.from_row(row) for row in proxies],
                    proxies[-1] if len(proxies) > 0 else None,
                )

    def count_candidate_proxies(self) -> int:
        with self._write_lock:
            with self.connect() as conn:
                num_fresh = conn.execute(
                    "SELECT COUNT(*) FROM proxies").fetchone()[0]
                return num_fresh

    def count_candidate_proxies_with_status(self, status: str) -> int:
        with self._write_lock:
            with self.connect() as conn:
                num_fresh = conn.execute(
                    "SELECT COUNT(*) FROM proxies WHERE last_status = ?", (status,)
                ).fetchone()[0]
                return num_fresh

    def move_dead_proxies(self, ttl_config: DeadTTLConfig) -> None:
        sql = f"""
        INSERT INTO dead_proxies (
            proxy_hash, raw_link, scheme, reason, created_at, expires_at
        )
        SELECT
            p.proxy_hash,
            p.raw_link,
            p.scheme,
            CASE COALESCE(p.reason, 'other')
                WHEN 'cidr_discarded' THEN 'cidr_discarded'
                WHEN 'connect_failed' THEN 'connect_failed'
                WHEN 'latency_exceeded' THEN 'latency_exceeded'
                WHEN 'speed_test_failed' THEN 'speed_test_failed'
                WHEN 'speed_below_threshold' THEN 'speed_below_threshold'
                WHEN 'url_test_failed' THEN 'url_test_failed'
                WHEN 'discarded_filtering' THEN 'discarded_filtering'
                ELSE 'other'
            END AS reason,
            strftime('%s', 'now') AS created_at,
            CASE COALESCE(p.reason, 'other')
                WHEN 'cidr_discarded' THEN strftime('%s', 'now', '+{ttl_config.cidr_discarded} days')
                WHEN 'connect_failed' THEN strftime('%s', 'now', '+{ttl_config.connect_failed} days')
                WHEN 'latency_exceeded' THEN strftime('%s', 'now', '+{ttl_config.latency_exceeded} days')
                WHEN 'speed_test_failed' THEN strftime('%s', 'now', '+{ttl_config.speed_test_failed} days')
                WHEN 'speed_below_threshold' THEN strftime('%s', 'now', '+{ttl_config.speed_below_threshold} days')
                WHEN 'url_test_failed' THEN strftime('%s', 'now', '+{ttl_config.url_test_failed} days')
                WHEN 'discarded_filtering' THEN strftime('%s', 'now', '+{ttl_config.discarded_filtering} days')
                ELSE strftime('%s', 'now', '+{ttl_config.other} days')
            END AS expires_at
        FROM proxies p
        WHERE p.last_status = 'dead';

        DELETE FROM proxies
        WHERE last_status = 'dead';
        """

        with self._write_lock:
            with self.connect() as conn:
                conn.executescript(sql)
                conn.commit()

    def geoip_filter_proxies(self, geoip_config: GeoIPConfig):
        if not geoip_config:
            return

        with self._write_lock:
            with self.connect() as conn:
                cur = conn.cursor()

                country_placeholders = ",".join(
                    "?" for _ in geoip_config.countries)
                filter_mode = " " if geoip_config.method == "exclude" else " NOT "

                sql = f"""
                    UPDATE proxies
                    SET last_status = 'dead',
                        reason = 'discarded_filtering'
                    WHERE
                        last_status IN ('url_ok', 'speed_ok')
                        AND country IS NOT NULL
                        AND country{filter_mode}IN ({country_placeholders})
                """

                params = (*geoip_config.countries,)
                cur.execute(sql, params)
                conn.commit()

    def mark_dead_duplicate_ip_proxies(self) -> int:
        with self._write_lock:
            with self.connect() as conn:
                cursor = conn.execute("""
                    WITH ranked AS (
                        SELECT
                            proxy_hash,
                            ROW_NUMBER() OVER (
                                PARTITION BY exit_ip
                                ORDER BY
                                    latency_ms IS NULL,
                                    latency_ms ASC,
                                    proxy_hash ASC
                            ) AS rn
                        FROM proxies
                        WHERE exit_ip IS NOT NULL
                        AND last_status <> 'dead'
                    )
                    UPDATE proxies
                    SET
                        last_status = 'dead',
                        reason = 'duplicate'
                    WHERE last_status <> 'dead' 
                        AND proxy_hash IN (
                            SELECT proxy_hash FROM ranked WHERE rn > 1
                        )
                    """)
                cursor.execute("SELECT changes()")
                row = cursor.fetchone()
                return row[0] if row is not None else 0

    def cleanup_duplicate_dead_proxies(self) -> int:
        with self._write_lock:
            with self.connect() as conn:
                cursor = conn.execute("""
                    DELETE FROM dead_proxies
                    WHERE reason = 'duplicate'
                    """)
                return cursor.rowcount
