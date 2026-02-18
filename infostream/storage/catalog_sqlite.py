from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path


@dataclass
class VersionDecision:
    version: str
    is_new_item: bool
    is_new_version: bool


@dataclass
class DailyCacheRecord:
    date_key: str
    normalized_url: str
    item_id: str
    version: str
    source_type: str
    source_name: str
    source_group: str
    item_json_path: str
    evidence_json_path: str
    raw_root_path: str
    fetched_at: str
    run_id: str


@dataclass
class SourceCooldown:
    source_group: str
    blocked_until: str
    reason: str
    updated_at: str


class CatalogSQLite:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS items (
                id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                first_seen_at TEXT NOT NULL,
                latest_version TEXT NOT NULL,
                latest_text_hash TEXT NOT NULL,
                latest_meta_hash TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS item_versions (
                id TEXT NOT NULL,
                version TEXT NOT NULL,
                fetched_at TEXT NOT NULL,
                published_at TEXT,
                title TEXT NOT NULL,
                content_type TEXT NOT NULL,
                text_hash TEXT NOT NULL,
                meta_hash TEXT NOT NULL,
                item_json_path TEXT NOT NULL,
                evidence_json_path TEXT NOT NULL,
                raw_root_path TEXT NOT NULL,
                PRIMARY KEY (id, version)
            );

            CREATE TABLE IF NOT EXISTS run_items (
                run_id TEXT NOT NULL,
                id TEXT NOT NULL,
                version TEXT,
                status TEXT NOT NULL,
                error_code TEXT,
                PRIMARY KEY (run_id, id, status)
            );

            CREATE TABLE IF NOT EXISTS daily_url_cache (
                date_key TEXT NOT NULL,
                normalized_url TEXT NOT NULL,
                item_id TEXT NOT NULL,
                version TEXT NOT NULL,
                source_type TEXT NOT NULL,
                source_name TEXT NOT NULL DEFAULT '',
                source_group TEXT NOT NULL,
                item_json_path TEXT NOT NULL,
                evidence_json_path TEXT NOT NULL,
                raw_root_path TEXT NOT NULL,
                fetched_at TEXT NOT NULL,
                run_id TEXT NOT NULL,
                PRIMARY KEY (date_key, normalized_url)
            );

            CREATE INDEX IF NOT EXISTS idx_daily_url_cache_group_fetched
            ON daily_url_cache (date_key, source_group, fetched_at DESC);

            CREATE TABLE IF NOT EXISTS source_cooldowns (
                source_group TEXT PRIMARY KEY,
                blocked_until TEXT NOT NULL,
                reason TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """
        )
        self._ensure_column("daily_url_cache", "source_name", "TEXT NOT NULL DEFAULT ''")
        self.conn.commit()

    def _ensure_column(self, table: str, column: str, definition: str) -> None:
        cols = self.conn.execute(f"PRAGMA table_info({table})").fetchall()
        names = {str(row["name"]) for row in cols}
        if column in names:
            return
        self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    def upsert_version(
        self,
        *,
        item_id: str,
        source: str,
        first_seen_at: str,
        fetched_at: str,
        published_at: str | None,
        title: str,
        content_type: str,
        text_hash: str,
        meta_hash: str,
        item_json_path: str,
        evidence_json_path: str,
        raw_root_path: str,
    ) -> VersionDecision:
        cur = self.conn.cursor()
        current = cur.execute(
            "SELECT latest_version, latest_text_hash, latest_meta_hash FROM items WHERE id = ?",
            (item_id,),
        ).fetchone()

        if current is None:
            version = "v1"
            cur.execute(
                """
                INSERT INTO items (id, source, first_seen_at, latest_version, latest_text_hash, latest_meta_hash)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (item_id, source, first_seen_at, version, text_hash, meta_hash),
            )
            cur.execute(
                """
                INSERT INTO item_versions (
                    id, version, fetched_at, published_at, title, content_type,
                    text_hash, meta_hash, item_json_path, evidence_json_path, raw_root_path
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    item_id,
                    version,
                    fetched_at,
                    published_at,
                    title,
                    content_type,
                    text_hash,
                    meta_hash,
                    item_json_path,
                    evidence_json_path,
                    raw_root_path,
                ),
            )
            self.conn.commit()
            return VersionDecision(version=version, is_new_item=True, is_new_version=True)

        current_version = str(current["latest_version"])
        current_text_hash = str(current["latest_text_hash"])
        current_meta_hash = str(current["latest_meta_hash"])

        if current_text_hash == text_hash and current_meta_hash == meta_hash:
            return VersionDecision(version=current_version, is_new_item=False, is_new_version=False)

        version_num = _parse_version(current_version) + 1
        next_version = f"v{version_num}"
        cur.execute(
            """
            UPDATE items
            SET latest_version = ?, latest_text_hash = ?, latest_meta_hash = ?
            WHERE id = ?
            """,
            (next_version, text_hash, meta_hash, item_id),
        )
        cur.execute(
            """
            INSERT INTO item_versions (
                id, version, fetched_at, published_at, title, content_type,
                text_hash, meta_hash, item_json_path, evidence_json_path, raw_root_path
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                item_id,
                next_version,
                fetched_at,
                published_at,
                title,
                content_type,
                text_hash,
                meta_hash,
                item_json_path,
                evidence_json_path,
                raw_root_path,
            ),
        )
        self.conn.commit()
        return VersionDecision(version=next_version, is_new_item=False, is_new_version=True)

    def exists_item(self, item_id: str) -> bool:
        row = self.conn.execute("SELECT 1 FROM items WHERE id = ? LIMIT 1", (item_id,)).fetchone()
        return row is not None

    def record_run_item(self, run_id: str, item_id: str, version: str | None, status: str, error_code: str | None = None) -> None:
        self.conn.execute(
            """
            INSERT OR REPLACE INTO run_items (run_id, id, version, status, error_code)
            VALUES (?, ?, ?, ?, ?)
            """,
            (run_id, item_id, version, status, error_code),
        )
        self.conn.commit()

    def upsert_daily_url_cache(
        self,
        *,
        date_key: str,
        normalized_url: str,
        item_id: str,
        version: str,
        source_type: str,
        source_name: str,
        source_group: str,
        item_json_path: str,
        evidence_json_path: str,
        raw_root_path: str,
        fetched_at: str,
        run_id: str,
    ) -> None:
        self.conn.execute(
            """
            INSERT OR REPLACE INTO daily_url_cache (
                date_key, normalized_url, item_id, version, source_type, source_name, source_group,
                item_json_path, evidence_json_path, raw_root_path, fetched_at, run_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                date_key,
                normalized_url,
                item_id,
                version,
                source_type,
                source_name,
                source_group,
                item_json_path,
                evidence_json_path,
                raw_root_path,
                fetched_at,
                run_id,
            ),
        )
        self.conn.commit()

    def get_daily_url_cache(self, date_key: str, normalized_url: str) -> DailyCacheRecord | None:
        row = self.conn.execute(
            """
            SELECT
                date_key, normalized_url, item_id, version, source_type, source_name, source_group,
                item_json_path, evidence_json_path, raw_root_path, fetched_at, run_id
            FROM daily_url_cache
            WHERE date_key = ? AND normalized_url = ?
            LIMIT 1
            """,
            (date_key, normalized_url),
        ).fetchone()
        if row is None:
            return None
        return DailyCacheRecord(
            date_key=str(row["date_key"]),
            normalized_url=str(row["normalized_url"]),
            item_id=str(row["item_id"]),
            version=str(row["version"]),
            source_type=str(row["source_type"]),
            source_name=str(row["source_name"] or ""),
            source_group=str(row["source_group"]),
            item_json_path=str(row["item_json_path"]),
            evidence_json_path=str(row["evidence_json_path"]),
            raw_root_path=str(row["raw_root_path"]),
            fetched_at=str(row["fetched_at"]),
            run_id=str(row["run_id"]),
        )

    def list_daily_cache(self, date_key: str) -> list[DailyCacheRecord]:
        rows = self.conn.execute(
            """
            SELECT
                date_key, normalized_url, item_id, version, source_type, source_name, source_group,
                item_json_path, evidence_json_path, raw_root_path, fetched_at, run_id
            FROM daily_url_cache
            WHERE date_key = ?
            ORDER BY fetched_at DESC
            """,
            (date_key,),
        ).fetchall()
        result: list[DailyCacheRecord] = []
        for row in rows:
            result.append(
                DailyCacheRecord(
                    date_key=str(row["date_key"]),
                    normalized_url=str(row["normalized_url"]),
                    item_id=str(row["item_id"]),
                    version=str(row["version"]),
                    source_type=str(row["source_type"]),
                    source_name=str(row["source_name"] or ""),
                    source_group=str(row["source_group"]),
                    item_json_path=str(row["item_json_path"]),
                    evidence_json_path=str(row["evidence_json_path"]),
                    raw_root_path=str(row["raw_root_path"]),
                    fetched_at=str(row["fetched_at"]),
                    run_id=str(row["run_id"]),
                )
            )
        return result

    def list_daily_cache_by_source(self, date_key: str, source_type: str) -> list[DailyCacheRecord]:
        rows = self.conn.execute(
            """
            SELECT
                date_key, normalized_url, item_id, version, source_type, source_name, source_group,
                item_json_path, evidence_json_path, raw_root_path, fetched_at, run_id
            FROM daily_url_cache
            WHERE date_key = ? AND source_type = ?
            ORDER BY fetched_at DESC
            """,
            (date_key, source_type),
        ).fetchall()
        result: list[DailyCacheRecord] = []
        for row in rows:
            result.append(
                DailyCacheRecord(
                    date_key=str(row["date_key"]),
                    normalized_url=str(row["normalized_url"]),
                    item_id=str(row["item_id"]),
                    version=str(row["version"]),
                    source_type=str(row["source_type"]),
                    source_name=str(row["source_name"] or ""),
                    source_group=str(row["source_group"]),
                    item_json_path=str(row["item_json_path"]),
                    evidence_json_path=str(row["evidence_json_path"]),
                    raw_root_path=str(row["raw_root_path"]),
                    fetched_at=str(row["fetched_at"]),
                    run_id=str(row["run_id"]),
                )
            )
        return result

    def list_daily_cache_by_source_name(self, date_key: str, source_name: str) -> list[DailyCacheRecord]:
        rows = self.conn.execute(
            """
            SELECT
                date_key, normalized_url, item_id, version, source_type, source_name, source_group,
                item_json_path, evidence_json_path, raw_root_path, fetched_at, run_id
            FROM daily_url_cache
            WHERE date_key = ? AND source_name = ?
            ORDER BY fetched_at DESC
            """,
            (date_key, source_name),
        ).fetchall()
        result: list[DailyCacheRecord] = []
        for row in rows:
            result.append(
                DailyCacheRecord(
                    date_key=str(row["date_key"]),
                    normalized_url=str(row["normalized_url"]),
                    item_id=str(row["item_id"]),
                    version=str(row["version"]),
                    source_type=str(row["source_type"]),
                    source_name=str(row["source_name"] or ""),
                    source_group=str(row["source_group"]),
                    item_json_path=str(row["item_json_path"]),
                    evidence_json_path=str(row["evidence_json_path"]),
                    raw_root_path=str(row["raw_root_path"]),
                    fetched_at=str(row["fetched_at"]),
                    run_id=str(row["run_id"]),
                )
            )
        return result

    def set_source_cooldown(self, *, source_group: str, blocked_until: str, reason: str, updated_at: str) -> None:
        self.conn.execute(
            """
            INSERT INTO source_cooldowns (source_group, blocked_until, reason, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(source_group) DO UPDATE SET
                blocked_until = excluded.blocked_until,
                reason = excluded.reason,
                updated_at = excluded.updated_at
            """,
            (source_group, blocked_until, reason, updated_at),
        )
        self.conn.commit()

    def get_source_cooldown(self, source_group: str) -> SourceCooldown | None:
        row = self.conn.execute(
            """
            SELECT source_group, blocked_until, reason, updated_at
            FROM source_cooldowns
            WHERE source_group = ?
            LIMIT 1
            """,
            (source_group,),
        ).fetchone()
        if row is None:
            return None
        return SourceCooldown(
            source_group=str(row["source_group"]),
            blocked_until=str(row["blocked_until"]),
            reason=str(row["reason"]),
            updated_at=str(row["updated_at"]),
        )

    def close(self) -> None:
        self.conn.close()


def _parse_version(value: str) -> int:
    if value.startswith("v"):
        value = value[1:]
    try:
        return int(value)
    except ValueError:
        return 1
