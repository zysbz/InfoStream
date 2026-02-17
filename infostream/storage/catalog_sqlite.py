from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path


@dataclass
class VersionDecision:
    version: str
    is_new_item: bool
    is_new_version: bool


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
            """
        )
        self.conn.commit()

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

    def close(self) -> None:
        self.conn.close()


def _parse_version(value: str) -> int:
    if value.startswith("v"):
        value = value[1:]
    try:
        return int(value)
    except ValueError:
        return 1
