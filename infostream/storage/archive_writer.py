from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from infostream.contracts.item import Item, RawPayload
from infostream.storage.path_rules import build_item_dir_name, short_hash


@dataclass
class ItemWriteResult:
    item_dir: Path
    item_dir_relative: str
    item_json_path: Path
    evidence_json_path: Path
    raw_root_path: Path


class ArchiveWriter:
    def __init__(self, output_root: Path, run_id: str) -> None:
        self.output_root = output_root
        self.run_id = run_id
        self.run_dir = self.output_root / run_id
        self.items_dir = self.run_dir / "items"
        self.raw_dir = self.run_dir / "raw"
        self.logs_dir = self.run_dir / "logs"

        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.items_dir.mkdir(parents=True, exist_ok=True)
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)

    def write_item(self, item: Item, raw: RawPayload) -> ItemWriteResult:
        item_dir_name = build_item_dir_name(item.source, item.title, item.id)
        item_dir = self.items_dir / item_dir_name
        if item_dir.exists():
            item_dir = self.items_dir / build_item_dir_name(item.source, item.title, item.id, suffix=short_hash(item.source_url))

        item_dir.mkdir(parents=True, exist_ok=True)
        item_raw_dir = item_dir / "raw"
        item_raw_dir.mkdir(exist_ok=True)

        run_raw_file = self._write_raw_file(raw, self.raw_dir, prefix=item.id)
        item_raw_file = self._write_raw_file(raw, item_raw_dir, prefix="raw")

        item.raw_refs = [
            str(run_raw_file.relative_to(self.run_dir)).replace("\\", "/"),
            str(item_raw_file.relative_to(self.run_dir)).replace("\\", "/"),
        ]

        content_path = item_dir / "content.txt"
        content_path.write_text(item.text, encoding="utf-8")

        evidence_path = item_dir / "evidence.json"
        self.rewrite_evidence(item, evidence_path)

        meta_path = item_dir / "meta.json"
        self.rewrite_meta(item, meta_path)

        return ItemWriteResult(
            item_dir=item_dir,
            item_dir_relative=str(item_dir.relative_to(self.run_dir)).replace("\\", "/"),
            item_json_path=meta_path,
            evidence_json_path=evidence_path,
            raw_root_path=item_raw_dir,
        )

    def rewrite_meta(self, item: Item, meta_path: Path) -> None:
        meta_path.write_text(json.dumps(item.model_dump(mode="json"), ensure_ascii=False, indent=2), encoding="utf-8")

    def rewrite_evidence(self, item: Item, evidence_path: Path) -> None:
        evidence_path.write_text(json.dumps(item.evidence.model_dump(mode="json"), ensure_ascii=False, indent=2), encoding="utf-8")

    def write_digest(self, digest_md: str, digest_json: dict[str, Any]) -> None:
        (self.run_dir / "digest.md").write_text(digest_md, encoding="utf-8")
        (self.run_dir / "digest.json").write_text(json.dumps(digest_json, ensure_ascii=False, indent=2), encoding="utf-8")

    def write_run_meta(self, meta: dict[str, Any]) -> None:
        (self.run_dir / "run_meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    def _write_raw_file(self, raw: RawPayload, folder: Path, prefix: str) -> Path:
        payload = raw.payload
        if isinstance(payload, (dict, list)):
            path = folder / f"{short_hash(prefix)}.json"
            path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
            return path

        path = folder / f"{short_hash(prefix)}.txt"
        path.write_text(str(payload), encoding="utf-8")
        return path
