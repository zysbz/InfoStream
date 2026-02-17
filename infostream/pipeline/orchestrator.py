from __future__ import annotations

import hashlib
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

from infostream.config.models import RunConfig, SourceConfig, TimeoutsConfig, TranscribeConfig
from infostream.contracts.item import Entry, Item
from infostream.digest.generator import generate_digest
from infostream.digest.llm_client import LLMClient
from infostream.logging.run_logger import RunLogger
from infostream.pipeline.router import route_source
from infostream.pipeline.transcribe import TranscribePolicy, Transcriber
from infostream.plugins.registry import PluginRegistry
from infostream.storage.archive_writer import ArchiveWriter
from infostream.storage.catalog_sqlite import CatalogSQLite


def run_pipeline(
    *,
    sources: list[SourceConfig],
    run_config: RunConfig,
    timeouts: TimeoutsConfig,
    transcribe_config: TranscribeConfig,
    output_root: Path,
    data_root: Path,
    registry: PluginRegistry,
    rejected_add_urls: list[str] | None = None,
) -> dict[str, Any]:
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    writer = ArchiveWriter(output_root=output_root, run_id=run_id)
    logger = RunLogger(writer.logs_dir)
    catalog = CatalogSQLite(data_root / "catalog.db")

    transcriber = Transcriber(
        TranscribePolicy(
            enabled_domains={domain.lower() for domain in transcribe_config.enabled_domains},
            transcribe_since=transcribe_config.transcribe_since,
        )
    )
    llm_client = LLMClient()

    stats: dict[str, Any] = {
        "sources_total": len(sources),
        "sources_processed": 0,
        "entries_discovered": 0,
        "items_processed": 0,
        "items_success": 0,
        "new_items": 0,
        "new_versions": 0,
        "unchanged_items": 0,
        "failed_items": 0,
    }
    digest_candidates: list[tuple[Item, str]] = []
    run_started_at = datetime.now(timezone.utc)
    run_started_mono = time.monotonic()
    run_timed_out = False

    logger.info("run_started", run_id=run_id, output_dir=str(writer.run_dir), sources_total=len(sources))

    try:
        with httpx.Client(
            timeout=timeouts.request_timeout_sec,
            follow_redirects=True,
            headers={"User-Agent": "InfoStream/0.1"},
        ) as client:
            for source in sources:
                if _is_run_timed_out(run_started_mono, timeouts.run_timeout_sec):
                    run_timed_out = True
                    logger.warning("run_timeout_reached", source=source.name)
                    break

                if not source.enabled:
                    continue

                plugin = route_source(source, registry)
                _inject_source_auth(source)
                stats["sources_processed"] += 1
                source_start_mono = time.monotonic()
                source_timeout = source.timeout_sec or timeouts.source_timeout_sec

                logger.info("source_started", source=source.name, source_type=source.type)

                try:
                    entries = (
                        plugin.discover(source, client, timeouts.request_timeout_sec)
                        if plugin.capabilities.supports_discover
                        else [Entry(url=url, source_name=source.type) for url in source.entry_urls]
                    )
                except Exception as exc:
                    logger.error(
                        stage="discover",
                        source=source.type,
                        url="",
                        error_type=exc.__class__.__name__,
                        message=str(exc),
                        exc=exc,
                    )
                    continue

                stats["entries_discovered"] += len(entries)

                for entry in entries:
                    if _is_run_timed_out(run_started_mono, timeouts.run_timeout_sec):
                        run_timed_out = True
                        logger.warning("run_timeout_reached", source=source.name, entry=entry.url)
                        break

                    if time.monotonic() - source_start_mono > source_timeout:
                        logger.warning("source_timeout_reached", source=source.name, source_type=source.type)
                        break

                    stats["items_processed"] += 1
                    provisional_item_id = _hash_text(entry.url)

                    try:
                        raw = plugin.fetch(entry, client, timeouts.request_timeout_sec)
                        draft = plugin.extract(raw)
                        item_id = plugin.fingerprint(draft)
                        provisional_item_id = item_id

                        raw_hash = _hash_any(raw.payload)
                        content_hash = _hash_text(draft.text)

                        item = Item(
                            id=item_id,
                            version="v0",
                            source=draft.source,
                            source_url=draft.source_url,
                            title=draft.title,
                            published_at=draft.published_at,
                            fetched_at=draft.fetched_at,
                            content_type=draft.content_type,
                            text=draft.text,
                            tags=draft.tags,
                            evidence=plugin.provenance(raw, draft, content_hash, raw_hash),
                            raw_refs=[],
                        )

                        is_new_candidate = not catalog.exists_item(item.id)
                        if transcriber.should_transcribe(item, is_new_candidate):
                            item.text = transcriber.transcribe(item)
                            item.evidence.content_hash = _hash_text(item.text)

                        meta_hash = _hash_meta(item)
                        write_result = writer.write_item(item, raw)
                        decision = catalog.upsert_version(
                            item_id=item.id,
                            source=item.source,
                            first_seen_at=item.fetched_at.isoformat(),
                            fetched_at=item.fetched_at.isoformat(),
                            published_at=item.published_at.isoformat() if item.published_at else None,
                            title=item.title,
                            content_type=item.content_type,
                            text_hash=item.evidence.content_hash,
                            meta_hash=meta_hash,
                            item_json_path=str(write_result.item_json_path),
                            evidence_json_path=str(write_result.evidence_json_path),
                            raw_root_path=str(write_result.raw_root_path),
                        )

                        item.version = decision.version
                        writer.rewrite_meta(item, write_result.item_json_path)

                        if decision.is_new_item:
                            status = "new"
                            stats["new_items"] += 1
                        elif decision.is_new_version:
                            status = "updated"
                            stats["new_versions"] += 1
                        else:
                            status = "unchanged"
                            stats["unchanged_items"] += 1

                        catalog.record_run_item(run_id, item.id, item.version, status)
                        stats["items_success"] += 1

                        if decision.is_new_item or decision.is_new_version:
                            digest_candidates.append((item, write_result.item_dir_relative))
                    except Exception as exc:
                        stats["failed_items"] += 1
                        logger.error(
                            stage="item_process",
                            source=source.type,
                            url=entry.url,
                            error_type=exc.__class__.__name__,
                            message=str(exc),
                            exc=exc,
                        )
                        catalog.record_run_item(
                            run_id,
                            provisional_item_id,
                            None,
                            "error",
                            error_code=exc.__class__.__name__,
                        )

                logger.info("source_finished", source=source.name, source_type=source.type)

                if run_timed_out:
                    break

        digest_md, digest_json = generate_digest(digest_candidates, run_config, llm_client)
        writer.write_digest(digest_md, digest_json)

        run_meta = {
            "run_id": run_id,
            "started_at": run_started_at.isoformat(),
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "timed_out": run_timed_out,
            "stats": stats,
            "rejected_add_urls": rejected_add_urls or [],
            "paths": {
                "run_dir": str(writer.run_dir),
                "digest_md": str(writer.run_dir / "digest.md"),
                "digest_json": str(writer.run_dir / "digest.json"),
                "errors_json": str(writer.logs_dir / "errors.json"),
            },
            "run_config": run_config.model_dump(mode="json"),
            "sources_snapshot": [source.model_dump(mode="json") for source in sources],
        }
        writer.write_run_meta(run_meta)
        logger.info("run_finished", run_id=run_id, stats=stats, timed_out=run_timed_out)
        return run_meta
    finally:
        logger.flush_errors()
        catalog.close()


def _inject_source_auth(source: SourceConfig) -> None:
    if source.type == "github_search":
        token = os.getenv("GITHUB_TOKEN")
        if token and "github_token" not in source.params:
            source.params["github_token"] = token


def _hash_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _hash_any(payload: Any) -> str:
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return _hash_text(canonical)


def _hash_meta(item: Item) -> str:
    payload = {
        "title": item.title,
        "published_at": item.published_at.isoformat() if item.published_at else None,
        "source_url": item.source_url,
        "content_type": item.content_type,
        "tags": sorted(item.tags),
    }
    return _hash_any(payload)


def _is_run_timed_out(started_mono: float, timeout_sec: int) -> bool:
    return time.monotonic() - started_mono > timeout_sec
