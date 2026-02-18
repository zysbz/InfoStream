from __future__ import annotations

from collections import defaultdict
import hashlib
import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qsl, urlsplit

import httpx

from infostream.config.models import RunConfig, SourceConfig, TimeoutsConfig, TranscribeConfig
from infostream.contracts.item import Entry, Item
from infostream.digest.generator import generate_digest
from infostream.digest.llm_client import LLMClient
from infostream.logging.run_logger import RunLogger
from infostream.pipeline.router import route_source
from infostream.pipeline.transcribe import TranscribePolicy, Transcriber
from infostream.plugins.registry import PluginRegistry
from infostream.storage.archive_writer import ArchiveWriter, ItemWriteResult
from infostream.storage.catalog_sqlite import CatalogSQLite, DailyCacheRecord
from infostream.utils.timezone import date_key_for_timezone, parse_timezone
from infostream.utils.url_norm import normalize_url


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
    progress: Callable[[str, dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    run_tz = parse_timezone(run_config.timezone)
    run_started_at = datetime.now(run_tz)
    run_id = run_started_at.strftime("%Y%m%d_%H%M")
    date_key = date_key_for_timezone(run_started_at, run_tz)

    writer = ArchiveWriter(output_root=output_root, run_id=run_id)
    logger = RunLogger(writer.logs_dir, local_tz=run_tz)
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
        "reused_items": 0,
        "backfilled_items": 0,
        "failed_items": 0,
    }
    digest_candidates: list[tuple[Item, str]] = []
    digest_item_ids: set[str] = set()
    source_group_counts: dict[str, int] = defaultdict(int)
    source_name_counts: dict[str, int] = defaultdict(int)
    trending_source_limits = _build_github_trending_source_limits(sources, run_config)
    run_started_mono = time.monotonic()
    run_timed_out = False
    max_items_reached = False

    logger.info(
        "run_started",
        run_id=run_id,
        output_dir=str(writer.run_dir),
        sources_total=len(sources),
        timezone=run_config.timezone,
        date_key=date_key,
        max_items=run_config.max_items,
        source_limits=run_config.source_limits,
        trending_source_limits=trending_source_limits,
    )
    _emit_progress(
        progress,
        "run_started",
        run_id=run_id,
        sources_total=len(sources),
        max_items=run_config.max_items,
        timezone=run_config.timezone,
        trending_source_limits=trending_source_limits,
    )

    try:
        with httpx.Client(
            timeout=timeouts.request_timeout_sec,
            follow_redirects=True,
            headers={"User-Agent": "InfoStream/0.1"},
            trust_env=False,
        ) as client:
            for source in sources:
                if _is_run_timed_out(run_started_mono, timeouts.run_timeout_sec):
                    run_timed_out = True
                    logger.warning("run_timeout_reached", source=source.name)
                    _emit_progress(progress, "run_timeout_reached", source=source.name)
                    break
                if max_items_reached:
                    break
                if not source.enabled:
                    continue

                source_group = _source_group(source.type)
                source_limit = run_config.source_limits.get(source_group)
                source_name_limit = trending_source_limits.get(source.name)
                if _is_source_limit_reached(source_group_counts, source_group, source_limit):
                    logger.info(
                        "source_limit_skipped",
                        source=source.name,
                        source_group=source_group,
                        source_limit=source_limit,
                    )
                    _emit_progress(
                        progress,
                        "source_skipped_source_limit",
                        source=source.name,
                        source_group=source_group,
                        source_limit=source_limit,
                    )
                    continue
                if _is_source_limit_reached(source_name_counts, source.name, source_name_limit):
                    logger.info(
                        "source_name_limit_skipped",
                        source=source.name,
                        source_name_limit=source_name_limit,
                    )
                    _emit_progress(
                        progress,
                        "source_skipped_source_name_limit",
                        source=source.name,
                        source_name_limit=source_name_limit,
                    )
                    continue

                active_cooldown = _get_active_cooldown(catalog, source_group)
                if run_config.rate_limit_break_on_403 and active_cooldown is not None:
                    logger.info(
                        "source_cooldown_skipped",
                        source=source.name,
                        source_group=source_group,
                        blocked_until=active_cooldown.isoformat(),
                    )
                    _emit_progress(
                        progress,
                        "source_skipped_cooldown",
                        source=source.name,
                        source_group=source_group,
                        blocked_until=active_cooldown.isoformat(),
                    )
                    continue

                plugin = route_source(source, registry)
                _inject_source_auth(source)
                stats["sources_processed"] += 1
                source_start_mono = time.monotonic()
                source_timeout = source.timeout_sec or timeouts.source_timeout_sec

                logger.info(
                    "source_started",
                    source=source.name,
                    source_type=source.type,
                    source_group=source_group,
                    source_limit=source_limit,
                    source_name_limit=source_name_limit,
                )
                _emit_progress(
                    progress,
                    "source_started",
                    source=source.name,
                    source_type=source.type,
                    source_group=source_group,
                    source_limit=source_limit,
                    source_name_limit=source_name_limit,
                )

                if run_config.reuse_same_day and run_config.skip_discover_if_cached_same_day:
                    source_cache_records = catalog.list_daily_cache_by_source_name(date_key, source.name)
                    if source_cache_records:
                        logger.info(
                            "source_discover_skipped_same_day_cache",
                            source=source.name,
                            source_type=source.type,
                            cached_records=len(source_cache_records),
                        )
                        _emit_progress(
                            progress,
                            "source_discover_skipped_same_day_cache",
                            source=source.name,
                            source_type=source.type,
                            cached_records=len(source_cache_records),
                        )

                        for record in source_cache_records:
                            if max_items_reached:
                                break
                            if _is_source_limit_reached(source_group_counts, source_group, source_limit):
                                logger.info(
                                    "source_limit_reached",
                                    source=source.name,
                                    source_group=source_group,
                                    source_limit=source_limit,
                                )
                                break
                            if _is_source_limit_reached(source_name_counts, source.name, source_name_limit):
                                logger.info(
                                    "source_name_limit_reached",
                                    source=source.name,
                                    source_name_limit=source_name_limit,
                                )
                                break
                            if record.item_id in digest_item_ids:
                                continue

                            cached_item = _load_cached_item(record)
                            if cached_item is None:
                                continue

                            write_result = writer.write_reused_item(
                                cached_item,
                                reused_from_run_id=record.run_id,
                                reused_from_item_json_path=record.item_json_path,
                                reused_from_evidence_json_path=record.evidence_json_path,
                                reused_from_raw_root_path=record.raw_root_path,
                                reuse_date_key=date_key,
                            )
                            catalog.record_run_item(run_id, cached_item.id, cached_item.version, "reused_source_cache")
                            stats["items_success"] += 1
                            stats["reused_items"] += 1
                            stats["backfilled_items"] += 1

                            if _append_digest_candidate(
                                digest_candidates=digest_candidates,
                                digest_item_ids=digest_item_ids,
                                source_group_counts=source_group_counts,
                                source_name_counts=source_name_counts,
                                source_group=source_group,
                                source_name=source.name,
                                item=cached_item,
                                local_path=write_result.item_dir_relative,
                            ):
                                _emit_progress(
                                    progress,
                                    "item_reused",
                                    source=source.name,
                                    item_id=cached_item.id,
                                    candidates=len(digest_candidates),
                                )
                                if len(digest_candidates) >= run_config.max_items:
                                    max_items_reached = True
                                    logger.info("max_items_reached", max_items=run_config.max_items)
                                    _emit_progress(progress, "max_items_reached", max_items=run_config.max_items)
                                    break

                        logger.info("source_finished", source=source.name, source_type=source.type)
                        _emit_progress(
                            progress,
                            "source_finished",
                            source=source.name,
                            source_type=source.type,
                            entries=0,
                            candidates=len(digest_candidates),
                            reused=stats["reused_items"],
                        )
                        if run_timed_out or max_items_reached:
                            break
                        continue

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
                    _maybe_activate_source_cooldown(
                        run_config=run_config,
                        catalog=catalog,
                        logger=logger,
                        source_group=source_group,
                        source_name=source.name,
                        url="",
                        exc=exc,
                    )
                    continue

                stats["entries_discovered"] += len(entries)
                _emit_progress(
                    progress,
                    "source_discovered",
                    source=source.name,
                    source_type=source.type,
                    entries=len(entries),
                )

                for entry in entries:
                    if max_items_reached:
                        break
                    if _is_run_timed_out(run_started_mono, timeouts.run_timeout_sec):
                        run_timed_out = True
                        logger.warning("run_timeout_reached", source=source.name, entry=entry.url)
                        _emit_progress(progress, "run_timeout_reached", source=source.name, entry=entry.url)
                        break
                    if _is_source_limit_reached(source_group_counts, source_group, source_limit):
                        logger.info(
                            "source_limit_reached",
                            source=source.name,
                            source_group=source_group,
                            source_limit=source_limit,
                        )
                        break
                    if _is_source_limit_reached(source_name_counts, source.name, source_name_limit):
                        logger.info(
                            "source_name_limit_reached",
                            source=source.name,
                            source_name_limit=source_name_limit,
                        )
                        break
                    if time.monotonic() - source_start_mono > source_timeout:
                        logger.warning("source_timeout_reached", source=source.name, source_type=source.type)
                        break

                    stats["items_processed"] += 1
                    provisional_item_id = _hash_text(entry.url)
                    normalized_entry_url = normalize_url(entry.url)

                    if run_config.reuse_same_day:
                        cache_record = catalog.get_daily_url_cache(date_key, normalized_entry_url)
                        if cache_record is not None:
                            cached_item = _load_cached_item(cache_record)
                            if cached_item is not None:
                                provisional_item_id = cached_item.id
                                write_result = writer.write_reused_item(
                                    cached_item,
                                    reused_from_run_id=cache_record.run_id,
                                    reused_from_item_json_path=cache_record.item_json_path,
                                    reused_from_evidence_json_path=cache_record.evidence_json_path,
                                    reused_from_raw_root_path=cache_record.raw_root_path,
                                    reuse_date_key=date_key,
                                )
                                catalog.record_run_item(run_id, cached_item.id, cached_item.version, "reused")
                                stats["items_success"] += 1
                                stats["reused_items"] += 1

                                if _append_digest_candidate(
                                    digest_candidates=digest_candidates,
                                    digest_item_ids=digest_item_ids,
                                    source_group_counts=source_group_counts,
                                    source_name_counts=source_name_counts,
                                    source_group=source_group,
                                    source_name=source.name,
                                    item=cached_item,
                                    local_path=write_result.item_dir_relative,
                                ):
                                    if len(digest_candidates) >= run_config.max_items:
                                        max_items_reached = True
                                        logger.info("max_items_reached", max_items=run_config.max_items)
                                        _emit_progress(progress, "max_items_reached", max_items=run_config.max_items)
                                        break
                                    if _is_source_limit_reached(source_group_counts, source_group, source_limit):
                                        logger.info(
                                            "source_limit_reached",
                                            source=source.name,
                                            source_group=source_group,
                                            source_limit=source_limit,
                                        )
                                        break
                                    if _is_source_limit_reached(source_name_counts, source.name, source_name_limit):
                                        logger.info(
                                            "source_name_limit_reached",
                                            source=source.name,
                                            source_name_limit=source_name_limit,
                                        )
                                        break
                                continue

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

                        _record_daily_url_cache(
                            catalog=catalog,
                            date_key=date_key,
                            normalized_url=normalized_entry_url,
                            source_type=source.type,
                            source_name=source.name,
                            source_group=source_group,
                            run_id=run_id,
                            item=item,
                            write_result=write_result,
                        )
                        normalized_source_url = normalize_url(item.source_url)
                        if normalized_source_url and normalized_source_url != normalized_entry_url:
                            _record_daily_url_cache(
                                catalog=catalog,
                                date_key=date_key,
                                normalized_url=normalized_source_url,
                                source_type=source.type,
                                source_name=source.name,
                                source_group=source_group,
                                run_id=run_id,
                                item=item,
                                write_result=write_result,
                            )

                        if _append_digest_candidate(
                            digest_candidates=digest_candidates,
                            digest_item_ids=digest_item_ids,
                            source_group_counts=source_group_counts,
                            source_name_counts=source_name_counts,
                            source_group=source_group,
                            source_name=source.name,
                            item=item,
                            local_path=write_result.item_dir_relative,
                        ):
                            if len(digest_candidates) >= run_config.max_items:
                                max_items_reached = True
                                logger.info("max_items_reached", max_items=run_config.max_items)
                                _emit_progress(progress, "max_items_reached", max_items=run_config.max_items)
                                break
                            if _is_source_limit_reached(source_group_counts, source_group, source_limit):
                                logger.info(
                                    "source_limit_reached",
                                    source=source.name,
                                    source_group=source_group,
                                    source_limit=source_limit,
                                )
                                break
                            if _is_source_limit_reached(source_name_counts, source.name, source_name_limit):
                                logger.info(
                                    "source_name_limit_reached",
                                    source=source.name,
                                    source_name_limit=source_name_limit,
                                )
                                break
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
                        if _maybe_activate_source_cooldown(
                            run_config=run_config,
                            catalog=catalog,
                            logger=logger,
                            source_group=source_group,
                            source_name=source.name,
                            url=entry.url,
                            exc=exc,
                        ):
                            _emit_progress(
                                progress,
                                "source_rate_limited_break",
                                source=source.name,
                                source_group=source_group,
                            )
                            break

                logger.info("source_finished", source=source.name, source_type=source.type)
                _emit_progress(
                    progress,
                    "source_finished",
                    source=source.name,
                    source_type=source.type,
                    candidates=len(digest_candidates),
                    reused=stats["reused_items"],
                )
                if run_timed_out or max_items_reached:
                    break

        if not run_timed_out and run_config.backfill_from_same_day_cache and len(digest_candidates) < run_config.max_items:
            backfilled = _backfill_same_day_cache(
                catalog=catalog,
                writer=writer,
                logger=logger,
                run_config=run_config,
                run_id=run_id,
                date_key=date_key,
                digest_candidates=digest_candidates,
                digest_item_ids=digest_item_ids,
                source_group_counts=source_group_counts,
                source_name_counts=source_name_counts,
                source_name_limits=trending_source_limits,
                stats=stats,
            )
            if backfilled > 0 and len(digest_candidates) >= run_config.max_items:
                max_items_reached = True

        _emit_progress(
            progress,
            "digest_generating",
            candidates=len(digest_candidates),
            max_items=run_config.max_items,
        )
        digest_md, digest_json = generate_digest(digest_candidates, run_config, llm_client)
        writer.write_digest(digest_md, digest_json)

        run_meta = {
            "run_id": run_id,
            "started_at": run_started_at.isoformat(),
            "finished_at": datetime.now(run_tz).isoformat(),
            "date_key": date_key,
            "timezone": run_config.timezone,
            "timed_out": run_timed_out,
            "max_items_reached": max_items_reached,
            "source_group_counts": dict(source_group_counts),
            "source_name_counts": dict(source_name_counts),
            "trending_source_limits": trending_source_limits,
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
        logger.info(
            "run_finished",
            run_id=run_id,
            timed_out=run_timed_out,
            max_items_reached=max_items_reached,
            source_group_counts=dict(source_group_counts),
            source_name_counts=dict(source_name_counts),
            stats=stats,
        )
        _emit_progress(
            progress,
            "run_finished",
            run_id=run_id,
            timed_out=run_timed_out,
            max_items_reached=max_items_reached,
            stats=stats,
        )
        return run_meta
    finally:
        logger.flush_errors()
        catalog.close()


def _backfill_same_day_cache(
    *,
    catalog: CatalogSQLite,
    writer: ArchiveWriter,
    logger: RunLogger,
    run_config: RunConfig,
    run_id: str,
    date_key: str,
    digest_candidates: list[tuple[Item, str]],
    digest_item_ids: set[str],
    source_group_counts: dict[str, int],
    source_name_counts: dict[str, int],
    source_name_limits: dict[str, int],
    stats: dict[str, Any],
) -> int:
    backfilled = 0
    for record in catalog.list_daily_cache(date_key):
        if len(digest_candidates) >= run_config.max_items:
            break
        if record.item_id in digest_item_ids:
            continue

        source_group = record.source_group or _source_group(record.source_type)
        source_limit = run_config.source_limits.get(source_group)
        if _is_source_limit_reached(source_group_counts, source_group, source_limit):
            continue
        source_name = record.source_name or record.source_type
        source_name_limit = source_name_limits.get(source_name)
        if _is_source_limit_reached(source_name_counts, source_name, source_name_limit):
            continue

        item = _load_cached_item(record)
        if item is None:
            continue

        write_result = writer.write_reused_item(
            item,
            reused_from_run_id=record.run_id,
            reused_from_item_json_path=record.item_json_path,
            reused_from_evidence_json_path=record.evidence_json_path,
            reused_from_raw_root_path=record.raw_root_path,
            reuse_date_key=date_key,
        )
        catalog.record_run_item(run_id, item.id, item.version, "reused_backfill")
        stats["items_success"] += 1
        stats["reused_items"] += 1
        stats["backfilled_items"] += 1
        backfilled += 1

        _append_digest_candidate(
            digest_candidates=digest_candidates,
            digest_item_ids=digest_item_ids,
            source_group_counts=source_group_counts,
            source_name_counts=source_name_counts,
            source_group=source_group,
            source_name=source_name,
            item=item,
            local_path=write_result.item_dir_relative,
        )
        logger.info(
            "same_day_backfill_hit",
            item_id=item.id,
            source_group=source_group,
            reused_from_run_id=record.run_id,
        )

    return backfilled


def _load_cached_item(record: DailyCacheRecord) -> Item | None:
    path = Path(record.item_json_path)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        item = Item.model_validate(payload)
        item.version = record.version or item.version
        return item
    except Exception:
        return None


def _append_digest_candidate(
    *,
    digest_candidates: list[tuple[Item, str]],
    digest_item_ids: set[str],
    source_group_counts: dict[str, int],
    source_name_counts: dict[str, int],
    source_group: str,
    source_name: str,
    item: Item,
    local_path: str,
) -> bool:
    if item.id in digest_item_ids:
        return False
    digest_item_ids.add(item.id)
    digest_candidates.append((item, local_path))
    source_group_counts[source_group] += 1
    source_name_counts[source_name] += 1
    return True


def _record_daily_url_cache(
    *,
    catalog: CatalogSQLite,
    date_key: str,
    normalized_url: str,
    source_type: str,
    source_name: str,
    source_group: str,
    run_id: str,
    item: Item,
    write_result: ItemWriteResult,
) -> None:
    if not normalized_url:
        return
    catalog.upsert_daily_url_cache(
        date_key=date_key,
        normalized_url=normalized_url,
        item_id=item.id,
        version=item.version,
        source_type=source_type,
        source_name=source_name,
        source_group=source_group,
        item_json_path=str(write_result.item_json_path),
        evidence_json_path=str(write_result.evidence_json_path),
        raw_root_path=str(write_result.raw_root_path),
        fetched_at=item.fetched_at.isoformat(),
        run_id=run_id,
    )


def _maybe_activate_source_cooldown(
    *,
    run_config: RunConfig,
    catalog: CatalogSQLite,
    logger: RunLogger,
    source_group: str,
    source_name: str,
    url: str,
    exc: Exception,
) -> bool:
    if not run_config.rate_limit_break_on_403:
        return False
    if source_group != "github":
        return False
    if not _is_rate_limit_403(exc):
        return False

    now_utc = datetime.now(timezone.utc)
    blocked_until = _resolve_blocked_until(exc, now_utc)
    catalog.set_source_cooldown(
        source_group=source_group,
        blocked_until=blocked_until.isoformat(),
        reason="github_403_rate_limit",
        updated_at=now_utc.isoformat(),
    )
    logger.warning(
        "source_rate_limited",
        source=source_name,
        source_group=source_group,
        url=url,
        blocked_until=blocked_until.isoformat(),
    )
    return True


def _get_active_cooldown(catalog: CatalogSQLite, source_group: str) -> datetime | None:
    cooldown = catalog.get_source_cooldown(source_group)
    if cooldown is None:
        return None
    try:
        blocked_until = datetime.fromisoformat(cooldown.blocked_until)
    except ValueError:
        return None
    if blocked_until.tzinfo is None:
        blocked_until = blocked_until.replace(tzinfo=timezone.utc)
    blocked_until = blocked_until.astimezone(timezone.utc)
    if blocked_until > datetime.now(timezone.utc):
        return blocked_until
    return None


def _is_rate_limit_403(exc: Exception) -> bool:
    if not isinstance(exc, httpx.HTTPStatusError):
        return False
    response = exc.response
    if response is None or response.status_code != 403:
        return False
    remaining = str(response.headers.get("X-RateLimit-Remaining", "")).strip()
    return remaining == "0" or "rate limit" in str(exc).lower()


def _resolve_blocked_until(exc: Exception, now_utc: datetime) -> datetime:
    if isinstance(exc, httpx.HTTPStatusError) and exc.response is not None:
        response = exc.response
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                seconds = int(float(retry_after))
                if seconds > 0:
                    return now_utc + timedelta(seconds=seconds)
            except ValueError:
                pass
        reset_at = response.headers.get("X-RateLimit-Reset")
        if reset_at:
            try:
                epoch = int(float(reset_at))
                reset_dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
                if reset_dt > now_utc:
                    return reset_dt
            except ValueError:
                pass
    return now_utc + timedelta(minutes=15)


def _build_github_trending_source_limits(sources: list[SourceConfig], run_config: RunConfig) -> dict[str, int]:
    total = run_config.github_trending_total_limit
    if total is None:
        return {}

    trending_sources = [source for source in sources if source.enabled and source.type == "github_trending"]
    if not trending_sources:
        return {}

    bucket_to_names: dict[str, list[str]] = defaultdict(list)
    for source in trending_sources:
        bucket = _trending_bucket_for_source(source)
        bucket_to_names[bucket].append(source.name)

    expected = {"global_weekly", "zh_weekly", "global_daily", "zh_daily"}
    if expected.issubset(set(bucket_to_names)):
        base = total // 4
        remainder = total % 4
        bucket_quotas = {
            "global_weekly": base,
            "zh_weekly": base,
            "global_daily": base + remainder,
            "zh_daily": base,
        }
        limits: dict[str, int] = {}
        for bucket, quota in bucket_quotas.items():
            _distribute_quota_to_names(limits, bucket_to_names.get(bucket, []), quota)
        return limits

    names = [source.name for source in trending_sources]
    base = total // len(names)
    remainder = total % len(names)
    limits = {name: base for name in names}
    remainder_target = _pick_global_daily_source_name(bucket_to_names) or names[0]
    limits[remainder_target] += remainder
    return limits


def _distribute_quota_to_names(limits: dict[str, int], names: list[str], quota: int) -> None:
    if not names:
        return
    base = quota // len(names)
    remainder = quota % len(names)
    for index, name in enumerate(names):
        limits[name] = base + (1 if index < remainder else 0)


def _pick_global_daily_source_name(bucket_to_names: dict[str, list[str]]) -> str | None:
    names = bucket_to_names.get("global_daily") or []
    if names:
        return names[0]
    return None


def _trending_bucket_for_source(source: SourceConfig) -> str:
    since, spoken_language_code = _extract_trending_filters(source)
    period = "daily" if since == "daily" else "weekly"
    is_zh = spoken_language_code.lower() in {"zh", "zh-cn", "zh-hans", "zh-hant"}
    if period == "daily":
        return "zh_daily" if is_zh else "global_daily"
    return "zh_weekly" if is_zh else "global_weekly"


def _extract_trending_filters(source: SourceConfig) -> tuple[str, str]:
    since = str(source.params.get("since") or source.params.get("period") or "").strip().lower()
    spoken_language_code = str(source.params.get("spoken_language_code") or "").strip().lower()

    for entry_url in source.entry_urls:
        parts = urlsplit(entry_url)
        if parts.netloc.lower() not in {"github.com", "www.github.com"}:
            continue
        if not parts.path.lower().startswith("/trending"):
            continue
        for key, value in parse_qsl(parts.query, keep_blank_values=True):
            lowered_key = key.lower().strip()
            lowered_value = value.lower().strip()
            if lowered_key == "since" and not since:
                since = lowered_value
            if lowered_key == "spoken_language_code" and not spoken_language_code:
                spoken_language_code = lowered_value

    if since not in {"daily", "weekly", "monthly"}:
        since = "weekly"
    if since == "monthly":
        since = "weekly"
    return since, spoken_language_code


def _emit_progress(
    progress: Callable[[str, dict[str, Any]], None] | None,
    event: str,
    **payload: Any,
) -> None:
    if progress is None:
        return
    try:
        progress(event, payload)
    except Exception:
        return


def _inject_source_auth(source: SourceConfig) -> None:
    if source.type in {"github_search", "github_trending"}:
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


def _source_group(source_type: str) -> str:
    lowered = source_type.lower()
    if lowered.startswith("github"):
        return "github"
    if lowered.startswith("bilibili"):
        return "bilibili"
    return lowered


def _is_source_limit_reached(source_group_counts: dict[str, int], source_group: str, source_limit: int | None) -> bool:
    return source_limit is not None and source_group_counts[source_group] >= source_limit
