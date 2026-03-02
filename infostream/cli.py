from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from pathlib import Path

from infostream.config.loader import load_run_config, load_sources_config, load_timeouts_config, merge_add_urls
from infostream.config.models import RunConfig, SourceConfig
from infostream.pipeline.orchestrator import run_pipeline
from infostream.plugins.registry import build_default_registry
from infostream.utils.url_norm import normalize_url


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    _load_dotenv(Path(".env"))

    if args.command == "list-plugins":
        registry = build_default_registry()
        print(json.dumps(registry.list_plugins(), ensure_ascii=False, indent=2))
        return 0

    if args.command == "validate-config":
        return _validate_config(args.sources, args.run_config)

    if args.command == "run":
        return _run(args)

    parser.print_help()
    return 1


def _run(args: argparse.Namespace) -> int:
    registry = build_default_registry()
    sources_config = load_sources_config(args.sources)
    run_config = load_run_config(args.run_config)
    if args.max_items is not None:
        run_config = RunConfig.model_validate({**run_config.model_dump(mode="json"), "max_items": args.max_items})
    timeouts = load_timeouts_config(args.timeouts)

    merged_sources, rejected_add_urls = merge_add_urls(sources_config, args.add_url or [], registry)
    _validate_sources_with_registry(merged_sources, registry)
    _validate_source_url_limits(merged_sources, run_config)

    progress = None if args.no_progress else _build_progress_printer()

    run_meta = run_pipeline(
        sources=merged_sources,
        run_config=run_config,
        timeouts=timeouts,
        transcribe_config=sources_config.transcribe,
        output_root=Path(args.output_root),
        data_root=Path(args.data_root),
        registry=registry,
        rejected_add_urls=rejected_add_urls,
        progress=progress,
    )

    print(
        json.dumps(
            {
                "run_id": run_meta["run_id"],
                "timed_out": run_meta["timed_out"],
                "max_items_reached": run_meta["max_items_reached"],
                "source_group_counts": run_meta.get("source_group_counts", {}),
                "source_name_counts": run_meta.get("source_name_counts", {}),
                "source_url_counts": run_meta.get("source_url_counts", {}),
                "source_name_limits": run_meta.get("source_name_limits", {}),
                "source_url_limits": run_meta.get("source_url_limits", {}),
                "trending_source_limits": run_meta.get("trending_source_limits", {}),
                "stats": run_meta["stats"],
                "output_dir": run_meta["paths"]["run_dir"],
                "digest_md": run_meta["paths"]["digest_md"],
                "digest_json": run_meta["paths"]["digest_json"],
                "summary_md": run_meta["paths"].get("summary_md", ""),
                "web_html": run_meta["paths"].get("web_html", ""),
                "web_opened": run_meta["paths"].get("web_opened", False),
                "errors_json": run_meta["paths"]["errors_json"],
                "rejected_add_urls": run_meta["rejected_add_urls"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def _validate_config(sources_path: str, run_config_path: str | None) -> int:
    try:
        registry = build_default_registry()
        sources_config = load_sources_config(sources_path)
        run_config = load_run_config(run_config_path)
        _validate_sources_with_registry(sources_config.sources, registry)
        _validate_source_url_limits(sources_config.sources, run_config)
    except Exception as exc:
        print(f"config validation failed: {exc}")
        return 2

    print("config validation passed")
    return 0


def _validate_sources_with_registry(sources, registry) -> None:
    for source in sources:
        plugin = registry.get(source.type)
        if plugin is None:
            raise ValueError(f"source '{source.name}' uses unknown plugin type '{source.type}'")

        for url in source.entry_urls:
            matched = registry.match_url(url)
            if matched is None:
                raise ValueError(f"source '{source.name}' URL has no matching plugin pattern: {url}")
            if matched.source_name != source.type:
                raise ValueError(
                    f"source '{source.name}' URL '{url}' matches '{matched.source_name}', not declared '{source.type}'"
                )


def _validate_source_url_limits(sources: list[SourceConfig], run_config: RunConfig) -> None:
    required: dict[str, list[str]] = {}
    for source in sources:
        if not source.enabled:
            continue
        for url in source.entry_urls:
            normalized = normalize_url(url)
            if not normalized:
                continue
            required.setdefault(normalized, []).append(f"{source.name}: {url}")

    if not required:
        return

    configured = run_config.source_url_limits
    missing = [required[key][0] for key in sorted(required.keys()) if key not in configured]
    if missing:
        preview = ", ".join(missing[:5])
        more = "" if len(missing) <= 5 else f" ... (+{len(missing) - 5} more)"
        raise ValueError(
            "missing source_url_limits for entry_urls; each source URL must define a quota. "
            f"missing={preview}{more}"
        )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="InfoStream CLI")
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run", help="execute one ingestion and digest run")
    run_parser.add_argument("--sources", default="configs/sources.yaml")
    run_parser.add_argument("--run-config", default="configs/run_config.json")
    run_parser.add_argument("--timeouts", default="configs/timeouts.yaml")
    run_parser.add_argument("--output-root", default="output")
    run_parser.add_argument("--data-root", default="data")
    run_parser.add_argument("--add-url", action="append", default=[])
    run_parser.add_argument("--max-items", type=int, help="Maximum items to summarize (1-200)")
    run_parser.add_argument("--no-progress", action="store_true", help="Disable runtime progress messages")

    validate_parser = subparsers.add_parser("validate-config", help="validate source and run configs")
    validate_parser.add_argument("--sources", default="configs/sources.yaml")
    validate_parser.add_argument("--run-config", default="configs/run_config.json")

    subparsers.add_parser("list-plugins", help="list registered plugins")
    return parser


def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return

    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _build_progress_printer():
    def _print(event: str, payload: dict[str, object]) -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        if event == "run_started":
            print(f"[{ts}] run_started run_id={payload.get('run_id')} sources={payload.get('sources_total')} max_items={payload.get('max_items')}")
            limits = payload.get("trending_source_limits")
            if isinstance(limits, dict) and limits:
                print(f"[{ts}] trending_limits {json.dumps(limits, ensure_ascii=False)}")
            return
        if event == "source_started":
            print(f"[{ts}] source_started {payload.get('source')} ({payload.get('source_type')})")
            return
        if event == "source_discovered":
            print(f"[{ts}] source_discovered {payload.get('source')} entries={payload.get('entries')}")
            return
        if event == "source_discover_skipped_same_day_cache":
            print(f"[{ts}] source_cache_reuse {payload.get('source')} cached={payload.get('cached_records')}")
            return
        if event == "source_skipped_cooldown":
            print(f"[{ts}] source_skipped_cooldown {payload.get('source')} until={payload.get('blocked_until')}")
            return
        if event == "source_skipped_source_limit":
            print(f"[{ts}] source_skipped_group_limit {payload.get('source')} limit={payload.get('source_limit')}")
            return
        if event == "source_skipped_source_name_limit":
            print(f"[{ts}] source_skipped_name_limit {payload.get('source')} limit={payload.get('source_name_limit')}")
            return
        if event == "source_skipped_source_url_limit":
            print(
                f"[{ts}] source_skipped_url_limit {payload.get('source')} "
                f"url={payload.get('source_url')} limit={payload.get('source_url_limit')}"
            )
            return
        if event == "source_finished":
            print(f"[{ts}] source_finished {payload.get('source')} candidates={payload.get('candidates')}")
            return
        if event == "max_items_reached":
            print(f"[{ts}] max_items_reached {payload.get('max_items')}")
            return
        if event == "digest_generating":
            print(f"[{ts}] digest_generating candidates={payload.get('candidates')}")
            return
        if event == "run_timeout_reached":
            print(f"[{ts}] run_timeout_reached")
            return
        if event == "run_finished":
            print(f"[{ts}] run_finished run_id={payload.get('run_id')}")

    return _print
