from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from infostream.config.loader import load_run_config, load_sources_config, load_timeouts_config, merge_add_urls
from infostream.pipeline.orchestrator import run_pipeline
from infostream.plugins.registry import build_default_registry


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
    timeouts = load_timeouts_config(args.timeouts)

    merged_sources, rejected_add_urls = merge_add_urls(sources_config, args.add_url or [], registry)
    _validate_sources_with_registry(merged_sources, registry)

    run_meta = run_pipeline(
        sources=merged_sources,
        run_config=run_config,
        timeouts=timeouts,
        transcribe_config=sources_config.transcribe,
        output_root=Path(args.output_root),
        data_root=Path(args.data_root),
        registry=registry,
        rejected_add_urls=rejected_add_urls,
    )

    print(
        json.dumps(
            {
                "run_id": run_meta["run_id"],
                "timed_out": run_meta["timed_out"],
                "stats": run_meta["stats"],
                "output_dir": run_meta["paths"]["run_dir"],
                "digest_md": run_meta["paths"]["digest_md"],
                "digest_json": run_meta["paths"]["digest_json"],
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
        _ = load_run_config(run_config_path)
        _validate_sources_with_registry(sources_config.sources, registry)
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