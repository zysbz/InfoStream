from __future__ import annotations

import copy
import json
from pathlib import Path

import yaml

from infostream.config.models import RunConfig, SourceConfig, SourcesFileConfig, TimeoutsConfig


def load_sources_config(path: str | Path) -> SourcesFileConfig:
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"sources config not found: {config_path}")

    raw = yaml.safe_load(config_path.read_text(encoding="utf-8-sig")) or {}
    config = SourcesFileConfig.model_validate(raw)

    for source in config.sources:
        if source.type == "github_search":
            source.params = {
                "keywords": source.params.get("keywords", config.github_search.keywords),
                "sort": source.params.get("sort", config.github_search.sort),
                "order": source.params.get("order", config.github_search.order),
                **{k: v for k, v in source.params.items() if k not in {"keywords", "sort", "order"}},
            }

    return config


def load_run_config(path: str | Path | None) -> RunConfig:
    if path is None:
        return RunConfig()

    config_path = Path(path)
    if not config_path.exists():
        return RunConfig()

    raw = json.loads(config_path.read_text(encoding="utf-8-sig"))
    return RunConfig.model_validate(raw)


def load_timeouts_config(path: str | Path | None) -> TimeoutsConfig:
    if path is None:
        return TimeoutsConfig()

    config_path = Path(path)
    if not config_path.exists():
        return TimeoutsConfig()

    raw = yaml.safe_load(config_path.read_text(encoding="utf-8-sig")) or {}
    return TimeoutsConfig.model_validate(raw)


def merge_add_urls(
    sources_config: SourcesFileConfig,
    add_urls: list[str],
    registry: "PluginRegistry",
) -> tuple[list[SourceConfig], list[str]]:
    merged = [copy.deepcopy(source) for source in sources_config.sources if source.enabled]
    rejected: list[str] = []

    for index, url in enumerate(add_urls):
        plugin = registry.match_url(url)
        if plugin is None:
            rejected.append(url)
            continue

        merged.append(
            SourceConfig(
                name=f"adhoc_{plugin.source_name}_{index + 1}",
                type=plugin.source_name,
                enabled=True,
                entry_urls=[url],
                discover_depth=1,
                params={},
            )
        )

    return merged, rejected


from infostream.plugins.registry import PluginRegistry
