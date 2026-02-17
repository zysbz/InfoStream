from __future__ import annotations

from infostream.config.models import SourceConfig
from infostream.plugins.registry import PluginRegistry


def route_source(source: SourceConfig, registry: PluginRegistry):
    plugin = registry.get(source.type)
    if plugin is None:
        raise ValueError(f"No plugin registered for source type: {source.type}")
    return plugin


def classify_url(url: str, registry: PluginRegistry) -> str | None:
    plugin = registry.match_url(url)
    if plugin is None:
        return None
    return plugin.source_name