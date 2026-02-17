from __future__ import annotations

import re
from dataclasses import dataclass

from infostream.contracts.plugin import SourcePlugin
from infostream.plugins.bilibili_up import BilibiliUpPlugin
from infostream.plugins.github_search import GitHubSearchPlugin
from infostream.plugins.github_trending import GitHubTrendingPlugin
from infostream.plugins.rss_atom import RSSAtomPlugin


@dataclass
class PluginMatch:
    plugin: SourcePlugin
    pattern: str


class PluginRegistry:
    def __init__(self) -> None:
        self._plugins: dict[str, SourcePlugin] = {}

    def register(self, plugin: SourcePlugin) -> None:
        self._plugins[plugin.source_name] = plugin

    def get(self, source_type: str) -> SourcePlugin | None:
        return self._plugins.get(source_type.lower())

    def match_url(self, url: str) -> SourcePlugin | None:
        for plugin in self._plugins.values():
            for pattern in plugin.supported_url_patterns:
                if re.match(pattern, url, flags=re.IGNORECASE):
                    return plugin
        return None

    def list_plugins(self) -> list[dict[str, object]]:
        rows: list[dict[str, object]] = []
        for plugin in self._plugins.values():
            rows.append(
                {
                    "source_name": plugin.source_name,
                    "supported_url_patterns": plugin.supported_url_patterns,
                    "capabilities": plugin.capabilities.model_dump(),
                }
            )
        return sorted(rows, key=lambda row: str(row["source_name"]))


def build_default_registry() -> PluginRegistry:
    registry = PluginRegistry()
    registry.register(GitHubTrendingPlugin())
    registry.register(GitHubSearchPlugin())
    registry.register(RSSAtomPlugin())
    registry.register(BilibiliUpPlugin())
    return registry