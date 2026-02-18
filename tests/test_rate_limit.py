from __future__ import annotations

import time
from datetime import datetime, timezone

import httpx

from infostream.config.models import RunConfig, SourceConfig, TimeoutsConfig, TranscribeConfig
from infostream.contracts.item import Entry, ItemDraft, RawPayload
from infostream.contracts.plugin import PluginCapabilities, SourcePlugin
from infostream.pipeline.orchestrator import run_pipeline
from infostream.plugins.registry import PluginRegistry
from infostream.storage.catalog_sqlite import CatalogSQLite


class RateLimitedGitHubPlugin(SourcePlugin):
    source_name = "github_search"
    supported_url_patterns = [r"^https?://github\.com/search.*$"]
    capabilities = PluginCapabilities(supports_discover=True, supports_transcribe=False, requires_auth=False)

    def __init__(self) -> None:
        super().__init__()
        self.discover_calls = 0
        self.fetch_calls = 0

    def discover(self, source_config, client, request_timeout_sec):
        self.discover_calls += 1
        return [
            Entry(url="https://github.com/a/b", source_name=self.source_name),
            Entry(url="https://github.com/c/d", source_name=self.source_name),
        ]

    def fetch(self, entry, client, request_timeout_sec):
        self.fetch_calls += 1
        request = httpx.Request("GET", entry.url)
        response = httpx.Response(
            403,
            request=request,
            headers={
                "X-RateLimit-Remaining": "0",
                "X-RateLimit-Reset": str(int(time.time()) + 3600),
            },
        )
        raise httpx.HTTPStatusError("403 rate limit exceeded", request=request, response=response)

    def extract(self, raw: RawPayload) -> ItemDraft:  # pragma: no cover
        raise AssertionError("extract should not be called in this test")

    def fingerprint(self, item: ItemDraft) -> str:  # pragma: no cover
        return "unused"

    def provenance(self, raw, item, content_hash, raw_hash):  # pragma: no cover
        raise AssertionError("provenance should not be called in this test")


def test_github_403_triggers_cooldown_and_breaks_source(tmp_path, monkeypatch):
    monkeypatch.delenv("DASHSCOPE_API_KEY", raising=False)
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)

    plugin = RateLimitedGitHubPlugin()
    registry = PluginRegistry()
    registry.register(plugin)
    sources = [
        SourceConfig(
            name="github_search_main",
            type="github_search",
            enabled=True,
            entry_urls=["https://github.com/search?q=llm"],
        )
    ]

    run_config = RunConfig(
        max_items=10,
        source_limits={},
        timezone="UTC+08:00",
        reuse_same_day=False,
        backfill_from_same_day_cache=False,
        rate_limit_break_on_403=True,
    )
    timeouts = TimeoutsConfig()
    transcribe = TranscribeConfig()

    first_meta = run_pipeline(
        sources=sources,
        run_config=run_config,
        timeouts=timeouts,
        transcribe_config=transcribe,
        output_root=tmp_path / "output",
        data_root=tmp_path / "data",
        registry=registry,
    )
    assert plugin.discover_calls == 1
    assert plugin.fetch_calls == 1
    assert first_meta["stats"]["failed_items"] == 1

    catalog = CatalogSQLite(tmp_path / "data" / "catalog.db")
    cooldown = catalog.get_source_cooldown("github")
    catalog.close()
    assert cooldown is not None
    blocked_until = datetime.fromisoformat(cooldown.blocked_until)
    if blocked_until.tzinfo is None:
        blocked_until = blocked_until.replace(tzinfo=timezone.utc)
    assert blocked_until > datetime.now(timezone.utc)

    second_meta = run_pipeline(
        sources=sources,
        run_config=run_config,
        timeouts=timeouts,
        transcribe_config=transcribe,
        output_root=tmp_path / "output",
        data_root=tmp_path / "data",
        registry=registry,
    )
    assert plugin.discover_calls == 1
    assert plugin.fetch_calls == 1
    assert second_meta["stats"]["sources_processed"] == 0
