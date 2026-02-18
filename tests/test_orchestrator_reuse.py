from __future__ import annotations

from infostream.config.models import RunConfig, SourceConfig, TimeoutsConfig, TranscribeConfig
from infostream.contracts.item import Entry, Evidence, ItemDraft, RawPayload
from infostream.contracts.plugin import PluginCapabilities, SourcePlugin
from infostream.pipeline.orchestrator import run_pipeline
from infostream.plugins.registry import PluginRegistry


class FakeSourcePlugin(SourcePlugin):
    source_name = "fake_source"
    supported_url_patterns = [r"^https?://example\.com/.*$"]
    capabilities = PluginCapabilities(supports_discover=True, supports_transcribe=False, requires_auth=False)

    def __init__(self) -> None:
        super().__init__()
        self.discover_urls = ["https://example.com/repo/1"]
        self.fetch_calls = 0

    def discover(self, source_config, client, request_timeout_sec):
        return [Entry(url=url, source_name=self.source_name) for url in self.discover_urls]

    def fetch(self, entry, client, request_timeout_sec):
        self.fetch_calls += 1
        return RawPayload(
            entry_url=entry.url,
            source_name=self.source_name,
            content_type="json",
            payload={
                "full_name": "owner/repo",
                "title": "owner/repo",
                "description": "test repo",
                "html_url": "https://github.com/owner/repo",
            },
            status_code=200,
            headers={},
            final_url=entry.url,
            metadata={},
        )

    def extract(self, raw):
        payload = raw.payload if isinstance(raw.payload, dict) else {}
        return ItemDraft(
            source=self.source_name,
            source_url=str(payload.get("html_url") or raw.entry_url),
            title=str(payload.get("title") or "owner/repo"),
            fetched_at=raw.fetched_at,
            content_type="repo",
            text=str(payload.get("description") or ""),
            tags=["test"],
        )

    def fingerprint(self, item):
        return "owner/repo"

    def provenance(self, raw, item, content_hash, raw_hash):
        return Evidence(
            source_url=item.source_url,
            fetched_at=raw.fetched_at,
            content_hash=content_hash,
            raw_hash=raw_hash,
            request_context={},
            extract_hints={"plugin": self.source_name},
        )


def _build_run_config() -> RunConfig:
    return RunConfig(
        max_items=5,
        source_limits={},
        reuse_same_day=True,
        backfill_from_same_day_cache=True,
        timezone="UTC+08:00",
    )


def test_run_pipeline_reuses_same_day_url(tmp_path, monkeypatch):
    monkeypatch.delenv("DASHSCOPE_API_KEY", raising=False)
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)

    plugin = FakeSourcePlugin()
    registry = PluginRegistry()
    registry.register(plugin)
    sources = [SourceConfig(name="fake", type="fake_source", enabled=True, entry_urls=["https://example.com/list"])]

    first_meta = run_pipeline(
        sources=sources,
        run_config=_build_run_config(),
        timeouts=TimeoutsConfig(),
        transcribe_config=TranscribeConfig(),
        output_root=tmp_path / "output",
        data_root=tmp_path / "data",
        registry=registry,
    )
    assert first_meta["stats"]["items_success"] >= 1
    assert plugin.fetch_calls == 1

    second_meta = run_pipeline(
        sources=sources,
        run_config=_build_run_config(),
        timeouts=TimeoutsConfig(),
        transcribe_config=TranscribeConfig(),
        output_root=tmp_path / "output",
        data_root=tmp_path / "data",
        registry=registry,
    )
    assert plugin.fetch_calls == 1
    assert second_meta["stats"]["reused_items"] >= 1


def test_run_pipeline_backfills_from_same_day_cache(tmp_path, monkeypatch):
    monkeypatch.delenv("DASHSCOPE_API_KEY", raising=False)
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)

    plugin = FakeSourcePlugin()
    registry = PluginRegistry()
    registry.register(plugin)
    sources = [SourceConfig(name="fake", type="fake_source", enabled=True, entry_urls=["https://example.com/list"])]

    run_pipeline(
        sources=sources,
        run_config=_build_run_config(),
        timeouts=TimeoutsConfig(),
        transcribe_config=TranscribeConfig(),
        output_root=tmp_path / "output",
        data_root=tmp_path / "data",
        registry=registry,
    )
    assert plugin.fetch_calls == 1

    plugin.discover_urls = []
    second_meta = run_pipeline(
        sources=sources,
        run_config=RunConfig(
            max_items=1,
            source_limits={},
            reuse_same_day=True,
            backfill_from_same_day_cache=True,
            timezone="UTC+08:00",
        ),
        timeouts=TimeoutsConfig(),
        transcribe_config=TranscribeConfig(),
        output_root=tmp_path / "output",
        data_root=tmp_path / "data",
        registry=registry,
    )
    assert second_meta["stats"]["backfilled_items"] >= 1
