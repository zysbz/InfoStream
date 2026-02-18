from __future__ import annotations

from datetime import datetime, timezone

from infostream.config.models import RunConfig, SourceConfig, TimeoutsConfig, TranscribeConfig
from infostream.contracts.item import Entry, Evidence, ItemDraft, RawPayload
from infostream.contracts.plugin import PluginCapabilities, SourcePlugin
from infostream.pipeline import orchestrator as orchestrator_module
from infostream.pipeline.orchestrator import run_pipeline
from infostream.plugins.registry import PluginRegistry
from infostream.storage.catalog_sqlite import CatalogSQLite
from infostream.utils.timezone import date_key_for_timezone, parse_timezone


class BoundaryFakePlugin(SourcePlugin):
    source_name = "fake_source"
    supported_url_patterns = [r"^https?://example\.com/.*$"]
    capabilities = PluginCapabilities(supports_discover=True, supports_transcribe=False, requires_auth=False)

    def __init__(self) -> None:
        super().__init__()
        self.fetch_calls = 0

    def discover(self, source_config, client, request_timeout_sec):
        return [Entry(url="https://example.com/repo/1", source_name=self.source_name)]

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


def test_timezone_date_key_boundary_utc_plus_8():
    tz = parse_timezone("UTC+08:00")
    dt_0058 = datetime(2026, 2, 17, 16, 58, tzinfo=timezone.utc)  # 2026-02-18 00:58 +08:00
    dt_0120 = datetime(2026, 2, 17, 17, 20, tzinfo=timezone.utc)  # 2026-02-18 01:20 +08:00
    dt_next_day = datetime(2026, 2, 18, 16, 1, tzinfo=timezone.utc)  # 2026-02-19 00:01 +08:00

    assert date_key_for_timezone(dt_0058, tz) == "2026-02-18"
    assert date_key_for_timezone(dt_0120, tz) == "2026-02-18"
    assert date_key_for_timezone(dt_next_day, tz) == "2026-02-19"


def test_same_day_reuse_hits_and_cross_day_misses(tmp_path, monkeypatch):
    monkeypatch.delenv("DASHSCOPE_API_KEY", raising=False)
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)

    plugin = BoundaryFakePlugin()
    registry = PluginRegistry()
    registry.register(plugin)
    sources = [SourceConfig(name="fake", type="fake_source", enabled=True, entry_urls=["https://example.com/list"])]

    run_config = RunConfig(
        max_items=5,
        source_limits={},
        reuse_same_day=True,
        backfill_from_same_day_cache=False,
        timezone="UTC+08:00",
    )
    timeouts = TimeoutsConfig()
    transcribe = TranscribeConfig()

    monkeypatch.setattr(orchestrator_module, "date_key_for_timezone", lambda value, tz: "2026-02-18")
    run_pipeline(
        sources=sources,
        run_config=run_config,
        timeouts=timeouts,
        transcribe_config=transcribe,
        output_root=tmp_path / "output",
        data_root=tmp_path / "data",
        registry=registry,
    )
    assert plugin.fetch_calls == 1

    run_pipeline(
        sources=sources,
        run_config=run_config,
        timeouts=timeouts,
        transcribe_config=transcribe,
        output_root=tmp_path / "output",
        data_root=tmp_path / "data",
        registry=registry,
    )
    assert plugin.fetch_calls == 1

    monkeypatch.setattr(orchestrator_module, "date_key_for_timezone", lambda value, tz: "2026-02-19")
    run_pipeline(
        sources=sources,
        run_config=run_config,
        timeouts=timeouts,
        transcribe_config=transcribe,
        output_root=tmp_path / "output",
        data_root=tmp_path / "data",
        registry=registry,
    )
    assert plugin.fetch_calls == 2

    catalog = CatalogSQLite(tmp_path / "data" / "catalog.db")
    assert catalog.get_daily_url_cache("2026-02-18", "https://example.com/repo/1") is not None
    assert catalog.get_daily_url_cache("2026-02-19", "https://example.com/repo/1") is not None
    catalog.close()
