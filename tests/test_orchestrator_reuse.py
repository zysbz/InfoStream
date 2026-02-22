from __future__ import annotations

from pathlib import Path

from infostream.config.models import RunConfig, SourceConfig, TimeoutsConfig, TranscribeConfig
from infostream.contracts.item import Entry, Evidence, ItemDraft, RawPayload
from infostream.contracts.plugin import PluginCapabilities, SourcePlugin
from infostream.pipeline import orchestrator as orchestrator_module
from infostream.pipeline.orchestrator import _normalize_summary_markdown, run_pipeline
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


def test_run_pipeline_passes_llm_model_from_run_config(tmp_path, monkeypatch):
    monkeypatch.delenv("DASHSCOPE_API_KEY", raising=False)
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)

    captured: dict[str, str] = {}

    class _FakeLLMClient:
        def __init__(self, model: str = "deepseek-v3.2", api_key: str | None = None) -> None:
            captured["model"] = model

        def summarize_item(self, item, prompt_template, language):
            return {
                "one_liner": item.title,
                "bullets": ["a", "b", "c"],
                "why_it_matters": None,
            }

    monkeypatch.setattr(orchestrator_module, "LLMClient", _FakeLLMClient)

    plugin = FakeSourcePlugin()
    registry = PluginRegistry()
    registry.register(plugin)
    sources = [SourceConfig(name="fake", type="fake_source", enabled=True, entry_urls=["https://example.com/list"])]

    _ = run_pipeline(
        sources=sources,
        run_config=RunConfig(
            max_items=1,
            source_limits={},
            llm_model="qwen3.5-397b-a17b",
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

    assert captured["model"] == "qwen3.5-397b-a17b"


def test_run_pipeline_writes_summary_and_fixed_html(tmp_path, monkeypatch):
    monkeypatch.delenv("DASHSCOPE_API_KEY", raising=False)
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.setenv("INFOSTREAM_AUTO_OPEN_WEB", "1")

    (tmp_path / "\u7f51\u9875prompt.md").write_text("summarize to markdown", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    opened_targets: list[str] = []
    monkeypatch.setattr(orchestrator_module.webbrowser, "open", lambda target: opened_targets.append(target) or True)

    class _FakeLLMClient:
        def __init__(self, model: str = "deepseek-v3.2", api_key: str | None = None) -> None:
            self.model = model

        def summarize_item(self, item, prompt_template, language):
            return {
                "one_liner": item.title,
                "bullets": ["a", "b", "c"],
                "why_it_matters": None,
            }

        def summarize_markdown(self, markdown, prompt_template, language):
            assert "summarize" in prompt_template
            return "# Daily Tech Digest\n\n## Highlights\n\n1. **Test**: done\n"

    monkeypatch.setattr(orchestrator_module, "LLMClient", _FakeLLMClient)

    plugin = FakeSourcePlugin()
    registry = PluginRegistry()
    registry.register(plugin)
    sources = [SourceConfig(name="fake", type="fake_source", enabled=True, entry_urls=["https://example.com/list"])]

    meta = run_pipeline(
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

    summary_path = Path(meta["paths"]["summary_md"])
    web_html_path = Path(meta["paths"]["web_html"])

    assert summary_path.exists()
    assert summary_path.name == "summary.md"
    summary_text = summary_path.read_text(encoding="utf-8")
    assert "# 每日科技动态" in summary_text
    assert "**时间：" in summary_text
    assert "---" in summary_text
    assert "## 一、Highlights" in summary_text

    assert web_html_path.exists()
    assert web_html_path == tmp_path / "output" / "latest.html"
    html_text = web_html_path.read_text(encoding="utf-8")
    assert "<!doctype html>" in html_text.lower()
    assert meta["paths"]["web_opened"] is True
    assert len(opened_targets) == 1
    assert opened_targets[0].startswith("file:///")


def test_normalize_summary_markdown_splits_numbered_section_lines():
    raw_summary = (
        "时间：2026年02月22日\n\n"
        "1. 模型发布与技术升级\n"
        "1. **发布 A**：说明 A\n\n"
        "2. 企业应用案例\n"
        "1. **落地 B**：说明 B\n"
    )
    normalized = _normalize_summary_markdown(
        raw_summary,
        generated_at="2026-02-22T12:00:00+08:00",
        language="zh-CN",
    )

    assert "## 一、模型发布与技术升级" in normalized
    assert "## 二、企业应用案例" in normalized
    assert "## 一、今日要点" not in normalized
