from datetime import datetime, timezone
from types import SimpleNamespace

import httpx

from infostream.contracts.item import Evidence, Item
from infostream.digest import llm_client as llm_module


def _build_item() -> Item:
    return Item(
        id="owner/repo",
        version="v1",
        source="github_search",
        source_url="https://github.com/owner/repo",
        title="owner/repo",
        published_at=None,
        fetched_at=datetime.now(timezone.utc),
        content_type="repo",
        text="A useful repository for LLM inference serving.",
        tags=["llm", "inference"],
        evidence=Evidence(
            source_url="https://github.com/owner/repo",
            fetched_at=datetime.now(timezone.utc),
            content_hash="a",
            raw_hash="b",
        ),
        raw_refs=[],
    )


def test_llm_client_without_key_uses_fallback(monkeypatch):
    monkeypatch.delenv("DASHSCOPE_API_KEY", raising=False)
    client = llm_module.LLMClient(api_key=None)

    result = client.summarize_item(_build_item(), "test prompt", "zh-CN")
    assert result["one_liner"]
    assert len(result["bullets"]) == 3
    assert result["why_it_matters"] is None


class _FakeCompletions:
    def __init__(self, response_content: str, captured: dict):
        self._response_content = response_content
        self._captured = captured

    def create(self, **kwargs):
        self._captured["kwargs"] = kwargs
        message = SimpleNamespace(content=self._response_content)
        choice = SimpleNamespace(message=message)
        return SimpleNamespace(choices=[choice])


class _FakeClient:
    def __init__(self, response_content: str, captured: dict):
        self.chat = SimpleNamespace(completions=_FakeCompletions(response_content, captured))


def _patch_openai(monkeypatch, response_content: str, captured: dict):
    def _factory(*, api_key: str, base_url: str, http_client):
        captured["api_key"] = api_key
        captured["base_url"] = base_url
        captured["http_client"] = http_client
        return _FakeClient(response_content, captured)

    monkeypatch.setattr(llm_module, "OpenAI", _factory)


def test_llm_client_parse_json_and_disable_thinking(monkeypatch):
    captured: dict = {}
    response_content = '{"one_liner":"一句话","bullets":["a","b","c"],"why_it_matters":"重要"}'
    _patch_openai(monkeypatch, response_content, captured)

    client = llm_module.LLMClient(api_key="test-key")
    result = client.summarize_item(_build_item(), "test prompt", "zh-CN")

    assert captured["api_key"] == "test-key"
    assert captured["base_url"] == llm_module.DASHSCOPE_BASE_URL
    assert isinstance(captured["http_client"], httpx.Client)
    kwargs = captured["kwargs"]
    assert kwargs["model"] == "deepseek-v3.2"
    assert kwargs["stream"] is False
    assert kwargs["extra_body"]["enable_thinking"] is False

    assert result["one_liner"] == "一句话"
    assert result["bullets"] == ["a", "b", "c"]
    assert result["why_it_matters"] == "重要"


def test_llm_client_non_json_falls_back(monkeypatch):
    captured: dict = {}
    _patch_openai(monkeypatch, "this is not json", captured)

    client = llm_module.LLMClient(api_key="test-key")
    result = client.summarize_item(_build_item(), "test prompt", "zh-CN")

    assert result["one_liner"]
    assert len(result["bullets"]) == 3
    assert result["why_it_matters"] is None


def test_llm_client_uses_configured_model(monkeypatch):
    captured: dict = {}
    response_content = '{"one_liner":"一句话","bullets":["a","b","c"],"why_it_matters":"重要"}'
    _patch_openai(monkeypatch, response_content, captured)

    client = llm_module.LLMClient(model="qwen3.5-397b-a17b", api_key="test-key")
    _ = client.summarize_item(_build_item(), "test prompt", "zh-CN")

    kwargs = captured["kwargs"]
    assert kwargs["model"] == "qwen3.5-397b-a17b"


def test_llm_client_summarize_markdown_without_key_uses_fallback(monkeypatch):
    monkeypatch.delenv("DASHSCOPE_API_KEY", raising=False)
    client = llm_module.LLMClient(api_key=None)

    digest_md = (
        "# Daily Digest\n\n"
        "- Generated at: 2026-02-22T12:00:00+08:00\n\n"
        "## 1. owner/repo\n"
        "This is a useful project.\n"
        "- Source: https://github.com/owner/repo\n"
    )
    result = client.summarize_markdown(digest_md, "prompt", "zh-CN")

    assert "# 每日科技动态" in result
    assert "1. **owner/repo**" in result
    assert "Source:" not in result


def test_llm_client_summarize_markdown_strips_fence(monkeypatch):
    captured: dict = {}
    response_content = "```markdown\n# 标题\n\n1. 条目\n```"
    _patch_openai(monkeypatch, response_content, captured)

    client = llm_module.LLMClient(api_key="test-key")
    result = client.summarize_markdown("raw markdown", "prompt", "zh-CN")

    assert result == "# 标题\n\n1. 条目\n"
