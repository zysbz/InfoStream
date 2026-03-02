import pytest

from infostream.cli import _validate_source_url_limits, _validate_sources_with_registry
from infostream.config.models import RunConfig, SourceConfig
from infostream.plugins.registry import build_default_registry


def test_validate_sources_unknown_type_raises():
    registry = build_default_registry()
    sources = [SourceConfig(name='x', type='unknown_type', entry_urls=['https://example.com/feed.xml'])]
    with pytest.raises(ValueError):
        _validate_sources_with_registry(sources, registry)


def test_validate_sources_mismatched_url_raises():
    registry = build_default_registry()
    sources = [SourceConfig(name='x', type='github_search', entry_urls=['https://example.com/feed.xml'])]
    with pytest.raises(ValueError):
        _validate_sources_with_registry(sources, registry)


def test_validate_source_url_limits_missing_quota_raises():
    sources = [
        SourceConfig(
            name="rss_ai_feeds",
            type="rss_atom",
            entry_urls=["https://huggingface.co/blog/feed.xml", "https://www.deepmind.com/blog/rss.xml"],
        )
    ]
    run_config = RunConfig(source_url_limits={"https://huggingface.co/blog/feed.xml": 6})
    with pytest.raises(ValueError):
        _validate_source_url_limits(sources, run_config)


def test_validate_source_url_limits_all_urls_configured_passes():
    sources = [
        SourceConfig(
            name="rss_ai_feeds",
            type="rss_atom",
            entry_urls=["https://huggingface.co/blog/feed.xml", "https://www.deepmind.com/blog/rss.xml"],
        )
    ]
    run_config = RunConfig(
        source_url_limits={
            "https://huggingface.co/blog/feed.xml": 6,
            "https://www.deepmind.com/blog/rss.xml": 4,
        }
    )
    _validate_source_url_limits(sources, run_config)
