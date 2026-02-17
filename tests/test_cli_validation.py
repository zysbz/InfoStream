import pytest

from infostream.cli import _validate_sources_with_registry
from infostream.config.models import SourceConfig
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