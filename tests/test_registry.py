from infostream.config.models import SourceConfig
from infostream.plugins.registry import build_default_registry


def test_registry_match_urls():
    registry = build_default_registry()
    assert registry.match_url('https://github.com/trending').source_name == 'github_trending'
    assert registry.match_url('https://api.github.com/search/repositories').source_name == 'github_search'
    assert registry.match_url('https://example.com/feed.xml').source_name == 'rss_atom'
    assert registry.match_url('https://rss.arxiv.org/rss/cs.AI').source_name == 'rss_atom'


def test_registry_get_source_plugin():
    registry = build_default_registry()
    plugin = registry.get('github_search')
    source = SourceConfig(name='s', type='github_search', entry_urls=['https://api.github.com/search/repositories'])
    assert plugin is not None
    assert source.type == plugin.source_name
