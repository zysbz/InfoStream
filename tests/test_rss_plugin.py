from __future__ import annotations

import httpx
import pytest

from infostream.config.models import SourceConfig
from infostream.plugins.rss_atom import RSSAtomPlugin


def test_rss_discover_partial_failure_still_returns_entries():
    bad_feed = "https://example.com/bad.xml"
    good_feed = "https://example.com/good.xml"
    feed_xml = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Test Feed</title>
    <item>
      <title>Post 1</title>
      <link>https://example.com/post-1</link>
      <description>Hello</description>
    </item>
  </channel>
</rss>
"""

    def handler(request: httpx.Request) -> httpx.Response:
        if str(request.url) == bad_feed:
            return httpx.Response(404, request=request, text="not found")
        if str(request.url) == good_feed:
            return httpx.Response(200, request=request, text=feed_xml)
        return httpx.Response(500, request=request, text="unexpected")

    client = httpx.Client(transport=httpx.MockTransport(handler))
    plugin = RSSAtomPlugin()
    source = SourceConfig(
        name="rss_test",
        type="rss_atom",
        enabled=True,
        entry_urls=[bad_feed, good_feed],
    )

    entries = plugin.discover(source, client, 20)
    client.close()

    assert len(entries) == 1
    assert entries[0].url == "https://example.com/post-1"


def test_rss_discover_all_failed_raises_runtime_error():
    feed_url = "https://example.com/all_failed.xml"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, request=request, text="not found")

    client = httpx.Client(transport=httpx.MockTransport(handler))
    plugin = RSSAtomPlugin()
    source = SourceConfig(
        name="rss_test",
        type="rss_atom",
        enabled=True,
        entry_urls=[feed_url],
    )

    with pytest.raises(RuntimeError):
        plugin.discover(source, client, 20)
    client.close()
