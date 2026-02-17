from __future__ import annotations

import hashlib
from datetime import datetime
from typing import Any

import feedparser
import httpx
from dateutil import parser as dt_parser

from infostream.contracts.item import Entry, Evidence, ItemDraft, RawPayload
from infostream.contracts.plugin import PluginCapabilities, SourcePlugin


class RSSAtomPlugin(SourcePlugin):
    source_name = "rss_atom"
    supported_url_patterns = [
        r"^https?://.*(rss|atom|feed|\.xml)(\?.*)?$",
    ]
    capabilities = PluginCapabilities(supports_discover=True, supports_transcribe=False, requires_auth=False)

    def discover(self, source_config: "SourceConfig", client: httpx.Client, request_timeout_sec: int) -> list[Entry]:
        entries: list[Entry] = []
        for feed_url in source_config.entry_urls:
            feed = feedparser.parse(feed_url)
            for feed_entry in feed.entries:
                link = feed_entry.get("link")
                if not link:
                    continue
                entries.append(
                    Entry(
                        url=link,
                        source_name=self.source_name,
                        metadata={
                            "feed_url": feed_url,
                            "feed_entry": _to_plain_dict(feed_entry),
                        },
                    )
                )
        return entries

    def fetch(self, entry: Entry, client: httpx.Client, request_timeout_sec: int) -> RawPayload:
        if "feed_entry" in entry.metadata:
            return RawPayload(
                entry_url=entry.url,
                source_name=self.source_name,
                content_type="feed_entry",
                payload=entry.metadata["feed_entry"],
                status_code=200,
                headers={},
                final_url=entry.url,
                metadata={"feed_url": entry.metadata.get("feed_url")},
            )

        response = client.get(entry.url, timeout=request_timeout_sec)
        response.raise_for_status()
        return RawPayload(
            entry_url=entry.url,
            source_name=self.source_name,
            content_type="html",
            payload=response.text,
            status_code=response.status_code,
            headers=dict(response.headers),
            final_url=str(response.url),
            metadata={},
        )

    def extract(self, raw: RawPayload) -> ItemDraft:
        payload: dict[str, Any] = raw.payload if isinstance(raw.payload, dict) else {}
        title = payload.get("title") or raw.entry_url
        published_at = _parse_datetime(payload.get("published") or payload.get("updated"))

        content_parts: list[str] = []
        summary = payload.get("summary")
        if summary:
            content_parts.append(str(summary))

        content_list = payload.get("content")
        if isinstance(content_list, list):
            for block in content_list:
                if isinstance(block, dict) and block.get("value"):
                    content_parts.append(str(block["value"]))

        text = "\n\n".join(content_parts).strip()
        tags = [str(tag.get("term")).lower() for tag in payload.get("tags", []) if isinstance(tag, dict) and tag.get("term")]

        return ItemDraft(
            source=self.source_name,
            source_url=payload.get("link") or raw.entry_url,
            title=title,
            published_at=published_at,
            fetched_at=raw.fetched_at,
            content_type="article",
            text=text,
            tags=sorted(set(tags)),
        )

    def fingerprint(self, item: ItemDraft) -> str:
        normalized = item.source_url.strip().lower().rstrip("/")
        return hashlib.sha1(normalized.encode("utf-8")).hexdigest()

    def provenance(self, raw: RawPayload, item: ItemDraft, content_hash: str, raw_hash: str) -> Evidence:
        return Evidence(
            source_url=item.source_url,
            fetched_at=raw.fetched_at,
            content_hash=content_hash,
            raw_hash=raw_hash,
            request_context={
                "status_code": raw.status_code,
                "headers": raw.headers,
                "final_url": raw.final_url,
                "feed_url": raw.metadata.get("feed_url"),
            },
            extract_hints={"plugin": self.source_name},
        )


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    try:
        return dt_parser.parse(str(value))
    except Exception:
        return None


def _to_plain_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return {k: _to_plain_json(v) for k, v in value.items()}
    return {}


def _to_plain_json(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _to_plain_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_to_plain_json(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


from infostream.config.models import SourceConfig