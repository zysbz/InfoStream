from __future__ import annotations

import httpx

from infostream.contracts.item import Entry, Evidence, ItemDraft, RawPayload
from infostream.contracts.plugin import PluginCapabilities, SourcePlugin


class BilibiliUpPlugin(SourcePlugin):
    source_name = "bilibili_up"
    supported_url_patterns = [
        r"^https?://space\.bilibili\.com/\d+/upload/video/?$",
        r"^https?://www\.bilibili\.com/video/BV[0-9A-Za-z]+/?$",
    ]
    capabilities = PluginCapabilities(supports_discover=True, supports_transcribe=True, requires_auth=False)

    def discover(self, source_config: "SourceConfig", client: httpx.Client, request_timeout_sec: int) -> list[Entry]:
        raise NotImplementedError("bilibili_up plugin is TODO in this MVP")

    def fetch(self, entry: Entry, client: httpx.Client, request_timeout_sec: int) -> RawPayload:
        raise NotImplementedError("bilibili_up plugin is TODO in this MVP")

    def extract(self, raw: RawPayload) -> ItemDraft:
        raise NotImplementedError("bilibili_up plugin is TODO in this MVP")

    def fingerprint(self, item: ItemDraft) -> str:
        raise NotImplementedError("bilibili_up plugin is TODO in this MVP")

    def provenance(self, raw: RawPayload, item: ItemDraft, content_hash: str, raw_hash: str) -> Evidence:
        raise NotImplementedError("bilibili_up plugin is TODO in this MVP")


from infostream.config.models import SourceConfig