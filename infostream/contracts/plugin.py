from __future__ import annotations

from abc import ABC, abstractmethod

import httpx
from pydantic import BaseModel

from infostream.contracts.item import Entry, Evidence, ItemDraft, RawPayload


class PluginCapabilities(BaseModel):
    supports_discover: bool = True
    supports_transcribe: bool = False
    requires_auth: bool = False


class SourcePlugin(ABC):
    source_name: str
    supported_url_patterns: list[str]
    capabilities: PluginCapabilities

    def __init__(self) -> None:
        if not hasattr(self, "source_name"):
            raise ValueError("Plugin must define source_name")
        if not hasattr(self, "supported_url_patterns"):
            self.supported_url_patterns = []
        if not hasattr(self, "capabilities"):
            self.capabilities = PluginCapabilities()

    def discover(self, source_config: "SourceConfig", client: httpx.Client, request_timeout_sec: int) -> list[Entry]:
        return [Entry(url=url, source_name=source_config.type) for url in source_config.entry_urls]

    @abstractmethod
    def fetch(self, entry: Entry, client: httpx.Client, request_timeout_sec: int) -> RawPayload:
        raise NotImplementedError

    @abstractmethod
    def extract(self, raw: RawPayload) -> ItemDraft:
        raise NotImplementedError

    @abstractmethod
    def fingerprint(self, item: ItemDraft) -> str:
        raise NotImplementedError

    @abstractmethod
    def provenance(self, raw: RawPayload, item: ItemDraft, content_hash: str, raw_hash: str) -> Evidence:
        raise NotImplementedError


from infostream.config.models import SourceConfig