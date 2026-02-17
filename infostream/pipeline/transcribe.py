from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from urllib.parse import urlparse

from infostream.contracts.item import Item


@dataclass
class TranscribePolicy:
    enabled_domains: set[str]
    transcribe_since: datetime | None = None


class Transcriber:
    def __init__(self, policy: TranscribePolicy) -> None:
        self.policy = policy

    def should_transcribe(self, item: Item, is_new_item: bool) -> bool:
        if item.content_type != "video":
            return False
        if not is_new_item:
            return False

        domain = (urlparse(item.source_url).hostname or "").lower()
        if not any(domain == allowed or domain.endswith(f".{allowed}") for allowed in self.policy.enabled_domains):
            return False

        if self.policy.transcribe_since and item.published_at:
            return item.published_at >= self.policy.transcribe_since
        return True

    def transcribe(self, item: Item) -> str:
        # TODO: connect external transcription API with retries and quota controls.
        return item.text