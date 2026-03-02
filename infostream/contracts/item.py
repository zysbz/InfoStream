from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, Field

ContentType = Literal["repo", "article", "paper", "video", "post", "other"]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class Entry(BaseModel):
    url: str
    source_name: str
    metadata: dict[str, Any] = Field(default_factory=dict)
    discovered_at: datetime = Field(default_factory=utc_now)


class RawPayload(BaseModel):
    entry_url: str
    source_name: str
    fetched_at: datetime = Field(default_factory=utc_now)
    content_type: str = "other"
    payload: Any
    status_code: int | None = None
    headers: dict[str, str] = Field(default_factory=dict)
    final_url: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class Evidence(BaseModel):
    source_url: str
    fetched_at: datetime
    content_hash: str
    raw_hash: str
    request_context: dict[str, Any] = Field(default_factory=dict)
    extract_hints: dict[str, Any] = Field(default_factory=dict)


class ItemDraft(BaseModel):
    source: str
    source_url: str
    title: str
    published_at: datetime | None = None
    fetched_at: datetime = Field(default_factory=utc_now)
    content_type: ContentType = "other"
    text: str = ""
    tags: list[str] = Field(default_factory=list)


class Item(BaseModel):
    id: str
    version: str
    source: str
    source_url: str
    title: str
    published_at: datetime | None = None
    fetched_at: datetime
    content_type: ContentType
    text: str
    tags: list[str] = Field(default_factory=list)
    evidence: Evidence
    raw_refs: list[str] = Field(default_factory=list)


class DigestItem(BaseModel):
    item_id: str
    title: str
    one_liner: str
    bullets: list[str]
    why_it_matters: str | None = None
    tags: list[str] = Field(default_factory=list)
    source_url: str
    local_path: str
    status: Literal["new", "updated", "unchanged", "reused"] = "new"
    section: Literal["new", "updated", "reused"] = "new"
    published_at: datetime | None = None
    fetched_at: datetime | None = None
