from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator


class SourceConfig(BaseModel):
    name: str
    type: str
    enabled: bool = True
    entry_urls: list[str] = Field(default_factory=list)
    discover_depth: int = Field(default=1, ge=1)
    since: datetime | None = None
    timeout_sec: int | None = Field(default=None, ge=1)
    params: dict[str, Any] = Field(default_factory=dict)

    @field_validator("type")
    @classmethod
    def normalize_type(cls, value: str) -> str:
        return value.strip().lower()


class TranscribeConfig(BaseModel):
    enabled_domains: list[str] = Field(default_factory=list)
    transcribe_since: datetime | None = None


class GitHubSearchConfig(BaseModel):
    keywords: list[str] = Field(default_factory=lambda: ["llm", "rag", "agent", "inference", "serving"])
    sort: str = "stars"
    order: str = "desc"


class SourcesFileConfig(BaseModel):
    sources: list[SourceConfig] = Field(default_factory=list)
    transcribe: TranscribeConfig = Field(default_factory=TranscribeConfig)
    github_search: GitHubSearchConfig = Field(default_factory=GitHubSearchConfig)


class RunConfig(BaseModel):
    max_items: int = Field(default=50, ge=1)
    prompt_template: str = "Summarize the item into one_liner, 3 bullets, and why_it_matters."
    focus_tags: list[str] = Field(default_factory=list)
    keywords: list[str] = Field(default_factory=list)
    priority_strategy: str = "github_hot_then_paper_blog_video"
    language: str = "zh-CN"


class TimeoutsConfig(BaseModel):
    request_timeout_sec: int = Field(default=20, ge=1)
    source_timeout_sec: int = Field(default=180, ge=1)
    run_timeout_sec: int = Field(default=1800, ge=1)