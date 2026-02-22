from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator

from infostream.utils.timezone import parse_timezone


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
    max_items: int = Field(default=10, ge=1, le=200)
    prompt_template: str = "Summarize the item into one_liner, 3 bullets, and why_it_matters."
    focus_tags: list[str] = Field(default_factory=list)
    keywords: list[str] = Field(default_factory=list)
    priority_strategy: str = "github_hot_then_paper_blog_video"
    language: str = "zh-CN"
    llm_model: str = "deepseek-v3.2"
    source_limits: dict[str, int] = Field(
        default_factory=lambda: {
            "github": 10,
            "bilibili": 5,
        }
    )
    github_trending_total_limit: int | None = None
    timezone: str = "UTC+08:00"
    reuse_same_day: bool = True
    backfill_from_same_day_cache: bool = True
    skip_discover_if_cached_same_day: bool = True
    reuse_materialize_mode: Literal["reference"] = "reference"
    rate_limit_break_on_403: bool = True

    @field_validator("source_limits")
    @classmethod
    def validate_source_limits(cls, value: dict[str, int]) -> dict[str, int]:
        normalized: dict[str, int] = {}
        for key, limit in value.items():
            key_norm = key.strip().lower()
            if not key_norm:
                raise ValueError("source_limits contains an empty key")
            if limit < 1 or limit > 50:
                raise ValueError("source_limits value must be between 1 and 50")
            normalized[key_norm] = limit
        return normalized

    @field_validator("llm_model")
    @classmethod
    def validate_llm_model(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("llm_model must not be empty")
        return normalized

    @field_validator("timezone")
    @classmethod
    def validate_timezone(cls, value: str) -> str:
        parse_timezone(value)
        return value

    @field_validator("github_trending_total_limit")
    @classmethod
    def validate_github_trending_total_limit(cls, value: int | None) -> int | None:
        if value is None:
            return value
        if value < 1 or value > 200:
            raise ValueError("github_trending_total_limit must be between 1 and 200")
        return value


class TimeoutsConfig(BaseModel):
    request_timeout_sec: int = Field(default=20, ge=1)
    source_timeout_sec: int = Field(default=180, ge=1)
    run_timeout_sec: int = Field(default=1800, ge=1)
