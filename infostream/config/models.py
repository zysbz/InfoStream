from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, ValidationInfo, field_validator, model_validator

from infostream.utils.timezone import parse_timezone
from infostream.utils.url_norm import normalize_url


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


_DIGEST_STATUS_VALUES = {"new", "updated", "unchanged", "reused"}
_DIGEST_SECTION_KEYS = ("new", "updated", "reused")


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
    source_name_limits: dict[str, int] = Field(default_factory=dict)
    source_url_limits: dict[str, int] = Field(default_factory=dict)
    github_trending_total_limit: int | None = None
    timezone: str = "UTC+08:00"
    reuse_same_day: bool = True
    backfill_from_same_day_cache: bool = True
    skip_discover_if_cached_same_day: bool = True
    reuse_materialize_mode: Literal["reference"] = "reference"
    rate_limit_break_on_403: bool = True
    digest_include_statuses: list[str] = Field(default_factory=lambda: ["new", "updated"])
    digest_fallback_statuses: list[str] = Field(default_factory=lambda: ["reused", "unchanged"])
    digest_section_quota: dict[str, int] = Field(
        default_factory=lambda: {
            "new": 50,
            "updated": 30,
            "reused": 20,
        }
    )
    freshness_window_hours: int = Field(default=168, ge=24, le=336)
    show_reused_section: bool = True

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

    @field_validator("source_name_limits")
    @classmethod
    def validate_source_name_limits(cls, value: dict[str, int]) -> dict[str, int]:
        normalized: dict[str, int] = {}
        for key, limit in value.items():
            key_norm = key.strip().lower()
            if not key_norm:
                raise ValueError("source_name_limits contains an empty key")
            if limit < 1 or limit > 200:
                raise ValueError("source_name_limits value must be between 1 and 200")
            normalized[key_norm] = limit
        return normalized

    @field_validator("source_url_limits")
    @classmethod
    def validate_source_url_limits(cls, value: dict[str, int]) -> dict[str, int]:
        normalized: dict[str, int] = {}
        for key, limit in value.items():
            key_raw = str(key).strip()
            if not key_raw:
                raise ValueError("source_url_limits contains an empty key")
            key_norm = normalize_url(key_raw)
            if not key_norm:
                raise ValueError(f"source_url_limits contains an invalid URL key: {key_raw}")
            if limit < 1 or limit > 200:
                raise ValueError("source_url_limits value must be between 1 and 200")
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

    @field_validator("digest_include_statuses", "digest_fallback_statuses")
    @classmethod
    def validate_digest_statuses(cls, value: list[str], info: ValidationInfo) -> list[str]:
        normalized: list[str] = []
        seen: set[str] = set()
        for status in value:
            status_norm = str(status).strip().lower()
            if not status_norm:
                raise ValueError("digest status must not be empty")
            if status_norm not in _DIGEST_STATUS_VALUES:
                raise ValueError(f"unsupported digest status: {status_norm}")
            if status_norm not in seen:
                normalized.append(status_norm)
                seen.add(status_norm)
        if not normalized and info.field_name == "digest_include_statuses":
            raise ValueError("digest status list must not be empty")
        return normalized

    @field_validator("digest_section_quota")
    @classmethod
    def validate_digest_section_quota(cls, value: dict[str, int]) -> dict[str, int]:
        normalized = {key: 0 for key in _DIGEST_SECTION_KEYS}
        for key, quota in value.items():
            key_norm = str(key).strip().lower()
            if key_norm not in normalized:
                raise ValueError(f"unsupported digest section: {key_norm}")
            if quota < 0 or quota > 100:
                raise ValueError("digest_section_quota value must be between 0 and 100")
            normalized[key_norm] = quota
        if sum(normalized.values()) <= 0:
            raise ValueError("digest_section_quota must contain at least one positive value")
        return normalized

    @model_validator(mode="after")
    def validate_digest_status_sets(self) -> "RunConfig":
        overlap = set(self.digest_include_statuses).intersection(self.digest_fallback_statuses)
        if overlap:
            joined = ", ".join(sorted(overlap))
            raise ValueError(f"digest status appears in both include and fallback: {joined}")
        return self


class TimeoutsConfig(BaseModel):
    request_timeout_sec: int = Field(default=20, ge=1)
    source_timeout_sec: int = Field(default=180, ge=1)
    run_timeout_sec: int = Field(default=1800, ge=1)
