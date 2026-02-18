from __future__ import annotations

from infostream.config.models import RunConfig, SourceConfig
from infostream.pipeline.orchestrator import _build_github_trending_source_limits


def _build_trending_sources() -> list[SourceConfig]:
    return [
        SourceConfig(
            name="github_trending_weekly_global",
            type="github_trending",
            enabled=True,
            entry_urls=["https://github.com/trending"],
            params={"since": "weekly"},
        ),
        SourceConfig(
            name="github_trending_weekly_zh",
            type="github_trending",
            enabled=True,
            entry_urls=["https://github.com/trending"],
            params={"since": "weekly", "spoken_language_code": "zh"},
        ),
        SourceConfig(
            name="github_trending_daily_global",
            type="github_trending",
            enabled=True,
            entry_urls=["https://github.com/trending"],
            params={"since": "daily"},
        ),
        SourceConfig(
            name="github_trending_daily_zh",
            type="github_trending",
            enabled=True,
            entry_urls=["https://github.com/trending"],
            params={"since": "daily", "spoken_language_code": "zh"},
        ),
    ]


def test_trending_quota_even_split_20():
    limits = _build_github_trending_source_limits(
        _build_trending_sources(),
        RunConfig(github_trending_total_limit=20),
    )
    assert limits["github_trending_weekly_global"] == 5
    assert limits["github_trending_weekly_zh"] == 5
    assert limits["github_trending_daily_global"] == 5
    assert limits["github_trending_daily_zh"] == 5


def test_trending_quota_remainder_goes_to_daily_global():
    limits = _build_github_trending_source_limits(
        _build_trending_sources(),
        RunConfig(github_trending_total_limit=22),
    )
    assert limits["github_trending_weekly_global"] == 5
    assert limits["github_trending_weekly_zh"] == 5
    assert limits["github_trending_daily_global"] == 7
    assert limits["github_trending_daily_zh"] == 5
