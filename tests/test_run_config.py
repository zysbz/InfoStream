import pytest
from pydantic import ValidationError

from infostream.config.models import RunConfig


def test_run_config_default_max_items_is_10():
    cfg = RunConfig()
    assert cfg.max_items == 10


def test_run_config_default_llm_model_is_deepseek():
    cfg = RunConfig()
    assert cfg.llm_model == "deepseek-v3.2"


def test_run_config_llm_model_accepts_qwen():
    cfg = RunConfig(llm_model="qwen3.5-397b-a17b")
    assert cfg.llm_model == "qwen3.5-397b-a17b"


def test_run_config_llm_model_rejects_blank():
    with pytest.raises(ValidationError):
        RunConfig(llm_model="   ")


def test_run_config_max_items_accepts_200():
    cfg = RunConfig(max_items=200)
    assert cfg.max_items == 200


def test_run_config_max_items_rejects_over_200():
    with pytest.raises(ValidationError):
        RunConfig(max_items=201)


def test_run_config_source_limits_accepts_values():
    cfg = RunConfig(source_limits={"github": 10, "bilibili": 5})
    assert cfg.source_limits["github"] == 10
    assert cfg.source_limits["bilibili"] == 5


def test_run_config_source_limits_rejects_over_50():
    with pytest.raises(ValidationError):
        RunConfig(source_limits={"github": 51})


def test_run_config_source_name_limits_accepts_values():
    cfg = RunConfig(source_name_limits={"RSS_AI_FEEDS": 12})
    assert cfg.source_name_limits["rss_ai_feeds"] == 12


def test_run_config_source_name_limits_rejects_over_200():
    with pytest.raises(ValidationError):
        RunConfig(source_name_limits={"rss_ai_feeds": 201})


def test_run_config_source_url_limits_accepts_values():
    cfg = RunConfig(source_url_limits={"https://huggingface.co/blog/feed.xml": 12})
    assert cfg.source_url_limits["https://huggingface.co/blog/feed.xml"] == 12


def test_run_config_source_url_limits_rejects_over_200():
    with pytest.raises(ValidationError):
        RunConfig(source_url_limits={"https://huggingface.co/blog/feed.xml": 201})


def test_run_config_timezone_accepts_utc_plus_8():
    cfg = RunConfig(timezone="UTC+08:00")
    assert cfg.timezone == "UTC+08:00"


def test_run_config_timezone_rejects_invalid_value():
    with pytest.raises(ValidationError):
        RunConfig(timezone="INVALID_TIMEZONE")


def test_run_config_reuse_defaults():
    cfg = RunConfig()
    assert cfg.reuse_same_day is True
    assert cfg.backfill_from_same_day_cache is True
    assert cfg.skip_discover_if_cached_same_day is True
    assert cfg.reuse_materialize_mode == "reference"
    assert cfg.rate_limit_break_on_403 is True


def test_run_config_github_trending_total_limit_accepts_20():
    cfg = RunConfig(github_trending_total_limit=20)
    assert cfg.github_trending_total_limit == 20


def test_run_config_github_trending_total_limit_rejects_large_value():
    with pytest.raises(ValidationError):
        RunConfig(github_trending_total_limit=201)


def test_run_config_default_freshness_window_is_one_week():
    cfg = RunConfig()
    assert cfg.freshness_window_hours == 168


def test_run_config_rejects_overlapping_digest_status_sets():
    with pytest.raises(ValidationError):
        RunConfig(
            digest_include_statuses=["new", "updated"],
            digest_fallback_statuses=["updated", "reused"],
        )


def test_run_config_allows_empty_digest_fallback_statuses():
    cfg = RunConfig(digest_include_statuses=["new", "updated"], digest_fallback_statuses=[])
    assert cfg.digest_fallback_statuses == []


def test_run_config_rejects_empty_digest_section_quota():
    with pytest.raises(ValidationError):
        RunConfig(digest_section_quota={"new": 0, "updated": 0, "reused": 0})
