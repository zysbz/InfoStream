from datetime import datetime, timedelta, timezone

from infostream.config.models import RunConfig
from infostream.contracts.item import Evidence, Item
from infostream.digest.generator import generate_digest
from infostream.digest.llm_client import LLMClient


def _build_item(
    item_id: str,
    title: str,
    text: str,
    *,
    fetched_at: datetime | None = None,
    published_at: datetime | None = None,
) -> Item:
    fetched = fetched_at or datetime.now(timezone.utc)
    return Item(
        id=item_id,
        version="v1",
        source="github_search",
        source_url=f"https://github.com/{item_id}",
        title=title,
        published_at=published_at,
        fetched_at=fetched,
        content_type="repo",
        text=text,
        tags=["llm", "inference"],
        evidence=Evidence(
            source_url=f"https://github.com/{item_id}",
            fetched_at=fetched,
            content_hash="a",
            raw_hash="b",
        ),
        raw_refs=[],
    )


def test_generate_digest_fallback_without_api_key():
    item = _build_item("owner/repo", "owner/repo", "A useful repository for LLM inference serving.")

    run_config = RunConfig(max_items=10)
    client = LLMClient(api_key="")
    markdown, digest_json = generate_digest([(item, "items/github_search__owner_repo__12345678")], run_config, client)

    assert "# Daily Digest" in markdown
    assert digest_json["total"] == 1
    first = digest_json["items"][0]
    assert first["item_id"] == "owner/repo"
    assert first["title"] == "owner/repo"


def test_generate_digest_backfills_when_keyword_filter_too_strict():
    item_match = _build_item("owner/repo1", "owner/repo1", "This project is for LLM inference serving.")
    item_other = _build_item("owner/repo2", "owner/repo2", "A networking project without related keywords.")

    run_config = RunConfig(max_items=2, keywords=["llm"], timezone="UTC+08:00")
    client = LLMClient(api_key="")
    _, digest_json = generate_digest(
        [
            (item_match, "items/github_search__owner_repo1__11111111"),
            (item_other, "items/github_search__owner_repo2__22222222"),
        ],
        run_config,
        client,
    )

    assert digest_json["total"] == 2
    assert str(digest_json["generated_at"]).endswith("+08:00")


def test_generate_digest_stale_items_can_backfill_when_underfilled():
    now = datetime.now(timezone.utc)
    fresh_item = _build_item("owner/fresh", "owner/fresh", "recent update", fetched_at=now - timedelta(hours=24))
    stale_item = _build_item("owner/stale", "owner/stale", "old update", fetched_at=now - timedelta(days=10))

    run_config = RunConfig(max_items=5, freshness_window_hours=168)
    client = LLMClient(api_key="")
    _, digest_json = generate_digest(
        [
            (fresh_item, "items/github_search__owner_fresh__11111111", "new"),
            (stale_item, "items/github_search__owner_stale__22222222", "new"),
        ],
        run_config,
        client,
    )

    assert digest_json["total"] == 2
    assert digest_json["stats"]["stale_backfilled"] == 1
    assert digest_json["stats"]["dropped_by_freshness_window"] == 0


def test_generate_digest_uses_fallback_statuses_when_primary_insufficient():
    item_new = _build_item("owner/new", "owner/new", "new content")
    item_reused = _build_item("owner/reused", "owner/reused", "reused content")

    run_config = RunConfig(
        max_items=2,
        digest_include_statuses=["new"],
        digest_fallback_statuses=["reused"],
        digest_section_quota={"new": 50, "updated": 0, "reused": 50},
    )
    client = LLMClient(api_key="")
    _, digest_json = generate_digest(
        [
            (item_new, "items/github_search__owner_new__11111111", "new"),
            (item_reused, "items/github_search__owner_reused__22222222", "reused"),
        ],
        run_config,
        client,
    )

    assert digest_json["total"] == 2
    statuses = [item["status"] for item in digest_json["items"]]
    assert "new" in statuses
    assert "reused" in statuses
    assert digest_json["stats"]["selected_reused"] == 1


def test_generate_digest_promoted_reused_can_enter_primary_pool():
    item_new = _build_item("owner/new", "owner/new", "new content")
    item_reused = _build_item("owner/reused", "owner/reused", "reused content")

    run_config = RunConfig(
        max_items=2,
        digest_include_statuses=["new"],
        digest_fallback_statuses=[],
    )
    client = LLMClient(api_key="")
    _, digest_json = generate_digest(
        [
            (item_new, "items/github_search__owner_new__11111111", "new", False),
            (item_reused, "items/github_search__owner_reused__22222222", "reused", True),
        ],
        run_config,
        client,
    )

    assert digest_json["total"] == 2
    statuses = [item["status"] for item in digest_json["items"]]
    assert statuses.count("reused") == 1
