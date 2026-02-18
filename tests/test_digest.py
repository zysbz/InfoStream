from datetime import datetime, timezone

from infostream.config.models import RunConfig
from infostream.contracts.item import Evidence, Item
from infostream.digest.generator import generate_digest
from infostream.digest.llm_client import LLMClient


def _build_item(item_id: str, title: str, text: str) -> Item:
    return Item(
        id=item_id,
        version="v1",
        source="github_search",
        source_url=f"https://github.com/{item_id}",
        title=title,
        published_at=None,
        fetched_at=datetime.now(timezone.utc),
        content_type="repo",
        text=text,
        tags=["llm", "inference"],
        evidence=Evidence(
            source_url=f"https://github.com/{item_id}",
            fetched_at=datetime.now(timezone.utc),
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

    # one item matches keyword, second item is backfilled to satisfy max_items.
    assert digest_json["total"] == 2
    assert str(digest_json["generated_at"]).endswith("+08:00")