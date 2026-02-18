from infostream.storage.catalog_sqlite import CatalogSQLite


def test_catalog_versioning(tmp_path):
    catalog = CatalogSQLite(tmp_path / 'catalog.db')

    first = catalog.upsert_version(
        item_id='owner/repo',
        source='github_search',
        first_seen_at='2026-01-01T00:00:00',
        fetched_at='2026-01-01T00:00:00',
        published_at=None,
        title='owner/repo',
        content_type='repo',
        text_hash='text1',
        meta_hash='meta1',
        item_json_path='items/a/meta.json',
        evidence_json_path='items/a/evidence.json',
        raw_root_path='items/a/raw',
    )
    assert first.version == 'v1'
    assert first.is_new_item

    unchanged = catalog.upsert_version(
        item_id='owner/repo',
        source='github_search',
        first_seen_at='2026-01-01T00:00:00',
        fetched_at='2026-01-02T00:00:00',
        published_at=None,
        title='owner/repo',
        content_type='repo',
        text_hash='text1',
        meta_hash='meta1',
        item_json_path='items/b/meta.json',
        evidence_json_path='items/b/evidence.json',
        raw_root_path='items/b/raw',
    )
    assert unchanged.version == 'v1'
    assert not unchanged.is_new_version

    changed = catalog.upsert_version(
        item_id='owner/repo',
        source='github_search',
        first_seen_at='2026-01-01T00:00:00',
        fetched_at='2026-01-03T00:00:00',
        published_at=None,
        title='owner/repo',
        content_type='repo',
        text_hash='text2',
        meta_hash='meta2',
        item_json_path='items/c/meta.json',
        evidence_json_path='items/c/evidence.json',
        raw_root_path='items/c/raw',
    )
    assert changed.version == 'v2'
    assert changed.is_new_version

    catalog.close()


def test_catalog_daily_cache_and_cooldown(tmp_path):
    catalog = CatalogSQLite(tmp_path / "catalog.db")

    catalog.upsert_daily_url_cache(
        date_key="2026-02-18",
        normalized_url="https://github.com/owner/repo",
        item_id="owner/repo",
        version="v2",
        source_type="github_search",
        source_name="github_search_ai",
        source_group="github",
        item_json_path="output/20260218_0001/items/a/meta.json",
        evidence_json_path="output/20260218_0001/items/a/evidence.json",
        raw_root_path="output/20260218_0001/items/a/raw",
        fetched_at="2026-02-18T00:01:00+08:00",
        run_id="20260218_0001",
    )

    record = catalog.get_daily_url_cache("2026-02-18", "https://github.com/owner/repo")
    assert record is not None
    assert record.item_id == "owner/repo"
    assert record.version == "v2"

    rows = catalog.list_daily_cache("2026-02-18")
    assert len(rows) == 1
    assert rows[0].run_id == "20260218_0001"

    catalog.set_source_cooldown(
        source_group="github",
        blocked_until="2026-02-18T01:00:00+00:00",
        reason="github_403_rate_limit",
        updated_at="2026-02-18T00:45:00+00:00",
    )
    cooldown = catalog.get_source_cooldown("github")
    assert cooldown is not None
    assert cooldown.blocked_until == "2026-02-18T01:00:00+00:00"
    assert cooldown.reason == "github_403_rate_limit"

    catalog.close()
