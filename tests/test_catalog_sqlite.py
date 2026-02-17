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