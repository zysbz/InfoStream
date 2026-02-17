from datetime import datetime, timezone

from infostream.config.models import RunConfig
from infostream.contracts.item import Evidence, Item
from infostream.digest.generator import generate_digest
from infostream.digest.llm_client import LLMClient


def test_generate_digest_fallback_without_api_key():
    item = Item(
        id='owner/repo',
        version='v1',
        source='github_search',
        source_url='https://github.com/owner/repo',
        title='owner/repo',
        published_at=None,
        fetched_at=datetime.now(timezone.utc),
        content_type='repo',
        text='A useful repository for LLM inference serving.',
        tags=['llm', 'inference'],
        evidence=Evidence(
            source_url='https://github.com/owner/repo',
            fetched_at=datetime.now(timezone.utc),
            content_hash='a',
            raw_hash='b',
        ),
        raw_refs=[],
    )

    run_config = RunConfig(max_items=10)
    client = LLMClient(api_key='')
    markdown, digest_json = generate_digest([(item, 'items/github_search__owner_repo__12345678')], run_config, client)

    assert '# Daily Digest' in markdown
    assert digest_json['total'] == 1
    first = digest_json['items'][0]
    assert first['item_id'] == 'owner/repo'
    assert first['title'] == 'owner/repo'
