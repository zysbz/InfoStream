from __future__ import annotations

from datetime import datetime

from infostream.config.models import RunConfig
from infostream.contracts.item import DigestItem, Item
from infostream.digest.llm_client import LLMClient
from infostream.utils.timezone import parse_timezone


def generate_digest(
    items_with_paths: list[tuple[Item, str]],
    run_config: RunConfig,
    llm_client: LLMClient,
) -> tuple[str, dict[str, object]]:
    ranked = _rank_items(items_with_paths, run_config)
    selected = ranked[: run_config.max_items]

    digest_items: list[DigestItem] = []
    for item, local_path in selected:
        summary = llm_client.summarize_item(item, run_config.prompt_template, run_config.language)
        digest_items.append(
            DigestItem(
                item_id=item.id,
                title=item.title,
                one_liner=summary.get("one_liner", ""),
                bullets=list(summary.get("bullets", []))[:3],
                why_it_matters=summary.get("why_it_matters"),
                tags=item.tags,
                source_url=item.source_url,
                local_path=local_path,
            )
        )

    digest_json: dict[str, object] = {
        "generated_at": datetime.now(parse_timezone(run_config.timezone)).isoformat(),
        "language": run_config.language,
        "total": len(digest_items),
        "items": [item.model_dump(mode="json") for item in digest_items],
    }

    markdown_lines = [
        "# Daily Digest",
        "",
        f"- Generated at: {digest_json['generated_at']}",
        f"- Total items: {digest_json['total']}",
        "",
    ]
    for index, item in enumerate(digest_items, start=1):
        markdown_lines.append(f"## {index}. {item.title}")
        markdown_lines.append(item.one_liner)
        markdown_lines.append("")
        for bullet in item.bullets:
            markdown_lines.append(f"- {bullet}")
        if item.why_it_matters:
            markdown_lines.append(f"- Why it matters: {item.why_it_matters}")
        markdown_lines.append(f"- Source: {item.source_url}")
        markdown_lines.append(f"- Local: {item.local_path}")
        markdown_lines.append("")

    return "\n".join(markdown_lines).strip() + "\n", digest_json


def _rank_items(items_with_paths: list[tuple[Item, str]], run_config: RunConfig) -> list[tuple[Item, str]]:
    focus_tags = {tag.lower() for tag in run_config.focus_tags}
    keywords = [kw.lower() for kw in run_config.keywords]
    scored_all = sorted(items_with_paths, key=lambda pair: (_score_item(pair[0]), pair[0].fetched_at), reverse=True)

    filtered: list[tuple[Item, str]] = []
    for item, local_path in scored_all:
        if focus_tags and not focus_tags.intersection({tag.lower() for tag in item.tags}):
            continue
        if keywords:
            blob = f"{item.title}\n{item.text}".lower()
            if not any(keyword in blob for keyword in keywords):
                continue
        filtered.append((item, local_path))

    if not filtered:
        return scored_all

    if len(filtered) >= run_config.max_items:
        return filtered

    filtered_ids = {item.id for item, _ in filtered}
    remaining = [pair for pair in scored_all if pair[0].id not in filtered_ids]
    return filtered + remaining


def _score_item(item: Item) -> int:
    score = 0
    if item.source == "github_trending":
        score += 100
    elif item.source == "github_search":
        score += 90

    score += {
        "paper": 80,
        "article": 70,
        "repo": 65,
        "video": 60,
        "post": 55,
        "other": 40,
    }.get(item.content_type, 40)

    return score
