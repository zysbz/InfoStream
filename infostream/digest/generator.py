from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Literal

from infostream.config.models import RunConfig
from infostream.contracts.item import DigestItem, Item
from infostream.digest.llm_client import LLMClient
from infostream.utils.timezone import parse_timezone

DigestStatus = Literal["new", "updated", "unchanged", "reused"]
DigestSection = Literal["new", "updated", "reused"]
DigestCandidate = tuple[Item, str] | tuple[Item, str, str] | tuple[Item, str, str, bool]

_STATUS_VALUES: set[str] = {"new", "updated", "unchanged", "reused"}
_SECTION_ORDER: tuple[DigestSection, ...] = ("new", "updated", "reused")
_STATUS_TO_SECTION: dict[DigestStatus, DigestSection] = {
    "new": "new",
    "updated": "updated",
    "reused": "reused",
    "unchanged": "reused",
}
_STATUS_PRIORITY: dict[DigestStatus, int] = {
    "new": 400,
    "updated": 300,
    "reused": 160,
    "unchanged": 120,
}
_CONTENT_PRIORITY: dict[str, int] = {
    "paper": 80,
    "article": 70,
    "repo": 65,
    "video": 60,
    "post": 55,
    "other": 40,
}


@dataclass(frozen=True)
class _ScoredCandidate:
    item: Item
    local_path: str
    status: DigestStatus
    section: DigestSection
    score: float
    reference_at: datetime | None
    promote_primary: bool


def generate_digest(
    candidates: list[DigestCandidate],
    run_config: RunConfig,
    llm_client: LLMClient,
) -> tuple[str, dict[str, Any]]:
    run_tz = parse_timezone(run_config.timezone)
    generated_at = datetime.now(run_tz)

    dropped_by_freshness = 0
    candidates_after_freshness = 0

    scored: list[_ScoredCandidate] = []
    stale_scored: list[_ScoredCandidate] = []
    freshness_cutoff = generated_at - timedelta(hours=run_config.freshness_window_hours)
    for raw in candidates:
        item, local_path, status, promote_primary = _parse_candidate(raw)
        section = _STATUS_TO_SECTION[status]
        reference_at = _reference_time(item, run_tz)

        score = _score_item(item, status, run_config, generated_at, reference_at)
        candidate = _ScoredCandidate(
            item=item,
            local_path=local_path,
            status=status,
            section=section,
            score=score,
            reference_at=reference_at,
            promote_primary=promote_primary,
        )
        if reference_at is not None and reference_at < freshness_cutoff:
            dropped_by_freshness += 1
            stale_scored.append(candidate)
            continue

        scored.append(candidate)
        candidates_after_freshness += 1

    deduped_by_item: dict[str, _ScoredCandidate] = {}
    for candidate in scored:
        existing = deduped_by_item.get(candidate.item.id)
        if existing is None or candidate.score > existing.score:
            deduped_by_item[candidate.item.id] = candidate

    stale_deduped_by_item: dict[str, _ScoredCandidate] = {}
    for candidate in stale_scored:
        existing = stale_deduped_by_item.get(candidate.item.id)
        if existing is None or candidate.score > existing.score:
            stale_deduped_by_item[candidate.item.id] = candidate

    ranked = sorted(deduped_by_item.values(), key=lambda value: value.score, reverse=True)
    stale_ranked = sorted(stale_deduped_by_item.values(), key=lambda value: value.score, reverse=True)
    include_statuses = {status for status in run_config.digest_include_statuses}
    fallback_statuses = {status for status in run_config.digest_fallback_statuses}

    if not run_config.show_reused_section:
        include_statuses = {status for status in include_statuses if _STATUS_TO_SECTION[_as_status(status)] != "reused"}
        fallback_statuses = {status for status in fallback_statuses if _STATUS_TO_SECTION[_as_status(status)] != "reused"}
        ranked = [candidate for candidate in ranked if candidate.section != "reused"]
        stale_ranked = [candidate for candidate in stale_ranked if candidate.section != "reused"]

    primary_pool = [candidate for candidate in ranked if candidate.status in include_statuses or candidate.promote_primary]
    fallback_pool = [candidate for candidate in ranked if candidate.status in fallback_statuses]

    section_limits = _build_section_limits(
        run_config.max_items,
        run_config.digest_section_quota,
        show_reused=run_config.show_reused_section,
    )

    selected = _select_candidates(
        max_items=run_config.max_items,
        primary_pool=primary_pool,
        fallback_pool=fallback_pool,
        section_limits=section_limits,
    )
    stale_backfilled = 0
    if len(selected) < run_config.max_items:
        stale_primary_pool = [
            candidate for candidate in stale_ranked if candidate.status in include_statuses or candidate.promote_primary
        ]
        stale_fallback_pool = [candidate for candidate in stale_ranked if candidate.status in fallback_statuses]
        stale_backfilled = _append_stale_backfill(
            selected=selected,
            max_items=run_config.max_items,
            stale_primary_pool=stale_primary_pool,
            stale_fallback_pool=stale_fallback_pool,
        )

    digest_items: list[DigestItem] = []
    for candidate in selected:
        summary = llm_client.summarize_item(candidate.item, run_config.prompt_template, run_config.language)
        digest_items.append(_to_digest_item(candidate, summary))

    markdown = _render_markdown(digest_items, generated_at=generated_at, show_reused=run_config.show_reused_section)
    section_counts = _count_sections(digest_items)
    status_counts = _count_statuses(digest_items)
    selected_reused = status_counts["reused"] + status_counts["unchanged"]

    digest_json: dict[str, Any] = {
        "generated_at": generated_at.isoformat(),
        "language": run_config.language,
        "total": len(digest_items),
        "items": [item.model_dump(mode="json") for item in digest_items],
        "sections": section_counts,
        "selection": {
            "include_statuses": sorted(include_statuses),
            "fallback_statuses": sorted(fallback_statuses),
            "section_limits": section_limits,
            "freshness_window_hours": run_config.freshness_window_hours,
            "show_reused_section": run_config.show_reused_section,
        },
        "stats": {
            "candidates_total": len(candidates),
            "candidates_after_freshness": candidates_after_freshness,
            "candidates_after_dedup": len(ranked),
            "dropped_by_freshness_window": max(0, dropped_by_freshness - stale_backfilled),
            "stale_backfilled": stale_backfilled,
            "selected_total": len(digest_items),
            "selected_new": status_counts["new"],
            "selected_updated": status_counts["updated"],
            "selected_reused": selected_reused,
            "selected_unchanged": status_counts["unchanged"],
        },
    }
    return markdown, digest_json


def _parse_candidate(raw: DigestCandidate) -> tuple[Item, str, DigestStatus, bool]:
    if len(raw) == 2:
        item, local_path = raw
        return item, local_path, "new", False
    if len(raw) == 3:
        item, local_path, status_raw = raw
        return item, local_path, _as_status(status_raw), False
    item, local_path, status_raw, promote_primary_raw = raw
    return item, local_path, _as_status(status_raw), bool(promote_primary_raw)


def _as_status(value: str) -> DigestStatus:
    normalized = str(value).strip().lower()
    if normalized not in _STATUS_VALUES:
        return "new"
    return normalized  # type: ignore[return-value]


def _reference_time(item: Item, run_tz) -> datetime | None:
    reference = item.published_at or item.fetched_at
    if reference is None:
        return None
    if reference.tzinfo is None:
        return reference.replace(tzinfo=run_tz)
    return reference.astimezone(run_tz)


def _score_item(
    item: Item,
    status: DigestStatus,
    run_config: RunConfig,
    generated_at: datetime,
    reference_at: datetime | None,
) -> float:
    score = float(_STATUS_PRIORITY[status])
    score += float(_source_priority(item.source))
    score += float(_CONTENT_PRIORITY.get(item.content_type, _CONTENT_PRIORITY["other"]))

    if run_config.focus_tags:
        focus_matches = len(set(tag.lower() for tag in item.tags).intersection(tag.lower() for tag in run_config.focus_tags))
        score += float(focus_matches * 8)

    if run_config.keywords:
        haystack = " ".join([item.title, item.text, " ".join(item.tags)]).lower()
        keyword_matches = 0
        for keyword in run_config.keywords:
            keyword_norm = keyword.strip().lower()
            if keyword_norm and keyword_norm in haystack:
                keyword_matches += 1
        score += float(min(keyword_matches, 5) * 6)

    if reference_at is not None:
        age_hours = max((generated_at - reference_at).total_seconds() / 3600.0, 0.0)
        score += max(0.0, 72.0 - age_hours) / 4.0

    return score


def _source_priority(source: str) -> int:
    lowered = source.lower().strip()
    if lowered.startswith("github_trending"):
        return 100
    if lowered.startswith("github_search"):
        return 90
    if "rss" in lowered:
        return 70
    if "bilibili" in lowered:
        return 60
    return 50


def _build_section_limits(
    max_items: int,
    quota: dict[str, int],
    *,
    show_reused: bool,
) -> dict[DigestSection, int]:
    normalized: dict[DigestSection, int] = {
        "new": max(0, int(quota.get("new", 0))),
        "updated": max(0, int(quota.get("updated", 0))),
        "reused": max(0, int(quota.get("reused", 0))) if show_reused else 0,
    }
    total_weight = sum(normalized.values())
    if total_weight <= 0:
        return {"new": max_items, "updated": 0, "reused": 0}

    limits: dict[DigestSection, int] = {section: 0 for section in _SECTION_ORDER}
    remainders: list[tuple[float, int, DigestSection]] = []
    allocated = 0
    for order_index, section in enumerate(_SECTION_ORDER):
        weight = normalized[section]
        if weight <= 0:
            continue
        exact = max_items * weight / total_weight
        base = int(exact)
        limits[section] = base
        allocated += base
        remainders.append((exact - base, -order_index, section))

    remaining = max_items - allocated
    for _, _, section in sorted(remainders, reverse=True):
        if remaining <= 0:
            break
        limits[section] += 1
        remaining -= 1

    return limits


def _select_candidates(
    *,
    max_items: int,
    primary_pool: list[_ScoredCandidate],
    fallback_pool: list[_ScoredCandidate],
    section_limits: dict[DigestSection, int],
) -> list[_ScoredCandidate]:
    selected: list[_ScoredCandidate] = []
    selected_ids: set[str] = set()
    section_counts: dict[DigestSection, int] = {section: 0 for section in _SECTION_ORDER}

    def append_from_pool(pool: list[_ScoredCandidate], *, with_section_limit: bool) -> None:
        for candidate in pool:
            if len(selected) >= max_items:
                return
            if candidate.item.id in selected_ids:
                continue
            if with_section_limit and section_counts[candidate.section] >= section_limits[candidate.section]:
                continue
            selected.append(candidate)
            selected_ids.add(candidate.item.id)
            section_counts[candidate.section] += 1

    append_from_pool(primary_pool, with_section_limit=True)
    append_from_pool(primary_pool, with_section_limit=False)
    append_from_pool(fallback_pool, with_section_limit=True)
    append_from_pool(fallback_pool, with_section_limit=False)
    return selected[:max_items]


def _append_stale_backfill(
    *,
    selected: list[_ScoredCandidate],
    max_items: int,
    stale_primary_pool: list[_ScoredCandidate],
    stale_fallback_pool: list[_ScoredCandidate],
) -> int:
    selected_ids = {candidate.item.id for candidate in selected}
    stale_backfilled = 0

    def append_pool(pool: list[_ScoredCandidate]) -> None:
        nonlocal stale_backfilled
        for candidate in pool:
            if len(selected) >= max_items:
                return
            if candidate.item.id in selected_ids:
                continue
            selected.append(candidate)
            selected_ids.add(candidate.item.id)
            stale_backfilled += 1

    append_pool(stale_primary_pool)
    append_pool(stale_fallback_pool)
    return stale_backfilled


def _to_digest_item(candidate: _ScoredCandidate, summary: dict[str, Any]) -> DigestItem:
    one_liner = str(summary.get("one_liner") or "").strip() or candidate.item.title

    bullets_raw = summary.get("bullets")
    bullets: list[str] = []
    if isinstance(bullets_raw, list):
        for bullet in bullets_raw:
            text = str(bullet).strip()
            if text:
                bullets.append(text)
    bullets = bullets[:3]

    if len(bullets) < 3:
        fallback_bullets = [
            f"Tags: {', '.join(candidate.item.tags[:5])}" if candidate.item.tags else "",
            f"Source: {candidate.item.source_url}",
            f"Type: {candidate.item.content_type}",
        ]
        for line in fallback_bullets:
            text = line.strip()
            if not text:
                continue
            if text in bullets:
                continue
            bullets.append(text)
            if len(bullets) >= 3:
                break

    why_it_matters_raw = summary.get("why_it_matters")
    why_it_matters = str(why_it_matters_raw).strip() if why_it_matters_raw is not None else ""

    return DigestItem(
        item_id=candidate.item.id,
        title=candidate.item.title,
        one_liner=one_liner,
        bullets=bullets,
        why_it_matters=why_it_matters or None,
        tags=candidate.item.tags,
        source_url=candidate.item.source_url,
        local_path=candidate.local_path,
        status=candidate.status,
        section=candidate.section,
        published_at=candidate.item.published_at,
        fetched_at=candidate.item.fetched_at,
    )


def _render_markdown(items: list[DigestItem], *, generated_at: datetime, show_reused: bool) -> str:
    lines: list[str] = [
        "# Daily Digest",
        "",
        f"- Generated at: {generated_at.isoformat()}",
        f"- Total items: {len(items)}",
    ]
    if not items:
        return "\n".join(lines) + "\n"

    lines.append("")
    grouped = _group_items_by_section(items)
    global_index = 1
    for section in _SECTION_ORDER:
        if section == "reused" and not show_reused:
            continue
        section_items = grouped[section]
        if not section_items:
            continue
        lines.append(f"### {section.capitalize()} ({len(section_items)})")
        lines.append("")
        for item in section_items:
            lines.append(f"## {global_index}. {item.title}")
            lines.append(item.one_liner)
            lines.append("")
            for bullet in item.bullets:
                lines.append(f"- {bullet}")
            if item.why_it_matters:
                lines.append(f"- Why it matters: {item.why_it_matters}")
            lines.append(f"- Source: {item.source_url}")
            lines.append(f"- Local: {item.local_path}")
            lines.append("")
            global_index += 1
    return "\n".join(lines).rstrip() + "\n"


def _group_items_by_section(items: list[DigestItem]) -> dict[DigestSection, list[DigestItem]]:
    grouped: dict[DigestSection, list[DigestItem]] = {section: [] for section in _SECTION_ORDER}
    for item in items:
        grouped[item.section].append(item)
    return grouped


def _count_sections(items: list[DigestItem]) -> dict[str, int]:
    counts = {section: 0 for section in _SECTION_ORDER}
    for item in items:
        counts[item.section] += 1
    return counts


def _count_statuses(items: list[DigestItem]) -> dict[str, int]:
    counts: dict[str, int] = {
        "new": 0,
        "updated": 0,
        "reused": 0,
        "unchanged": 0,
    }
    for item in items:
        counts[item.status] += 1
    return counts
