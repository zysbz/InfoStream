from __future__ import annotations

import hashlib
from datetime import datetime
from typing import Any

import httpx
from dateutil import parser as dt_parser

from infostream.contracts.item import Entry, Evidence, ItemDraft, RawPayload
from infostream.contracts.plugin import PluginCapabilities, SourcePlugin


class GitHubSearchPlugin(SourcePlugin):
    source_name = "github_search"
    supported_url_patterns = [
        r"^https?://github\.com/search.*$",
        r"^https?://api\.github\.com/search/repositories.*$",
    ]
    capabilities = PluginCapabilities(supports_discover=True, supports_transcribe=False, requires_auth=False)

    def discover(self, source_config: "SourceConfig", client: httpx.Client, request_timeout_sec: int) -> list[Entry]:
        params = source_config.params
        keywords = params.get("keywords", [])
        sort = params.get("sort", "stars")
        order = params.get("order", "desc")
        per_keyword = int(params.get("per_keyword", 10))

        headers = {"Accept": "application/vnd.github+json"}
        token = params.get("github_token") or source_config.params.get("token")
        if token:
            headers["Authorization"] = f"Bearer {token}"

        entries: list[Entry] = []
        discover_url = (
            str(source_config.entry_urls[0]) if source_config.entry_urls else "https://api.github.com/search/repositories"
        )
        for keyword in keywords:
            response = client.get(
                "https://api.github.com/search/repositories",
                params={"q": keyword, "sort": sort, "order": order, "per_page": per_keyword},
                headers=headers,
                timeout=request_timeout_sec,
            )
            response.raise_for_status()
            payload = response.json()
            for repo in payload.get("items", []):
                repo_url = repo.get("html_url")
                if not repo_url:
                    continue
                entries.append(
                    Entry(
                        url=repo_url,
                        source_name=self.source_name,
                        metadata={
                            "repo": repo,
                            "keyword": keyword,
                            "discover_url": discover_url,
                            "status_code": response.status_code,
                            "headers": dict(response.headers),
                        },
                    )
                )

        return entries

    def fetch(self, entry: Entry, client: httpx.Client, request_timeout_sec: int) -> RawPayload:
        if "repo" in entry.metadata:
            return RawPayload(
                entry_url=entry.url,
                source_name=self.source_name,
                content_type="json",
                payload=entry.metadata["repo"],
                status_code=entry.metadata.get("status_code"),
                headers=entry.metadata.get("headers", {}),
                final_url=entry.url,
                metadata={
                    "keyword": entry.metadata.get("keyword"),
                    "discover_url": entry.metadata.get("discover_url"),
                },
            )

        response = client.get(entry.url, timeout=request_timeout_sec)
        response.raise_for_status()
        return RawPayload(
            entry_url=entry.url,
            source_name=self.source_name,
            content_type="html",
            payload=response.text,
            status_code=response.status_code,
            headers=dict(response.headers),
            final_url=str(response.url),
            metadata={},
        )

    def extract(self, raw: RawPayload) -> ItemDraft:
        repo: dict[str, Any]
        if isinstance(raw.payload, dict):
            repo = raw.payload
        else:
            repo = {}

        full_name = repo.get("full_name") or repo.get("name") or raw.entry_url
        description = repo.get("description") or ""
        language = repo.get("language") or ""
        keyword = raw.metadata.get("keyword")

        tags = [tag for tag in [language.lower() if language else "", keyword] if tag]
        topics = repo.get("topics") or []
        if isinstance(topics, list):
            tags.extend(str(topic).lower() for topic in topics)

        text_parts = [description]
        if topics:
            text_parts.append("Topics: " + ", ".join(str(topic) for topic in topics))
        if repo.get("stargazers_count") is not None:
            text_parts.append(f"Stars: {repo.get('stargazers_count')}")

        published_at = _parse_datetime(repo.get("created_at"))
        return ItemDraft(
            source=self.source_name,
            source_url=repo.get("html_url") or raw.entry_url,
            title=full_name,
            published_at=published_at,
            fetched_at=raw.fetched_at,
            content_type="repo",
            text="\n".join(part for part in text_parts if part),
            tags=sorted(set(tags)),
        )

    def fingerprint(self, item: ItemDraft) -> str:
        if "/" in item.title:
            return item.title.lower()
        return hashlib.sha1(item.source_url.encode("utf-8")).hexdigest()

    def provenance(self, raw: RawPayload, item: ItemDraft, content_hash: str, raw_hash: str) -> Evidence:
        request_context = {
            "status_code": raw.status_code,
            "headers": raw.headers,
            "final_url": raw.final_url,
        }
        discover_url = raw.metadata.get("discover_url")
        if isinstance(discover_url, str):
            request_context["discover_url"] = discover_url
        return Evidence(
            source_url=item.source_url,
            fetched_at=raw.fetched_at,
            content_hash=content_hash,
            raw_hash=raw_hash,
            request_context=request_context,
            extract_hints={"plugin": self.source_name},
        )


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    try:
        return dt_parser.parse(str(value))
    except Exception:
        return None


from infostream.config.models import SourceConfig
