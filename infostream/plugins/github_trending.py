from __future__ import annotations

import hashlib
import re
from datetime import datetime
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import httpx
from dateutil import parser as dt_parser

from infostream.contracts.item import Entry, Evidence, ItemDraft, RawPayload
from infostream.contracts.plugin import PluginCapabilities, SourcePlugin

_REPO_LINK = re.compile(r"href=\"/([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)\"")
_REPO_URL = re.compile(r"^https?://github\.com/([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+)/?$")
_NON_REPO_OWNERS = {
    "apps",
    "sponsors",
    "trending",
    "topics",
    "collections",
    "features",
    "marketplace",
    "orgs",
    "organizations",
    "users",
    "about",
    "pricing",
    "login",
    "join",
    "explore",
    "events",
    "settings",
}


class GitHubTrendingPlugin(SourcePlugin):
    source_name = "github_trending"
    supported_url_patterns = [
        r"^https?://github\.com/trending/?(?:\?.*)?$",
        r"^https?://[^\s]*github-trending[^\s]*/?.*$",
    ]
    capabilities = PluginCapabilities(supports_discover=True, supports_transcribe=False, requires_auth=False)

    def discover(self, source_config: "SourceConfig", client: httpx.Client, request_timeout_sec: int) -> list[Entry]:
        entry_urls = source_config.entry_urls or ["https://github.com/trending"]
        entries: list[Entry] = []
        token = source_config.params.get("github_token") or source_config.params.get("token")
        trending_since = str(source_config.params.get("since") or source_config.params.get("period") or "weekly").lower()
        spoken_language_code = str(source_config.params.get("spoken_language_code") or "").strip().lower()
        if trending_since not in {"daily", "weekly", "monthly"}:
            trending_since = "weekly"

        for entry_url in entry_urls:
            if "github.com/trending" in entry_url:
                target_url = _with_trending_filters(entry_url, trending_since, spoken_language_code)
                response = client.get(target_url, timeout=request_timeout_sec)
                response.raise_for_status()
                repos = sorted(set(repo for repo in _REPO_LINK.findall(response.text) if _is_repo_candidate(repo)))
                for repo in repos:
                    entries.append(
                        Entry(
                            url=f"https://github.com/{repo}",
                            source_name=self.source_name,
                            metadata={
                                "from": "trending_html",
                                "github_token": token,
                                "trending_since": trending_since,
                                "spoken_language_code": spoken_language_code,
                            },
                        )
                    )
                continue

            response = client.get(entry_url, timeout=request_timeout_sec)
            response.raise_for_status()
            payload = response.json()

            records: list[dict[str, Any]]
            if isinstance(payload, list):
                records = [record for record in payload if isinstance(record, dict)]
            elif isinstance(payload, dict):
                candidate = payload.get("items") or payload.get("repositories") or payload.get("data") or []
                records = [record for record in candidate if isinstance(record, dict)]
            else:
                records = []

            for record in records:
                repo_url = record.get("url") or record.get("url_path") or record.get("html_url")
                if not repo_url:
                    author = record.get("author")
                    name = record.get("name")
                    if author and name:
                        repo_url = f"https://github.com/{author}/{name}"
                if not repo_url:
                    continue
                if repo_url.startswith("/"):
                    repo_url = f"https://github.com{repo_url}"
                match = _REPO_URL.match(repo_url)
                if match and not _is_repo_candidate(f"{match.group(1)}/{match.group(2)}"):
                    continue

                entries.append(
                    Entry(
                        url=repo_url,
                        source_name=self.source_name,
                        metadata={
                            "repo": record,
                            "status_code": response.status_code,
                            "headers": dict(response.headers),
                            "github_token": token,
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
                metadata={"from": entry.metadata.get("from", "trending_api")},
            )

        match = _REPO_URL.match(entry.url)
        if match:
            owner, repo = match.group(1), match.group(2)
            headers = {"Accept": "application/vnd.github+json"}
            token = entry.metadata.get("github_token")
            if token:
                headers["Authorization"] = f"Bearer {token}"
            response = client.get(f"https://api.github.com/repos/{owner}/{repo}", headers=headers, timeout=request_timeout_sec)
            response.raise_for_status()
            return RawPayload(
                entry_url=entry.url,
                source_name=self.source_name,
                content_type="json",
                payload=response.json(),
                status_code=response.status_code,
                headers=dict(response.headers),
                final_url=str(response.url),
                metadata={"from": "github_api"},
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
            metadata={"from": "direct_html"},
        )

    def extract(self, raw: RawPayload) -> ItemDraft:
        repo: dict[str, Any] = raw.payload if isinstance(raw.payload, dict) else {}
        full_name = repo.get("full_name")
        if not full_name:
            author = repo.get("author")
            name = repo.get("name")
            if author and name:
                full_name = f"{author}/{name}"

        title = full_name or raw.entry_url
        description = repo.get("description") or ""
        language = repo.get("language") or ""
        topics = repo.get("topics") or []
        tags = [language.lower()] if language else []
        if isinstance(topics, list):
            tags.extend(str(topic).lower() for topic in topics)

        stars = repo.get("stargazers_count") or repo.get("stars")
        text_parts = [description]
        if stars is not None:
            text_parts.append(f"Stars: {stars}")
        if topics:
            text_parts.append("Topics: " + ", ".join(str(topic) for topic in topics))

        source_url = repo.get("html_url") or repo.get("url") or raw.entry_url
        if source_url.startswith("/"):
            source_url = f"https://github.com{source_url}"

        published_at = _parse_datetime(repo.get("created_at") or repo.get("builtBy") or repo.get("updated_at"))
        return ItemDraft(
            source=self.source_name,
            source_url=source_url,
            title=title,
            published_at=published_at,
            fetched_at=raw.fetched_at,
            content_type="repo",
            text="\n".join(part for part in text_parts if part),
            tags=sorted(set(tag for tag in tags if tag)),
        )

    def fingerprint(self, item: ItemDraft) -> str:
        if "/" in item.title:
            return item.title.lower()
        match = _REPO_URL.match(item.source_url)
        if match:
            return f"{match.group(1)}/{match.group(2)}".lower()
        return hashlib.sha1(item.source_url.encode("utf-8")).hexdigest()

    def provenance(self, raw: RawPayload, item: ItemDraft, content_hash: str, raw_hash: str) -> Evidence:
        return Evidence(
            source_url=item.source_url,
            fetched_at=raw.fetched_at,
            content_hash=content_hash,
            raw_hash=raw_hash,
            request_context={
                "status_code": raw.status_code,
                "headers": raw.headers,
                "final_url": raw.final_url,
            },
            extract_hints={"plugin": self.source_name, "from": raw.metadata.get("from")},
        )


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    try:
        return dt_parser.parse(str(value))
    except Exception:
        return None


def _with_trending_filters(url: str, since: str, spoken_language_code: str) -> str:
    parts = urlsplit(url)
    query_pairs = parse_qsl(parts.query, keep_blank_values=True)
    filtered = [(key, value) for key, value in query_pairs if key not in {"since", "spoken_language_code"}]
    filtered.append(("since", since))
    if spoken_language_code:
        filtered.append(("spoken_language_code", spoken_language_code))
    query = urlencode(filtered, doseq=True)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, query, ""))


def _is_repo_candidate(path: str) -> bool:
    parts = [segment for segment in path.split("/") if segment]
    if len(parts) != 2:
        return False
    owner, repo = parts[0].lower(), parts[1].lower()
    if owner in _NON_REPO_OWNERS:
        return False
    if repo in {"developers"}:
        return False
    return True


from infostream.config.models import SourceConfig
