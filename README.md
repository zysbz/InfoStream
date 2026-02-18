# InfoStream

[English](README.md) | [简体中文](README.zh-CN.md)

InfoStream is an extensible source-ingestion and daily-digest pipeline. It collects updates from multiple sources, normalizes and deduplicates content, stores reproducible run artifacts, and produces both human-readable and machine-readable digests.

## Overview

InfoStream is designed for repeatable daily intelligence workflows:

- Ingest from heterogeneous sources through a plugin registry.
- Normalize into a shared item contract.
- Persist source evidence and run metadata for auditability.
- Deduplicate and version items with SQLite.
- Generate AI summaries with deterministic fallback.

## Features

- Implemented source plugins:
  - `github_trending`
  - `github_search`
  - `rss_atom`
- `bilibili_up` plugin scaffold exists and is disabled in MVP.
- SQLite catalog for deduplication/versioning (`data/catalog.db`).
- Run-scoped archival outputs (`output/YYYYMMDD_HHMM/`).
- Digest outputs:
  - `digest.md` (human readable)
  - `digest.json` (machine readable)
- DashScope-compatible LLM client (default model `deepseek-v3.2`).
- Runtime protections:
  - same-day URL reuse
  - same-day cache backfill
  - source-group cooldown after GitHub `403` rate limit

## Quick Start

### Prerequisites

- Python `3.12+`
- `uv` recommended for dependency management

### Install

```powershell
uv sync
```

Optional (development dependencies):

```powershell
uv sync --extra dev
```

### Environment Variables

Create `.env` at the project root (based on `.env.example`):

```env
DASHSCOPE_API_KEY=your_dashscope_api_key
GITHUB_TOKEN=optional_github_token
```

- `DASHSCOPE_API_KEY`: required for LLM digest generation.
- `GITHUB_TOKEN`: optional, increases GitHub API quota.

### Validate Configuration

```powershell
uv run main.py validate-config --sources configs/sources.yaml --run-config configs/run_config.json
```

### Run One Pipeline

```powershell
uv run main.py run --sources configs/sources.yaml --run-config configs/run_config.json --timeouts configs/timeouts.yaml --output-root output --data-root data
```

## Configuration

Primary config files:

- `configs/sources.yaml`
  - source list (`name`, `type`, `enabled`, `entry_urls`, `params`, etc.)
  - transcribe policy
  - GitHub search keywords
- `configs/run_config.json`
  - runtime policy (`max_items`, `source_limits`, `timezone`, reuse/backfill switches, digest preferences)
  - `max_items` range: `1-50` (model default is `10`)
- `configs/timeouts.yaml`
  - request/source/run timeouts

Useful runtime overrides:

```powershell
uv run main.py run --max-items 20
uv run main.py run --add-url https://github.com/trending
uv run main.py run --no-progress
```

## CLI Usage

List plugins:

```powershell
uv run main.py list-plugins
```

Validate configs:

```powershell
uv run main.py validate-config --sources configs/sources.yaml --run-config configs/run_config.json
```

Run pipeline:

```powershell
uv run main.py run --sources configs/sources.yaml --run-config configs/run_config.json --timeouts configs/timeouts.yaml
```

## Output Layout

Each run creates:

```text
output/YYYYMMDD_HHMM/
  digest.md
  digest.json
  items/
  raw/
  logs/
    run.log
    errors.json
  run_meta.json
```

Each item bundle:

```text
items/<source>__<title_sanitized>__<shortid>/
  content.txt
  meta.json
  evidence.json
  raw/
```

## Testing

```powershell
uv run pytest -q --basetemp=./tmp_pytest
```

## Privacy and Security

- Never commit `.env` or real tokens.
- Keep API keys in environment variables only.
- `output/` and `data/` are local runtime artifacts and should stay out of version control.
- Review staged files before every push:

```powershell
git status --short
git diff --cached --name-only
```

## Roadmap

- Implement production-ready `bilibili_up` plugin.
- Extend NDJSON interoperability for C++ ingestion core.
- Add broader source templates and stronger policy controls.

See also:

- `docs/architecture.md`
- `docs/cpp_ndjson_contract.md`

## License

Released under the MIT License. See [LICENSE](LICENSE).
