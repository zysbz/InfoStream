# InfoStream

InfoStream is an extensible source ingestion and daily digest pipeline.

## MVP Scope

- Source plugins: `github_trending`, `github_search`, `rss_atom`
- Bilibili plugin: scaffold only (`TODO`, disabled by default)
- De-dup and versioning with SQLite (`data/catalog.db`)
- Run output archive with provenance under `output/YYYYMMDD_HHMM/`
- Digest output: `digest.md` and `digest.json`
- OpenAI-backed summarization with deterministic fallback

## Requirements

- Python 3.12+
- `uv` recommended for dependency management

## Install

```powershell
uv sync
```

## Environment

Create `.env` in project root:

```env
OPENAI_API_KEY=your_openai_key
GITHUB_TOKEN=optional_github_token
```

`GITHUB_TOKEN` helps avoid search rate limits.

## Run

```powershell
uv run main.py run --sources configs/sources.yaml --run-config configs/run_config.json --add-url https://github.com/trending
```

## Validate Config

```powershell
uv run main.py validate-config --sources configs/sources.yaml --run-config configs/run_config.json
```

## Test

```powershell
uv sync --extra dev
uv run pytest -q --basetemp=./tmp_pytest
```

## List Plugins

```powershell
uv run main.py list-plugins
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

Each item folder:

```text
items/<source>__<title_sanitized>__<shortid>/
  content.txt
  meta.json
  evidence.json
  raw/
```

Windows path constraints are handled automatically (`<>:"/\\|?*`, reserved names, trailing dot/space).

## Config Contracts

- `configs/sources.yaml`
  - `sources[]`: `name`, `type`, `enabled`, `entry_urls`, `discover_depth`, `since`, `timeout_sec`, `params`
  - `transcribe`: `enabled_domains`, `transcribe_since`
  - `github_search`: `keywords`, `sort`, `order`
- `configs/run_config.json`
  - `max_items`, `prompt_template`, `focus_tags`, `keywords`, `priority_strategy`, `language`
- `configs/timeouts.yaml`
  - `request_timeout_sec`, `source_timeout_sec`, `run_timeout_sec`

## Notes

- `--add-url` only applies to current run and is not persisted.
- LLM failures do not fail the run; fallback summary is used.
- NDJSON contract for future C++ integration: `docs/cpp_ndjson_contract.md`.
