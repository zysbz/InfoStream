# InfoStream

English | [简体中文](README.md)

InfoStream is an extensible source-ingestion and daily-digest pipeline. It collects updates from multiple sources, normalizes and deduplicates content, stores reproducible run artifacts, and produces both human-readable and machine-readable digests.

## Live Preview

- https://zysbz.github.io/InfoStream/

## Preview

![InfoStream web preview](docs/assets/infostream-web-preview.png)

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
  - `summary.md` (post-digest concise markdown)
  - `output/latest.html` (single reusable web page, overwritten each run)
- DashScope-compatible LLM client (default model `deepseek-v3.2`).
- Runtime protections:
  - same-day URL reuse
  - same-day cache backfill
  - source-group cooldown after GitHub `403` rate limit

## Quick Start (From Zero to First Run)

### 1) Install `uv`

macOS / Linux:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Windows PowerShell:

```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

Verify installation:

```bash
uv --version
python --version
```

### 2) Clone the project

```bash
git clone <your-repo-url> InfoStream
cd InfoStream
```

If you already have the project locally, just `cd` into the directory.

### 3) Configure environment variables

Create `.env` from the template:

macOS / Linux:

```bash
cp .env.example .env
```

Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

Then edit `.env`:

```env
DASHSCOPE_API_KEY=your_dashscope_api_key
GITHUB_TOKEN=optional_github_token
```

- `DASHSCOPE_API_KEY`: enables LLM summaries (without it, pipeline falls back to deterministic summaries).
- `GITHUB_TOKEN`: optional, increases GitHub API quota.

### 4) Install dependencies

```bash
uv sync
```

Optional (for development and tests):

```bash
uv sync --extra dev
```

### 5) Validate config before running

```bash
uv run main.py validate-config --sources configs/sources.yaml --run-config configs/run_config.json
```

### 6) Run the pipeline

```bash
uv run main.py run --sources configs/sources.yaml --run-config configs/run_config.json --timeouts configs/timeouts.yaml --output-root output --data-root data
```

### 7) Check outputs

- Run outputs are archived under `output/YYYYMMDD_HHMM/`
- Key files:
  - `digest.md`
  - `digest.json`
  - `summary.md`
- Fixed reusable webpage:
  - `output/latest.html`

## Configuration

Primary config files:

- `configs/sources.yaml`
  - source list (`name`, `type`, `enabled`, `entry_urls`, `params`, etc.)
  - transcribe policy
  - GitHub search keywords
- `configs/run_config.json`
  - runtime policy (`max_items`, `llm_model`, `source_limits`, `source_name_limits`, `source_url_limits`, `timezone`, reuse/backfill switches, digest preferences)
  - digest preference keys: `digest_include_statuses`, `digest_fallback_statuses`, `digest_section_quota`, `freshness_window_hours`, `show_reused_section`
  - `llm_model` optional model selector. Default: `deepseek-v3.2` (example: `qwen3.5-397b-a17b`)
  - `max_items` range: `1-200` (model default is `10`)
- `configs/timeouts.yaml`
  - request/source/run timeouts

Source quota and coverage notes (`run_config.json`):

- `source_limits`
  - Per source-group cap (for example `github`, `rss_atom`).
- `source_name_limits`
  - Per source-name cap (for example `github_search_ai`, `rss_ai_feeds`) to prevent a single source from dominating the candidate pool.
  - Keys are case-insensitive and normalized to lowercase.
  - Example:
    - `"source_name_limits": {"github_search_ai": 8, "rss_ai_feeds": 12}`
- `source_url_limits` (required, per-entry_url quota)
  - Every `entry_url` of every enabled source must define a quota. Missing keys fail fast in both `validate-config` and `run`.
  - This enforces separate quotas inside the same source group. For example, `huggingface` and `deepmind` feeds under `rss_ai_feeds` must be configured independently.
  - Example:
    - `"source_url_limits": {"https://huggingface.co/blog/feed.xml": 6, "https://www.deepmind.com/blog/rss.xml": 4}`
- Interaction with `github_trending_total_limit`
  - `github_trending_*` sources still receive auto-distributed per-source-name caps.
  - If both `source_name_limits` and auto-distributed cap apply to the same source, the stricter (smaller) one wins.
- Is every source guaranteed to contribute
  - No. A source may yield zero due to no new entries, timeout, cooldown/rate-limit, or fetch errors.
  - The pipeline still attempts all enabled sources in order and records actual per-source contribution in run metadata.
- RSS recency order
  - `rss_atom` now sorts discovered feed entries by publish time descending, so newer entries are processed first instead of relying on feed order.

Digest preference parameters (`run_config.json`):

- `digest_include_statuses`
  - Primary status pool; digest selection prioritizes these statuses first.
  - Typical values: `new`, `updated`.
- `digest_fallback_statuses`
  - Fallback status pool used when primary pool is insufficient.
  - Typical values: `reused`, `unchanged`.
  - Note: it must not overlap with `digest_include_statuses`.
  - It can be an empty array `[]` (no fallback; rely on primary pool plus stale backfill).
- `digest_section_quota`
  - Target section ratio (not a hard cap). It is converted into section targets based on `max_items`.
  - Example: `{"new": 50, "updated": 30, "reused": 20}` with `max_items=30` yields approx `15/9/6`.

Example ("prefer fresh, then backfill"):

```json
{
  "max_items": 30,
  "digest_include_statuses": ["new", "updated"],
  "digest_fallback_statuses": ["reused", "unchanged"],
  "digest_section_quota": {
    "new": 50,
    "updated": 30,
    "reused": 20
  }
}
```

Status criteria (used by `digest_include_statuses` / `digest_fallback_statuses`):

- `new`
  - Condition: `item_id` appears for the first time (no existing row in `items`).
  - Result: first version is stored (`v1`).
- `updated`
  - Condition: `item_id` already exists, and at least one of `text_hash` or `meta_hash` differs from the latest version.
  - Result: version is incremented (for example `v2`, `v3`).
- `unchanged`
  - Condition: `item_id` already exists, and both `text_hash` and `meta_hash` are unchanged.
  - Result: current latest version is reused; no new version row is created.
- `reused`
  - Condition: same-day cache hit (normalized URL cache or same-day source cache), so the item is reused without fresh fetch.
  - Result: item enters reuse path and is shown in the `reused` digest section.

Notes:

- `item_id` is produced by each plugin's `fingerprint` logic (not a raw URL equality check).
- In digest rendering, both `unchanged` and `reused` are grouped into the `reused` section.
- If an item has never appeared in any historical `digest`, it gets boosted during selection even when current run status is `reused/unchanged` (to avoid losing value after interrupted runs).

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
  summary.md
  items/
  raw/
  logs/
    run.log
    errors.json
  run_meta.json

output/
  latest.html
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

## GitHub Actions + Pages (Daily Auto Update)

This repo now supports a daily GitHub Actions run of `uv run main.py run`, then publishes `output/latest.html` as a GitHub Pages static site.

One-time setup:

1. Ensure the workflow file exists: `.github/workflows/daily-pages.yml`.
2. In your GitHub repo, go to `Settings -> Pages`, and set `Build and deployment -> Source` to `GitHub Actions`.
3. Optional: add `DASHSCOPE_API_KEY` in `Settings -> Secrets and variables -> Actions` (used for LLM summarization; if missing, deterministic fallback is used).

Notes:

- The workflow supports both manual trigger (`workflow_dispatch`) and daily schedule.
- Current cron is `0 0 * * *` (00:00 UTC daily, which is 08:00 in Asia/Shanghai).
- GitHub Actions scheduled runs can be delayed by a few to 10+ minutes, so start time may not be exactly `08:00`.
- During deployment, `output/latest.html` is copied to Pages `index.html` (and `latest.html` is also kept).

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
