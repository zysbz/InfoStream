# Architecture

## Modules

- `infostream/cli.py`: CLI entry, config loading, validation, run orchestration
- `infostream/config/*`: typed config models and file loaders
- `infostream/contracts/*`: shared data contracts (`Entry`, `RawPayload`, `Item`, plugin ABI)
- `infostream/plugins/*`: source-specific adapters (GitHub trending/search, RSS, Bilibili TODO)
- `infostream/pipeline/*`: routing, orchestration, transcription policy gate
- `infostream/storage/*`: SQLite catalog, archive writer, Windows-safe path rules
- `infostream/digest/*`: LLM summary client + digest generation
- `infostream/logging/*`: structured run logging and error aggregation

## Run Flow

1. Load `sources.yaml` + `run_config.json` + `timeouts.yaml`
2. Merge `--add-url` into in-memory source list (no config persistence)
3. Route each source to plugin
4. `discover -> fetch -> extract -> fingerprint -> provenance`
5. Persist raw + item bundle under `output/<run_id>/...`
6. Upsert de-dup/version state in SQLite (`data/catalog.db`)
7. Select new/updated items and generate digest via LLM (fallback on failure)
8. Emit `digest.md`, `digest.json`, `run_meta.json`, `logs/run.log`, `logs/errors.json`

## Failure Semantics

- Per-item failure: log and continue
- Per-source timeout: stop current source, continue remaining sources
- Global run timeout: graceful stop with partial outputs
- LLM failure: fallback deterministic summary, still produce digest files

## Extensibility

- Add source by implementing `SourcePlugin` contract and registering in `plugins/registry.py`
- Bilibili plugin scaffold exists but intentionally raises `NotImplementedError` in MVP
- C++ core can later feed NDJSON contract documented in `docs/cpp_ndjson_contract.md`