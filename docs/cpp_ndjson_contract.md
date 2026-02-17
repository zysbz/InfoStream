# C++ NDJSON Contract (MVP)

InfoStream Python accepts NDJSON lines from a future C++ ingestion core. Each line must be a valid JSON object.

## Required fields per line

- `stage`: string. Pipeline stage (`discover`, `fetch`, `extract`, etc.)
- `source`: string. Source name (`github_trending`, `rss_atom`, etc.)
- `url`: string. URL associated with this event
- `payload`: object|string|array. Raw structured data
- `fetched_at`: ISO8601 datetime string in UTC
- `request_meta`: object. Request/response details (headers, status, redirect chain)

## Example

```json
{"stage":"fetch","source":"github_trending","url":"https://github.com/openai/openai-python","payload":{"full_name":"openai/openai-python"},"fetched_at":"2026-02-17T10:00:00Z","request_meta":{"status_code":200}}
```

## Error handling

- Invalid JSON line: Python runtime records entry in `logs/errors.json` and continues.
- Missing required fields: Python runtime records schema violation in `logs/errors.json` and continues.
- `request_meta` is optional for back-compat but should be provided.

## Integration note

MVP runtime currently uses Python-native plugins directly. This contract is frozen for later C++ integration via stdout or file (`run_raw.ndjson`).