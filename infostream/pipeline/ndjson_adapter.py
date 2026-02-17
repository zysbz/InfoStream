from __future__ import annotations

import json
from typing import Iterable


REQUIRED_KEYS = {"stage", "source", "url", "payload", "fetched_at", "request_meta"}


def parse_ndjson_lines(lines: Iterable[str]) -> tuple[list[dict], list[dict]]:
    records: list[dict] = []
    errors: list[dict] = []

    for line_no, line in enumerate(lines, start=1):
        text = line.strip()
        if not text:
            continue

        try:
            record = json.loads(text)
        except json.JSONDecodeError as exc:
            errors.append(
                {
                    "line": line_no,
                    "error_type": "JSONDecodeError",
                    "message": str(exc),
                }
            )
            continue

        missing = sorted(REQUIRED_KEYS.difference(record.keys()))
        if missing:
            errors.append(
                {
                    "line": line_no,
                    "error_type": "SchemaError",
                    "message": f"Missing keys: {', '.join(missing)}",
                }
            )
            continue

        records.append(record)

    return records, errors