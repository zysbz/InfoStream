from __future__ import annotations

import json
import traceback
import uuid
from datetime import datetime, timezone, tzinfo
from pathlib import Path
from typing import Any


class RunLogger:
    def __init__(self, logs_dir: Path, local_tz: tzinfo = timezone.utc) -> None:
        self.logs_dir = logs_dir
        self.local_tz = local_tz
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.log_path = self.logs_dir / "run.log"
        self.errors_path = self.logs_dir / "errors.json"
        self.errors: list[dict[str, Any]] = []

    def log(self, level: str, event: str, **data: Any) -> None:
        now_utc = datetime.now(timezone.utc)
        payload = {
            "ts": now_utc.isoformat(),
            "ts_utc": now_utc.isoformat(),
            "ts_local": now_utc.astimezone(self.local_tz).isoformat(),
            "level": level.upper(),
            "event": event,
            **data,
        }
        with self.log_path.open("a", encoding="utf-8") as fp:
            fp.write(json.dumps(payload, ensure_ascii=False) + "\n")

    def info(self, event: str, **data: Any) -> None:
        self.log("INFO", event, **data)

    def warning(self, event: str, **data: Any) -> None:
        self.log("WARNING", event, **data)

    def error(
        self,
        *,
        stage: str,
        source: str,
        url: str,
        error_type: str,
        message: str,
        exc: Exception | None = None,
    ) -> None:
        trace_id = str(uuid.uuid4())
        error_payload: dict[str, Any] = {
            "trace_id": trace_id,
            "stage": stage,
            "source": source,
            "url": url,
            "error_type": error_type,
            "message": message,
        }
        if exc is not None:
            error_payload["stack"] = "".join(traceback.format_exception(exc))

        self.errors.append(error_payload)
        self.log("ERROR", "pipeline_error", **error_payload)

    def flush_errors(self) -> None:
        self.errors_path.write_text(json.dumps(self.errors, ensure_ascii=False, indent=2), encoding="utf-8")
