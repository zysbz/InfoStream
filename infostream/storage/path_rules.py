from __future__ import annotations

import hashlib
import re

_INVALID_CHARS = re.compile(r"[<>:\"/\\|?*]")
_WHITESPACE = re.compile(r"\s+")
_RESERVED_NAMES = {
    "CON",
    "PRN",
    "AUX",
    "NUL",
    "COM1",
    "COM2",
    "COM3",
    "COM4",
    "COM5",
    "COM6",
    "COM7",
    "COM8",
    "COM9",
    "LPT1",
    "LPT2",
    "LPT3",
    "LPT4",
    "LPT5",
    "LPT6",
    "LPT7",
    "LPT8",
    "LPT9",
}


def short_hash(value: str, length: int = 8) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()[:length]


def sanitize_windows_component(value: str, max_length: int = 120) -> str:
    cleaned = _INVALID_CHARS.sub("_", value)
    cleaned = _WHITESPACE.sub("_", cleaned).strip(" .")
    cleaned = cleaned[:max_length].rstrip(" .")

    if not cleaned:
        cleaned = "untitled"

    if cleaned.upper() in _RESERVED_NAMES:
        cleaned = f"_{cleaned}"

    return cleaned


def build_item_dir_name(source: str, title: str, item_id: str, suffix: str | None = None) -> str:
    source_part = sanitize_windows_component(source, max_length=40)
    title_part = sanitize_windows_component(title or "untitled", max_length=120)
    id_part = short_hash(item_id, length=8)

    base = f"{source_part}__{title_part}__{id_part}"
    if suffix:
        return f"{base}__{sanitize_windows_component(suffix, max_length=16)}"
    return base