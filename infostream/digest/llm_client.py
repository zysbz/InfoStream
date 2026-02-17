from __future__ import annotations

import json
import os
import re
from typing import Any

from infostream.contracts.item import Item

try:
    from openai import OpenAI
except Exception:  # pragma: no cover - dependency/runtime guard
    OpenAI = None  # type: ignore[assignment]


class LLMClient:
    def __init__(self, model: str = "gpt-4.1-mini", api_key: str | None = None) -> None:
        self.model = model
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        self.enabled = bool(self.api_key and OpenAI is not None)
        self.client = OpenAI(api_key=self.api_key) if self.enabled else None

    def summarize_item(self, item: Item, prompt_template: str, language: str = "zh-CN") -> dict[str, Any]:
        if not self.enabled or self.client is None:
            return self._fallback_summary(item)

        prompt = (
            f"{prompt_template}\n"
            "Output must be a JSON object with keys one_liner, bullets (length 3), and why_it_matters."
            f" Language: {language}.\n"
            f"Title: {item.title}\n"
            f"Source: {item.source}\n"
            f"Tags: {', '.join(item.tags)}\n"
            f"Text: {item.text[:6000]}"
        )

        try:
            response = self.client.responses.create(
                model=self.model,
                input=[
                    {"role": "system", "content": "You produce concise JSON only."},
                    {"role": "user", "content": prompt},
                ],
            )
            text = getattr(response, "output_text", "") or _extract_response_text(response)
            parsed = _parse_json_block(text)
            if not isinstance(parsed, dict):
                raise ValueError("LLM output is not a JSON object")

            one_liner = str(parsed.get("one_liner", "")).strip()
            bullets_raw = parsed.get("bullets", [])
            bullets = [str(b).strip() for b in bullets_raw if str(b).strip()][:3]
            while len(bullets) < 3:
                bullets.append("")

            return {
                "one_liner": one_liner or self._fallback_summary(item)["one_liner"],
                "bullets": bullets,
                "why_it_matters": str(parsed.get("why_it_matters", "")).strip() or None,
            }
        except Exception:
            return self._fallback_summary(item)

    def _fallback_summary(self, item: Item) -> dict[str, Any]:
        text = re.sub(r"\s+", " ", item.text or "").strip()
        one_liner = text[:120] if text else f"{item.title} ({item.source})"

        bullets = []
        if item.tags:
            bullets.append("Tags: " + ", ".join(item.tags[:5]))
        bullets.append("Source: " + item.source_url)
        bullets.append("Type: " + item.content_type)
        return {
            "one_liner": one_liner,
            "bullets": bullets[:3],
            "why_it_matters": None,
        }


def _parse_json_block(text: str) -> dict[str, Any] | list[Any] | None:
    text = text.strip()
    if not text:
        return None

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    match = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if not match:
        return None
    try:
        return json.loads(match.group(0))
    except json.JSONDecodeError:
        return None


def _extract_response_text(response: Any) -> str:
    output = getattr(response, "output", None)
    if not output:
        return ""

    chunks: list[str] = []
    for item in output:
        content = getattr(item, "content", None) or []
        for block in content:
            text = getattr(block, "text", None)
            if text:
                chunks.append(str(text))
    return "\n".join(chunks)