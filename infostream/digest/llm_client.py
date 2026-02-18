from __future__ import annotations

import json
import os
import re
from typing import Any

import httpx

from infostream.contracts.item import Item

try:
    from openai import OpenAI
except Exception:  # pragma: no cover - dependency/runtime guard
    OpenAI = None  # type: ignore[assignment]


DASHSCOPE_BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1"


class LLMClient:
    def __init__(self, model: str = "deepseek-v3.2", api_key: str | None = None) -> None:
        self.model = model
        self.api_key = api_key or os.getenv("DASHSCOPE_API_KEY")
        self.enabled = bool(self.api_key and OpenAI is not None)
        self.client = (
            OpenAI(
                api_key=self.api_key,
                base_url=DASHSCOPE_BASE_URL,
                http_client=httpx.Client(trust_env=False, timeout=60.0),
            )
            if self.enabled
            else None
        )

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
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You produce concise JSON only."},
                    {"role": "user", "content": prompt},
                ],
                stream=False,
                extra_body={"enable_thinking": False},
            )
            text = _extract_chat_completion_text(response)
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


def _extract_chat_completion_text(response: Any) -> str:
    choices = getattr(response, "choices", None)
    if not choices:
        return ""

    first_choice = choices[0] if choices else None
    message = getattr(first_choice, "message", None)
    content = getattr(message, "content", "") if message else ""

    if isinstance(content, str):
        return content

    if isinstance(content, list):
        chunks: list[str] = []
        for block in content:
            if isinstance(block, dict):
                block_text = block.get("text")
                if block_text:
                    chunks.append(str(block_text))
            else:
                block_text = getattr(block, "text", None)
                if block_text:
                    chunks.append(str(block_text))
        return "\n".join(chunks)

    return str(content or "")
