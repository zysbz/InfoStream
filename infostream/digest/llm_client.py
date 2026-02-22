from __future__ import annotations

from datetime import datetime
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

    def summarize_markdown(self, markdown: str, prompt_template: str, language: str = "zh-CN") -> str:
        cleaned = markdown.strip()
        if not cleaned:
            return self._fallback_markdown_summary(markdown, language)

        if not self.enabled or self.client is None:
            return self._fallback_markdown_summary(markdown, language)

        prompt = (
            f"{prompt_template}\n"
            "You will receive a markdown digest. Return a further condensed markdown summary."
            " Keep facts strictly grounded in the source markdown."
            " Do not output fenced code blocks. Do not output explanations."
            f" Language: {language}.\n"
            "Source markdown:\n"
            f"{cleaned[:18000]}"
        )

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You produce concise markdown only."},
                    {"role": "user", "content": prompt},
                ],
                stream=False,
                extra_body={"enable_thinking": False},
            )
            text = _extract_chat_completion_text(response)
            normalized = _normalize_markdown_response(text)
            if normalized:
                return normalized
        except Exception:
            pass

        return self._fallback_markdown_summary(markdown, language)

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

    def _fallback_markdown_summary(self, markdown: str, language: str = "zh-CN") -> str:
        generated_at = _extract_generated_at(markdown)
        time_display = _format_generated_at(generated_at)
        is_zh = language.lower().startswith("zh")
        title = "每日科技动态" if is_zh else "Daily Tech Digest"
        summary_heading = "今日要点" if is_zh else "Highlights"
        time_prefix = "时间" if is_zh else "Time"
        empty_text = "暂无可用摘要内容。" if is_zh else "No summary content available."
        default_summary = "详情见原文。" if is_zh else "See details in the original digest."

        items = _extract_digest_items(markdown)

        lines = [
            f"# {title}",
            "",
            f"**{time_prefix}：{time_display}**",
            "",
            f"## {summary_heading}",
        ]
        if not items:
            lines.extend(["", empty_text, ""])
            return "\n".join(lines)

        lines.append("")
        for index, (item_title, item_desc) in enumerate(items, start=1):
            lines.append(f"{index}. **{item_title}**：{item_desc or default_summary}")
        lines.append("")
        return "\n".join(lines)


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


def _normalize_markdown_response(text: str) -> str:
    normalized = text.strip()
    if not normalized:
        return ""

    fence_match = re.match(r"^```(?:markdown|md)?\s*([\s\S]*?)\s*```$", normalized, flags=re.IGNORECASE)
    if fence_match:
        normalized = fence_match.group(1).strip()

    if not normalized:
        return ""
    return normalized + "\n"


def _extract_generated_at(markdown: str) -> str:
    match = re.search(r"^- Generated at:\s*(.+?)\s*$", markdown, flags=re.MULTILINE)
    if not match:
        return ""
    return match.group(1).strip()


def _format_generated_at(raw: str) -> str:
    if not raw:
        return datetime.now().strftime("%Y-%m-%d")

    candidate = raw.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(candidate)
        return parsed.strftime("%Y年%m月%d日")
    except ValueError:
        return raw


def _extract_digest_items(markdown: str) -> list[tuple[str, str]]:
    heading_re = re.compile(r"^##\s+\d+\.\s+(.+?)\s*$")
    lines = markdown.replace("\r\n", "\n").replace("\r", "\n").split("\n")

    heading_indexes: list[tuple[int, str]] = []
    for idx, line in enumerate(lines):
        match = heading_re.match(line.strip())
        if match:
            heading_indexes.append((idx, match.group(1).strip()))

    result: list[tuple[str, str]] = []
    for i, (start_idx, title) in enumerate(heading_indexes):
        end_idx = heading_indexes[i + 1][0] if i + 1 < len(heading_indexes) else len(lines)
        block = lines[start_idx + 1 : end_idx]

        one_liner = ""
        fallback_bullet = ""
        for raw in block:
            text = raw.strip()
            if not text:
                continue
            if text.startswith("- Source:") or text.startswith("- Local:"):
                continue
            if text.startswith("- "):
                if not fallback_bullet:
                    fallback_bullet = text[2:].strip()
                continue
            one_liner = text
            break

        result.append((title, one_liner or fallback_bullet))

    return result
