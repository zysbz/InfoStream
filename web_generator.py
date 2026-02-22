from __future__ import annotations

import argparse
import html
import json
import re
import webbrowser
from pathlib import Path

ORDERED_ITEM_RE = re.compile(r"^\s*\d+\.\s+")
INDENTED_TEXT_RE = re.compile(r"^\s{2,}\S")
H1_RE = re.compile(r"^\s*#\s+(.+?)\s*$")


def normalize_markdown(text: str) -> str:
  normalized = text.replace("\r\n", "\n").replace("\r", "\n").lstrip("\ufeff")
  lines = normalized.split("\n")
  out: list[str] = []

  for i, line in enumerate(lines):
    if line.strip():
      out.append(line)
      continue

    prev_non_empty = ""
    for prev in reversed(out):
      if prev.strip():
        prev_non_empty = prev
        break

    next_non_empty = ""
    for j in range(i + 1, len(lines)):
      if lines[j].strip():
        next_non_empty = lines[j]
        break

    if (
      ORDERED_ITEM_RE.match(next_non_empty)
      and (ORDERED_ITEM_RE.match(prev_non_empty) or INDENTED_TEXT_RE.match(prev_non_empty))
    ):
      continue

    out.append("")

  return "\n".join(out).rstrip() + "\n"


def infer_title(markdown: str, fallback: str) -> str:
  for line in markdown.splitlines():
    match = H1_RE.match(line)
    if match:
      return match.group(1).strip()
  return fallback


def build_html(title: str, markdown: str) -> str:
  title_escaped = html.escape(title)
  markdown_payload = json.dumps(markdown, ensure_ascii=False)
  template = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>__TITLE__</title>
  <style>
    :root {
      --paper: #f4f1e7;
      --paper-soft: #faf7f1;
      --ink: #151515;
      --ink-soft: #5c584f;
      --line: #d8d1c1;
    }
    * { box-sizing: border-box; }
    html, body {
      margin: 0;
      padding: 0;
      min-height: 100%;
      background-color: var(--paper);
    }
    body {
      font-family: "Noto Sans SC", "Microsoft YaHei", "Segoe UI", sans-serif;
      color: var(--ink);
      min-height: 100vh;
      background:
        radial-gradient(circle at 16% 8%, rgba(255, 255, 255, 0.5) 0 12%, transparent 34%),
        radial-gradient(circle at 83% 82%, rgba(233, 225, 207, 0.5) 0 13%, transparent 35%),
        linear-gradient(180deg, #f8f5ed 0%, var(--paper) 100%);
      background-attachment: fixed;
    }
    .page {
      min-height: 100dvh;
      display: grid;
      justify-items: center;
      align-content: start;
      padding: 2rem 1rem 3rem;
    }
    .report-shell {
      width: min(920px, 100%);
      background: linear-gradient(180deg, var(--paper-soft), #f5f0e3);
      border: 1px solid var(--line);
      border-radius: 26px;
      box-shadow: 0 18px 44px rgba(35, 30, 20, 0.08);
      padding: clamp(1.1rem, 3vw, 2rem);
    }
    .eyebrow {
      margin: 0;
      font-size: 0.76rem;
      text-transform: uppercase;
      letter-spacing: 0.16em;
      color: var(--ink-soft);
      font-weight: 700;
    }
    .report-content {
      border-top: 1px solid var(--line);
      padding-top: 1.15rem;
      margin-top: 0.85rem;
    }
    .report-content h1,
    .report-content h2,
    .report-content h3,
    .report-content h4,
    .report-content h5,
    .report-content h6 {
      line-height: 1.2;
      margin: 1.15rem 0 0.55rem;
      text-align: left;
      letter-spacing: -0.01em;
    }
    .report-content h1 { font-size: clamp(1.5rem, 4vw, 2rem); }
    .report-content h2 { font-size: clamp(1.22rem, 3.2vw, 1.52rem); }
    .report-content h3 { font-size: clamp(1.08rem, 2.8vw, 1.26rem); }
    .report-content p {
      margin: 0.45rem 0;
      line-height: 1.75;
      color: #2c2a26;
      word-break: break-word;
    }
    .report-content ol,
    .report-content ul {
      margin: 0.38rem 0 0.95rem 1.4rem;
      padding: 0;
    }
    .report-content li {
      margin: 0.34rem 0;
      line-height: 1.75;
      word-break: break-word;
    }
    .report-content li .li-sub {
      color: #3f3b34;
      font-size: 0.95rem;
    }
    .report-content hr {
      border: 0;
      border-top: 1px solid var(--line);
      margin: 1rem 0;
    }
    .report-content strong { color: #111; }
    .report-content a {
      color: #1a1a1a;
      text-decoration: underline;
    }
    .empty-text {
      margin: 0;
      color: #746f65;
    }
    @media (max-width: 640px) {
      .report-shell { border-radius: 18px; }
    }
  </style>
</head>
<body>
  <main class="page">
    <section class="report-shell">
      <p class="eyebrow">Anthropic Style</p>
      <article id="report-content" class="report-content" aria-live="polite"></article>
    </section>
  </main>

  <script>
    const REPORT_MARKDOWN = __MARKDOWN__;

    function escapeHtml(text) {
      return text
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");
    }

    function renderInline(text) {
      let safe = escapeHtml(text);
      safe = safe.replace(/\\*\\*(.+?)\\*\\*/g, "<strong>$1</strong>");
      safe = safe.replace(/`([^`]+)`/g, "<code>$1</code>");
      safe = safe.replace(/\\[([^\\]]+)\\]\\((https?:\\/\\/[^\\s)]+)\\)/g, '<a href="$2" target="_blank" rel="noopener noreferrer">$1</a>');
      return safe;
    }

    function closeLists(state, html) {
      if (state.inOl) {
        html.push("</ol>");
        state.inOl = false;
      }
      if (state.inUl) {
        html.push("</ul>");
        state.inUl = false;
      }
    }

    function appendListContinuation(state, html, line) {
      if (state.lastLiIndex === null) {
        return false;
      }
      const idx = state.lastLiIndex;
      html[idx] = html[idx].replace("</li>", `<div class="li-sub">${renderInline(line.trim())}</div></li>`);
      return true;
    }

    function markdownToHtml(markdown) {
      const lines = markdown.replace(/\\r\\n?/g, "\\n").split("\\n");
      const html = [];
      const state = {
        inOl: false,
        inUl: false,
        lastLiIndex: null
      };

      for (const rawLine of lines) {
        const line = rawLine.trimEnd();
        const trimmed = line.trim();

        if (!trimmed) {
          closeLists(state, html);
          state.lastLiIndex = null;
          continue;
        }

        if (/^\\s{2,}\\S/.test(rawLine) && (state.inOl || state.inUl)) {
          const used = appendListContinuation(state, html, line);
          if (used) {
            continue;
          }
        }

        if (/^---+$/.test(trimmed)) {
          closeLists(state, html);
          state.lastLiIndex = null;
          html.push("<hr>");
          continue;
        }

        const headingMatch = trimmed.match(/^(#{1,6})\\s+(.+)$/);
        if (headingMatch) {
          closeLists(state, html);
          state.lastLiIndex = null;
          const level = headingMatch[1].length;
          html.push(`<h${level}>${renderInline(headingMatch[2])}</h${level}>`);
          continue;
        }

        const olMatch = trimmed.match(/^(\\d+)\\.\\s+(.+)$/);
        if (olMatch) {
          if (!state.inOl) {
            if (state.inUl) {
              html.push("</ul>");
              state.inUl = false;
            }
            const start = Number(olMatch[1]);
            html.push(start > 1 ? `<ol start="${start}">` : "<ol>");
            state.inOl = true;
          }
          html.push(`<li>${renderInline(olMatch[2])}</li>`);
          state.lastLiIndex = html.length - 1;
          continue;
        }

        const ulMatch = trimmed.match(/^[-*]\\s+(.+)$/);
        if (ulMatch) {
          if (!state.inUl) {
            if (state.inOl) {
              html.push("</ol>");
              state.inOl = false;
            }
            html.push("<ul>");
            state.inUl = true;
          }
          html.push(`<li>${renderInline(ulMatch[1])}</li>`);
          state.lastLiIndex = html.length - 1;
          continue;
        }

        closeLists(state, html);
        state.lastLiIndex = null;
        html.push(`<p>${renderInline(trimmed)}</p>`);
      }

      closeLists(state, html);
      return html.join("\\n");
    }

    function mountReport(markdown) {
      const container = document.querySelector("#report-content");
      if (!container) {
        return;
      }
      if (!markdown || !markdown.trim()) {
        container.innerHTML = '<p class="empty-text">No markdown content found.</p>';
        return;
      }
      container.innerHTML = markdownToHtml(markdown);
    }

    mountReport(typeof REPORT_MARKDOWN === "string" ? REPORT_MARKDOWN : "");
  </script>
</body>
</html>
"""
  return template.replace("__TITLE__", title_escaped).replace("__MARKDOWN__", markdown_payload)


def parse_args() -> argparse.Namespace:
  parser = argparse.ArgumentParser(
    description="Generate a standalone Anthropic-style HTML page from markdown and open it."
  )
  parser.add_argument(
    "markdown",
    nargs="?",
    help="Markdown file path. If omitted, uses --input or info.md.",
  )
  parser.add_argument(
    "-i",
    "--input",
    dest="input_arg",
    help="Markdown source file path (compatible with previous usage).",
  )
  parser.add_argument(
    "-o",
    "--output",
    help="Output HTML file path. Default: <markdown_stem>.html",
  )
  parser.add_argument(
    "--title",
    help="Optional page title. Default: first # heading in markdown, else file stem.",
  )
  parser.add_argument(
    "--no-open",
    action="store_true",
    help="Generate HTML only, do not auto-open browser.",
  )
  return parser.parse_args()


def main() -> int:
  args = parse_args()

  input_path = Path(args.markdown or args.input_arg or "info.md")
  if not input_path.exists():
    raise FileNotFoundError(f"Markdown file not found: {input_path}")

  output_path = Path(args.output) if args.output else input_path.with_suffix(".html")
  output_path.parent.mkdir(parents=True, exist_ok=True)

  markdown = input_path.read_text(encoding="utf-8-sig")
  markdown = normalize_markdown(markdown)

  title = args.title.strip() if args.title else infer_title(markdown, input_path.stem)
  html_doc = build_html(title, markdown)

  output_path.write_text(html_doc, encoding="utf-8")
  print(f"Generated {output_path} from {input_path}")

  if not args.no_open:
    target = output_path.resolve().as_uri()
    opened = webbrowser.open(target)
    if opened:
      print(f"Opened in browser: {target}")
    else:
      print(f"Could not auto-open browser, open manually: {target}")

  return 0


if __name__ == "__main__":
  raise SystemExit(main())
