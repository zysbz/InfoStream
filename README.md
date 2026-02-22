# InfoStream

[English](README_EN.md) | 简体中文

InfoStream 是一个可扩展的信息源采集与每日摘要流水线。它从多个来源抓取更新，统一结构、去重并沉淀可追溯产物，最终输出人读和机读两种摘要结果。

## Overview

InfoStream 面向可重复执行的日常信息流工作：

- 通过插件注册表接入异构信息源。
- 统一为共享的 item 合同结构。
- 落盘原始证据与运行元数据，支持追溯审计。
- 使用 SQLite 做去重与版本管理。
- 通过 AI 生成摘要，并在失败时回退到确定性摘要。

## Features

- 已实现插件：
  - `github_trending`
  - `github_search`
  - `rss_atom`
- `bilibili_up` 已有脚手架，但在 MVP 阶段默认禁用。
- SQLite 目录库用于去重与版本化（`data/catalog.db`）。
- 每次运行生成独立归档目录（`output/YYYYMMDD_HHMM/`）。
- 摘要产物：
  - `digest.md`（人读）
  - `digest.json`（机读）
  - `summary.md`（对 digest 的二次精简摘要）
  - `output/latest.html`（固定复用网页，每次运行覆盖更新）
- DashScope 兼容 LLM 客户端（默认模型 `deepseek-v3.2`）。
- 运行期保护机制：
  - 同日 URL 复用
  - 同日缓存回填
  - GitHub `403` 限流后的来源组冷却

## 快速开始（从安装 `uv` 到首次跑通）

### 1）安装 `uv`

macOS / Linux：

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Windows PowerShell：

```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

安装完成后验证：

```bash
uv --version
python --version
```

### 2）获取项目代码

```bash
git clone <你的仓库地址> InfoStream
cd InfoStream
```

如果你已经有本地代码，直接 `cd` 进入目录即可。

### 3）配置环境变量

先复制模板文件：

macOS / Linux：

```bash
cp .env.example .env
```

Windows PowerShell：

```powershell
Copy-Item .env.example .env
```

然后编辑 `.env`：

```env
DASHSCOPE_API_KEY=your_dashscope_api_key
GITHUB_TOKEN=optional_github_token
```

- `DASHSCOPE_API_KEY`：用于启用 LLM 摘要；未配置时会回退为确定性摘要。
- `GITHUB_TOKEN`：可选，用于提高 GitHub API 配额。

### 4）安装依赖

```bash
uv sync
```

可选（开发/测试依赖）：

```bash
uv sync --extra dev
```

### 5）先做配置校验

```bash
uv run main.py validate-config --sources configs/sources.yaml --run-config configs/run_config.json
```

### 6）运行一次流水线

```bash
uv run main.py run --sources configs/sources.yaml --run-config configs/run_config.json --timeouts configs/timeouts.yaml --output-root output --data-root data
```

### 7）查看结果

- 每次运行归档目录：`output/YYYYMMDD_HHMM/`
- 关键产物：
  - `digest.md`
  - `digest.json`
  - `summary.md`
- 固定复用网页：
  - `output/latest.html`

## Configuration

核心配置文件：

- `configs/sources.yaml`
  - 来源列表（`name`、`type`、`enabled`、`entry_urls`、`params` 等）
  - 转写策略
  - GitHub 搜索关键词
- `configs/run_config.json`
  - 运行策略（`max_items`、`llm_model`、`source_limits`、`timezone`、复用/回填开关、摘要偏好）
  - `llm_model` 可选模型字段，默认 `deepseek-v3.2`（示例：`qwen3.5-397b-a17b`）
  - `max_items` 范围：`1-200`（模型默认 `10`）
- `configs/timeouts.yaml`
  - 请求、来源、全局运行超时

常用运行时覆盖：

```powershell
uv run main.py run --max-items 20
uv run main.py run --add-url https://github.com/trending
uv run main.py run --no-progress
```

## CLI Usage

列出插件：

```powershell
uv run main.py list-plugins
```

校验配置：

```powershell
uv run main.py validate-config --sources configs/sources.yaml --run-config configs/run_config.json
```

执行流水线：

```powershell
uv run main.py run --sources configs/sources.yaml --run-config configs/run_config.json --timeouts configs/timeouts.yaml
```

## Output Layout

每次运行输出目录：

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

单条 item 目录：

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

## Privacy and Security

- 不要提交 `.env` 或真实密钥。
- API Key 仅通过环境变量注入。
- `output/` 与 `data/` 属于本地产物，不应进入版本库。
- 每次推送前检查暂存区：

```powershell
git status --short
git diff --cached --name-only
```

## Roadmap

- 完成可生产使用的 `bilibili_up` 插件。
- 扩展与 C++ 采集核心的 NDJSON 互操作。
- 增加更多来源模板与策略控制能力。

另见：

- `docs/architecture.md`
- `docs/cpp_ndjson_contract.md`

## License

本项目基于 MIT License 发布，详见 [LICENSE](LICENSE)。
