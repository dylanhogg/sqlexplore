# sqlexplore

[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![PyPI version](https://badge.fury.io/py/sqlexplore.svg?1)](https://badge.fury.io/py/sqlexplore)
[![build](https://github.com/dylanhogg/sqlexplore/actions/workflows/ci.yml/badge.svg)](https://github.com/dylanhogg/sqlexplore/actions/workflows/ci.yml)
[![Latest Tag](https://img.shields.io/github/v/tag/dylanhogg/sqlexplore)](https://github.com/dylanhogg/sqlexplore/tags)
[![Downloads](https://static.pepy.tech/badge/sqlexplore)](https://pepy.tech/project/sqlexplore)

`sqlexplore` is a terminal SQL explorer for flat files (`.csv`, `.tsv`, `.txt`, `.parquet`, `.pq`), powered by DuckDB.

Use it when you need quick answers from local files, URLs, or piped terminal output without building a separate pipeline first.

## Useful features

- Interactive TUI with query editor, results grid, cell preview, and activity log.
- Non-interactive mode (`--no-ui`) for one-shot queries in plain terminal output.
- SQL helper commands for common analysis and shaping tasks:
  `/summary`, `/describe`, `/profile`, `/hist`, `/corr`, `/top`, `/dupes`, `/crosstab`,
  `/sample`, `/filter`, `/sort`, `/group`, `/agg`.
- LLM-assisted SQL generation with `/llm-query`, plus trace tools (`/llm-history`, `/llm-show`).
- Query history and rerun helpers (`/history`, `/rerun`, `/history-log`, `/rerun-log`).
- Context-aware autocomplete for SQL and helper commands.
- Result export via `/save` to `.csv`, `.parquet`/`.pq`, or `.json`.
- JSON-aware table rendering and preview, including compact image-cell tokens for image-like values.
- Local files, HTTP(S) URLs, and stdin input (for piped text).
- Multiple sources via repeated `--data` (schemas must match; sources are unioned).
- Remote download cache controls with `--download-dir` and `--overwrite`.
- `.txt` input support with derived fields like `line_number`, `line_length`, `word_count`, and `line_hash`.

## Run with uvx (preferred)

Requires Python 3.13+.

Run directly without a manual install:

```bash
uvx sqlexplore --data my_data.parquet
```

Equivalent explicit form:

```bash
uvx --from sqlexplore sqlexplore --data my_data.parquet
```

Run one query and exit:

```bash
uvx sqlexplore --data https://github.com/dylanhogg/awesome-python/raw/refs/heads/main/github_data.parquet --execute "SELECT COUNT(*) AS n FROM data" --no-ui
```

Run SQL from file and exit:

```bash
uvx sqlexplore --data https://github.com/dylanhogg/awesome-python/raw/refs/heads/main/github_data.parquet --file ./queries/report.sql --no-ui
```

Use multiple inputs:

```bash
uvx sqlexplore --data ./data/january.parquet --data ./data/february.parquet
```

Analyze piped terminal text:

```bash
ls -lha | uvx sqlexplore
```

Open remote data (downloaded then loaded):

```bash
uvx sqlexplore --data https://github.com/dylanhogg/awesome-python/raw/refs/heads/main/github_data.parquet
```

## Install with pip

```bash
pip install sqlexplore
sqlexplore --data https://github.com/dylanhogg/awesome-python/raw/refs/heads/main/github_data.parquet
```

## LLM usage (optional)

Set an API key for your chosen LiteLLM provider (for example `OPENAI_API_KEY`), then run:

```sql
/llm-query top 10 customers by total revenue
```

Optional model override:

```bash
export SQLEXPLORE_LLM_MODEL=openai/gpt-5-mini
```

## Notes

- `--data` can be omitted when piping stdin.
- If stdin has no controlling TTY, sqlexplore falls back to `--no-ui`.
- `--limit` sets default helper query limit. `/limit` also updates row display limit.
- Logs are written to `sqlexplore.log` in your app log directory (with fallbacks).
- Run `sqlexplore --help` to view all options.

## Links

- [GitHub](https://github.com/dylanhogg/sqlexplore)
- [PyPI](https://pypi.org/project/sqlexplore/)
