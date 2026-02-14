# sqlexplore

Get answers from messy data files fast, without building pipelines first.

With `sqlexplore`, you can:

- Inspect new datasets in minutes instead of writing setup scripts.
- Ask ad hoc questions with SQL and get immediate feedback.
- Move from quick profiling to exportable results in one terminal workflow.

`sqlexplore` is a terminal SQL workbench for flat files. Point it at a local or remote dataset, then explore with DuckDB SQL in an interactive TUI or run one-shot queries in standard CLI mode.

## Useful features

- Works with `.csv`, `.tsv`, `.txt`, `.parquet`, and `.pq`.
- Accepts local paths and `http(s)` URLs for supported file types.
- Interactive TUI with query editor, results grid, preview pane, and activity log.
- SQL + helper commands for common analysis tasks: `/summary`, `/profile`, `/hist`, `/corr`, `/dupes`, `/top`, `/group`, and more.
- Context-aware autocomplete for SQL clauses and helper command arguments.
- Query history and rerun support (`/history`, `/rerun`), plus editor helpers (`/last`, `/clear`).
- Export last result to `.csv`, `.parquet`/`.pq`, or `.json` with `/save`.
- JSON-aware rendering in result cells and clickable links in preview.
- Image bytes in `BLOB` or `STRUCT{bytes,path}` cells render as compact `[img ...]` tags with metadata in preview.
- Non-interactive mode via `--execute`, `--file`, or `--no-ui`.
- Remote download controls: custom directory (`--download-dir`) and overwrite behavior (`--overwrite`); existing local downloads are reused by default.
- `.txt` files are ingested line-by-line with derived metrics (`line_number`, `word_count`, `line_hash`, etc).

## Usage examples

Open a local file in the TUI:

```bash
sqlexplore ./data/example.parquet
```

Open a remote dataset URL (downloaded first, then loaded):

```bash
sqlexplore https://github.com/dylanhogg/awesome-python/raw/refs/heads/main/github_data.parquet
```

Run one SQL query and exit:

```bash
sqlexplore ./data/example.parquet --execute "SELECT COUNT(*) AS n FROM data"
```

Run SQL from a file and exit:

```bash
sqlexplore ./data/example.parquet --file ./queries/report.sql
```

Run default sample query in plain terminal output (no TUI):

```bash
sqlexplore ./data/example.csv --no-ui
```

Control remote download location and overwrite:

```bash
sqlexplore https://github.com/dylanhogg/awesome-python/raw/refs/heads/main/github_data.parquet --download-dir ./data/cache --overwrite
```

Typical helper commands in the editor:

```sql
/summary
/profile amount
/top category 10
/hist amount 20 | amount > 0
/corr tip_amount total_amount
/dupes order_id
/save ./out/results.parquet
```

Show installed version:

```bash
sqlexplore --version
```

## Install

Requires Python 3.13+.

```bash
pip install sqlexplore
```

or:

```bash
uv tool install sqlexplore
```

## Links

- [GitHub](https://github.com/dylanhogg/sqlexplore)
- [PyPI](https://pypi.org/project/sqlexplore/)
