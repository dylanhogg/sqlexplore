# sqlexplore

SQL exploration over data files with your CLI

Currently in development (Feb 2026).

## Usage

Local file:

```bash
uv run sqlexplore ./data/example.parquet
```

Remote file URL:

```bash
uv run sqlexplore https://example.com/data_file.csv
```

Show version:

```bash
uv run sqlexplore --version
```

Remote URL behavior:

- Supports `http://` and `https://` URLs ending in `.csv`, `.tsv`, `.parquet`, or `.pq`.
- Downloads to `./data/downloads/<filename>`.
- If local download target already exists, prints warning and exits (no overwrite by default). Use `--overwrite` to replace it.
- Logs download details before normal app flow: remote/local path, progress, elapsed time, and file size.
- In TUI mode, the Activity pane shows app version on load, then startup download log lines (if any).

Result formatting:

- JSON syntax highlighting is auto-applied for `VARCHAR`/text columns when sampled values look like JSON objects/arrays.
- Detection samples only a few visible rows (not full columns) to keep rendering fast.
- Highlighting is disabled when query result row count is over `100,000`.

https://github.com/dylanhogg/sqlexplore

https://pypi.org/manage/project/sqlexplore/releases/
