# Plan: Multi-File Union Into `data`

## Goal
Support loading one or many data sources at startup via CLI, validate identical schema, and expose a single DuckDB `data` table that behaves like today (now backed by `UNION ALL` when multiple sources are provided).

## Scope
- Replace positional `[DATA]` with repeatable `--data`.
- Accept local file paths and HTTP(S) URLs (same behavior as now).
- Validate all sources share exact schema (column order, names, types) before union.
- If schema mismatch: print clear CLI error, exit.
- Add `/tables` helper command showing loaded/unioned tables + row counts.
- Keep existing SQL/TUI flow centered on `data`.

## CLI UX Contract
- Single source:
  - `sqlexplore --data data.csv`
- Multi-source:
  - `sqlexplore --data jan.parquet --data feb.parquet --data mar.parquet`
  - `sqlexplore --data https://host/a.parquet --data ./b.parquet`
- Stdin keeps working:
  - `cat lines.txt | sqlexplore`
  - `cat lines.txt | sqlexplore --data -`
- Invalid usage:
  - `--data -` mixed with other `--data` values => error (`stdin` must be exclusive).
  - no `--data` and stdin is tty => existing missing-source error (updated wording to mention `--data`).

## Phase 1: Prep For Implementation

### 1. Lock data-loading architecture (minimal disruption)
- Keep source path resolution in `src/sqlexplore/app.py`.
- Keep table creation and query behavior in `src/sqlexplore/core/engine.py`.
- Extend, don’t replace:
  - Add multi-source path resolution helpers beside `_resolve_main_data_source`.
  - Extend engine init to accept multiple resolved paths while preserving single-source path.

### 2. Define schema validation contract
- Canonical schema for each source: ordered `(column_name, column_type)`.
- Equality rule: exact same length + same column name at each position + same type at each position.
- Validation should run on inferred relation schema (`LIMIT 0`/`DESCRIBE`) to avoid full scans.
- Error format (CLI-visible, actionable):
  - which source failed (index + path/url),
  - what mismatched (count/name/type),
  - expected vs actual details.

### 3. Define table model for union and observability
- Per-source views (stable generated names), e.g. `data_src_1`, `data_src_2`, ...
- Final user-facing view remains exactly `data`:
  - `CREATE VIEW "data" AS SELECT * FROM "data_src_1" UNION ALL SELECT * FROM "data_src_2" ...`
- Store startup table metadata in engine for `/tables`:
  - logical table name,
  - source path/url,
  - row count,
  - role (`source` vs `union`).

### 4. Define helper command integration point
- Add `/tables` in existing command path:
  - constants + handler in `src/sqlexplore/commands/handlers.py`,
  - registration in `src/sqlexplore/commands/registry.py`,
  - protocol method(s) in `src/sqlexplore/commands/protocols.py`,
  - include in fallback helper list in `src/sqlexplore/completion/models.py`.

### 5. Prep test matrix (before edits)
- CLI resolution and arg parsing:
  - `tests/sqlexplore/test_main_data_source.py`
  - `tests/sqlexplore/test_stdin_io.py`
- Engine + helper behavior:
  - `tests/sqlexplore/test_engine_commands.py`
  - add focused multi-source engine tests (new file recommended: `tests/sqlexplore/test_engine_multi_source.py`)
- Completion/helper list checks:
  - `tests/sqlexplore/test_completion_edges.py`
  - `tests/sqlexplore/test_app.py` (helper completion count/prompt behavior)

### Phase 1 exit criteria
- Clear implementation spec for CLI semantics, schema matching, error format, `/tables` output columns.
- File-level change list and test plan finalized.

## Phase 2: Feature-Complete Implementation

### 1. CLI changes (`app.py`, `stdin_io.py`)
- Replace `data` positional argument with repeatable option:
  - `data: list[str] | None = typer.Option(None, "--data", help=...)`
- Add resolver that returns multiple paths:
  - local files: existing checks reused.
  - HTTP URLs: existing download flow reused per source.
  - stdin (`-` or piped input with no `--data`) returns single temp file source.
- Preserve existing startup activity logging for downloads/stdin.

### 2. Engine multi-source load + strict schema validation (`engine.py`)
- Extend engine constructor to accept `data_paths: tuple[Path, ...]` (or equivalent).
- For each source:
  - detect reader via existing `_detect_reader`,
  - build load SQL via existing `render_load_query`,
  - create per-source view (`data_src_n`).
- Validate schemas across source views before creating final `data` view.
- On mismatch:
  - raise `typer.BadParameter` with detailed mismatch message.
  - startup aborts cleanly.
- Create final `data` view as `UNION ALL` of source views.
- Keep existing `default_query`, `row_count`, schema refresh, completion behavior unchanged from user perspective.

### 3. `/tables` helper command
- Add engine accessor for startup table metadata (source/union row counts).
- Implement `/tables` command:
  - no args,
  - tabular response via existing `engine.table_response(...)`,
  - include union table and each source table.
- Register command so `/help`, completions, highlighting, and history all work automatically.

### 4. Error UX and messages
- Schema mismatch errors should be explicit and compact, e.g.:
  - `Schema mismatch in source 2: /path/b.csv`
  - `Expected col[3] amount DOUBLE, got amount VARCHAR`
- CLI exits non-zero on mismatch and invalid source combinations.

### 5. Tests (complete)
- Update existing CLI tests to new `--data` syntax (single local, single remote, overwrite, custom download dir, stdin).
- Add multi-source success tests:
  - two files same schema -> `SELECT COUNT(*) FROM data` equals summed rows.
  - `/tables` returns union + source rows.
- Add mismatch failure tests:
  - different column count,
  - same name different type,
  - same columns different order.
- Update helper command expectations where command set is asserted.

### 6. Validation run
- `uv run ruff format .`
- `uv run ruff check . --fix`
- `uv run pyright`
- `uv run pytest`

### Phase 2 exit criteria
- `--data` supports one or many sources (local/HTTP) with current UX quality.
- `data` behaves as single logical table backed by validated `UNION ALL`.
- Mismatched schemas fail fast with clear CLI details.
- `/tables` provides startup-loaded table visibility with row counts.
- Full test suite green.
