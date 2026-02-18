# Multi-Table Data Loading Plan (Implemented)

## Goal
Support loading multiple data files with a clean CLI + TUI UX in two modes:
1. `union`: all files share one logical table (default `data`) via `UNION ALL`.
2. `tables`: each file loads into its own named table for joins/custom SQL.

Keep existing single-file behavior unchanged.

## UX Spec
- Positional `data` arg accepts one or more sources.
- `--load-mode {union,tables}` controls how multiple files are exposed.
- `--table/-t` keeps current meaning (primary/default table name).
- `--table-name` (repeatable) names each source table in `tables` mode.
- `--active-table` sets helper-command target table in `tables` mode.
- Startup activity logs show resolved mode, table mappings, active table.

## Phase 1: App Layer CLI Parsing
- Changed CLI `data` from single path to `list[str] | None`.
- Added options: `--load-mode`, `--table-name`, `--active-table`.
- Kept existing single-file CLI path valid.

## Phase 2: App Layer Data Source Resolution
- Added `_resolve_data_sources(...)` returning normalized source paths/stdin info.
- Added source-config builder to validate and normalize:
  - union mode constraints
  - tables mode names, dedupe, active-table checks
- Built engine init args from resolved config.

## Phase 3: Engine Model
- Added `DataLoadMode` and `DataSourceBinding` model usage in engine init.
- Engine tracks:
  - `load_mode`
  - `data_sources`
  - `table_names`
  - active `table_name`
- Added table lookup/switch helpers and schema refresh per active table.

## Phase 4: Engine Load Path
- `union` mode:
  - single file: existing behavior
  - multi-file: `UNION ALL` view into primary table
- `tables` mode:
  - creates one view per named source table
- Existing reader detection/load-query generation reused.

## Phase 5: Schema Handling
- `schema_preview()` now shows mode, active table/source, loaded tables.
- `help_text()` includes active + loaded tables.
- `refresh_schema(table_name=...)` supports switching active table safely.

## Phase 6: Helper UX
- Added `/tables` helper command to list loaded tables and active marker.
- Added `/use <table>` helper command to switch active table.
- `/schema` message now includes active table context.

## Phase 7: Completion UX
- Completion model/protocols now include `table_names`.
- SQL completions suggest all loaded tables in relevant clauses.
- Added `/use` argument completion from loaded tables.
- Added `/tables` and `/use` into helper command defaults.

## Phase 8: TUI UX
- Results header includes active table label: `[table:<name>]`.
- Header refreshes on both result and non-result responses (e.g. `/use`).
- Sidebar schema panel updates after table switch.

## Phase 9: LLM Correctness
- Extended prompt constraints from single allowed table to allowed table set in tables mode.
- Extended SQL validation from one table to allowed table set (+ CTE allowances).
- Runner now passes engine allowed tables into default LLM prompt/repair/validation deps.
- Single-table wording/behavior remains intact when only one table is allowed.

## Phase 10: Backward Compatibility
- Single-file constructor path preserved:
  - defaults to `load_mode="union"`
  - active table remains primary table
  - existing integrations/tests keep same call shape and behavior
- New params are additive/optional on LLM prompt/validation functions.

## Main Files Updated
- `src/sqlexplore/app.py`
- `src/sqlexplore/core/engine.py`
- `src/sqlexplore/commands/handlers.py`
- `src/sqlexplore/commands/registry.py`
- `src/sqlexplore/commands/protocols.py`
- `src/sqlexplore/completion/completions.py`
- `src/sqlexplore/completion/models.py`
- `src/sqlexplore/completion/protocols.py`
- `src/sqlexplore/ui/tui.py`
- `src/sqlexplore/llm/llm_sql.py`
- `src/sqlexplore/commands/llm_runner.py`

## Validation
- Lint/type checks pass (`ruff`, `pyright`).
- Test suite passes after implementation (including multi-table + LLM + compatibility regressions).
