# Plan: Add `/save-marimo` MVP Session Export

## Goal
Add a new `/save-marimo` command that writes current sqlexplore session history to a Marimo notebook script file named `marimo_<session_id>.py`, so session analysis can be replayed.

## Scope
- In scope: new helper command, session history-to-notebook export, minimal Marimo script generation, tests.
- In scope: replaying SQL steps from session history in order.
- Out of scope: full command re-execution semantics, rich UI, diffing replay vs original outputs.

## Non-Negotiables
- `/save-marimo` writes `marimo_<session_id>.py`.
- Notebook loads same input datafile(s) used by current session.
- Notebook replays session steps in order as separate cells.
- SQL steps execute in cells; non-replayable commands emit comment entries.

## Implementation Plan

## 1) Add Command Surface
Files:
- `/Users/dylan/_gitdrh/sqlexplore/src/sqlexplore/commands/registry.py`
- `/Users/dylan/_gitdrh/sqlexplore/src/sqlexplore/commands/handlers.py`

Steps:
1. Add `/save-marimo` command spec and help text in command registry.
2. Add `cmd_save_marimo(engine, args)` handler.
3. Keep initial usage simple: no args for MVP.
4. Return success/error response via existing `EngineResponse` helpers.

## 2) Build Session-to-Marimo Exporter
Files:
- `/Users/dylan/_gitdrh/sqlexplore/src/sqlexplore/commands/handlers.py` (MVP inline helper), or
- `/Users/dylan/_gitdrh/sqlexplore/src/sqlexplore/core/marimo_export.py` (if helper grows)

Steps:
1. Use `engine.session_id` to compute filename `marimo_<session_id>.py`.
2. Source ordered steps from `engine.query_history`.
3. Exclude `/save-marimo` entry itself from export.
4. Map history entries:
   - `user_entered_sql`, `command_generated_sql`, `llm_generated_sql` -> SQL replay cell.
   - `user_entered_command` -> comment-only cell (`# command: ...`) unless already represented by generated SQL entry.
5. Write file in current working directory.

## 3) Generate Minimal Marimo Script
File:
- same exporter target from step 2

Steps:
1. Emit valid Marimo script scaffold (`import marimo`, `app = marimo.App()`, `@app.cell`, `if __name__ == "__main__": app.run()`).
2. First setup cell:
   - import `duckdb`
   - open same database target used by engine
   - load/register same input file path(s) and table name as current session.
3. Emit one cell per exported step in original order:
   - SQL cell executes query and renders result.
   - command/comment cell documents non-replayable command.
4. Keep generated code plain and readable; no extra abstraction.

## 4) Tests
Files:
- `/Users/dylan/_gitdrh/sqlexplore/tests/sqlexplore/test_engine_commands.py`

Steps:
1. Add `/save-marimo` command test that runs a short session and exports notebook.
2. Assert file `marimo_<session_id>.py` exists.
3. Assert file includes:
   - Marimo scaffold
   - data-loading/setup cell
   - SQL cells in expected order
   - comment cell(s) for command-only entries.
4. Assert `/save-marimo` command reports success path.

## Validation Commands
- `uv run ruff format .`
- `uv run ruff check . --fix`
- `uv run pyright`
- `uv run pytest`

## Risks and Mitigations
- Risk: command history includes entries not safely replayable.
  - Mitigation: serialize such entries as comment-only cells.
- Risk: loading source files may diverge if files moved after session start.
  - Mitigation: write absolute paths captured from current engine context.
- Risk: duplicate command + generated SQL entries.
  - Mitigation: skip comment cell when adjacent generated SQL already captures effect.

## Done Criteria
- `/save-marimo` is available in help/registry.
- Running `/save-marimo` writes `marimo_<session_id>.py`.
- Generated notebook can load session data source(s) and replay SQL steps in order.
- Non-replayable commands appear as comment entries.
- Lint, type check, and tests pass.
