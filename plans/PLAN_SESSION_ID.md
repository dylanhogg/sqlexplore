# Plan: Add Session ID Across Logs and History Tables

## Goal
Introduce a per-app-launch `session_id` that is initialized on app load, present in all log messages/events, and exposed in tabular history/debug command outputs (for example `/llm-history` and `/history-log`) so operators can correlate behavior to loaded data files and SQL command timelines.

## Scope
- In scope: session initialization lifecycle, logging propagation, engine wiring, command table columns, and tests.
- In scope: adding datafile/session context to startup logs/events for debugging.
- Out of scope: changing command semantics, UI layout redesign, or log storage backend changes.

## Non-Negotiables
- `session_id` created once per app launch.
- All emitted logs/events include `session_id`.
- Table-producing debug/history commands include `session_id` column where relevant.
- Backward compatibility for older log records without `session_id`.

## Implementation Plan

## 1) Session Lifecycle + Logging Core
Files:
- `/Users/dylan/_gitdrh/sqlexplore/src/sqlexplore/core/logging_utils.py`
- `/Users/dylan/_gitdrh/sqlexplore/src/sqlexplore/app.py`

Steps:
1. Add explicit session lifecycle API in `logging_utils.py`:
   - `start_session_id() -> str`
   - `get_session_id() -> str`
2. Replace import-time implicit session creation with startup initialization in `app.py` before first app log line.
3. Add a structured startup event (for example `session.start`) that includes:
   - `session_id`
   - resolved data paths/data sources
   - table name
   - database target
4. Ensure plain logger output includes `session_id` on every line via formatter/filter-level enrichment.

## 2) Thread Session Through Engine/Command Layer
Files:
- `/Users/dylan/_gitdrh/sqlexplore/src/sqlexplore/core/engine.py`
- `/Users/dylan/_gitdrh/sqlexplore/src/sqlexplore/commands/protocols.py`
- `/Users/dylan/_gitdrh/sqlexplore/src/sqlexplore/app.py`

Steps:
1. Add `session_id` field/property to `SqlExplorerEngine`.
2. Pass session id from `app.py` to engine constructor.
3. Extend `CommandEngine` protocol with `session_id`.
4. Keep all existing command execution behavior unchanged.

## 3) Add `session_id` to Relevant Table Outputs
Files:
- `/Users/dylan/_gitdrh/sqlexplore/src/sqlexplore/commands/handlers.py`

Steps:
1. Update `/history` result table to include `session_id`.
2. Update `/history-log` result table to include `session_id` from log events.
3. Update `/llm-history` result table to include `session_id` from log events.
4. Add `session_id` entry in `/llm-show` field/value output.
5. For legacy events missing the field, render empty string (or `unknown`) consistently.

## 4) Verification and Tests
Files:
- `/Users/dylan/_gitdrh/sqlexplore/tests/sqlexplore/test_engine_commands.py`
- `/Users/dylan/_gitdrh/sqlexplore/tests/sqlexplore/test_llm_runner.py` (if protocol stubs need updating)

Steps:
1. Update command table column/row expectations for new `session_id` column.
2. Add/adjust assertions that:
   - query and LLM events carry `session_id`
   - `/history-log` and `/llm-history` show expected `session_id`
   - startup session event records datafile context
3. Confirm no regressions in rerun/history behavior.

## Validation Commands
- `uv run ruff format .`
- `uv run ruff check . --fix`
- `uv run pyright`
- `uv run pytest`

## Risks and Mitigations
- Risk: log format changes break parsing.
  - Mitigation: keep `event_json=` payload stable; only add fields.
- Risk: protocol updates break test fakes.
  - Mitigation: update fake engines in tests with minimal `session_id` attribute.
- Risk: old logs missing `session_id`.
  - Mitigation: tolerant extraction with default value.

## Done Criteria
- `session_id` is initialized once at app load.
- All app log lines and structured events include `session_id`.
- `/history`, `/history-log`, and `/llm-history` expose `session_id` in results.
- Session startup logging/event captures data sources used by that session.
- Lint, type check, and tests pass.
