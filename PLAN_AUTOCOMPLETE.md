# Column-Aware SQL Autocomplete Plan (`app.py`)

## Goal
When user types aggregate calls like `SELECT MAX(`, autocomplete should prioritize real columns, not placeholder snippets like `MAX(value)`.

## Problem (Current)
- `src/sqlexplore/app.py:924` builds aggregate snippets once via `_aggregate_completion_items`.
- If no numeric column found, it falls back to `"value"` (`src/sqlexplore/app.py:927-930`).
- Completion is clause-aware (`SELECT`), not function-argument-aware, so inside `MAX(` we still show broad snippets.

## UX Target
- Typing `MAX(`, `MIN(`, `SUM(`, `AVG(`, `COUNT(` opens menu with valid columns immediately.
- Top results depend on function semantics:
  - `SUM/AVG`: numeric columns first.
  - `MIN/MAX`: all columns, numeric/date first.
  - `COUNT`: `*` + columns (`COUNT(DISTINCT col)` available).
- Accepting a completion inside `MAX(` inserts only column token (or `*` for `COUNT`), no forced alias.
- No placeholder `value` text shown in aggregate suggestions.

## Implementation Plan

### 1) Add function-argument context detection in `CompletionEngine`
- Extend `CompletionContext` with:
  - `sql_function: str | None`
  - `inside_function_args: bool`
- In `CompletionEngine._sql_context`:
  - Detect nearest unmatched `(` before cursor.
  - Read token immediately before `(`; if aggregate function name, set context.
  - Keep current clause detection as fallback.
- Handle incomplete SQL safely (same robustness as current partial parsing path).

### 2) Add aggregate-argument suggestion providers in `SqlExplorerEngine`
- Add:
  - `_column_completion_items_for_aggregate(func_name: str) -> list[CompletionItem]`
  - Optional helper for typed ranking (`_aggregate_arg_score(...)`).
- Ranking rules:
  - Prefer exact/prefix match.
  - Apply type bonus by function.
  - Penalize non-ideal types, but still allow them when needed.
- Always return real schema-derived columns; never synthesize `"value"`.

### 3) Update generic aggregate snippets to be schema-based
- Refactor `_aggregate_completion_items`:
  - Build snippets from real columns selected from schema.
  - Example: `SUM(amount) AS sum_amount` instead of `SUM(value) AS total`.
- If no ideal typed column exists:
  - Use best real fallback column.
  - Keep detail text explicit (`"non-numeric fallback"`), but still valid SQL.

### 4) Route completion flow by context
- In `CompletionEngine.get_items`:
  - If SQL mode + `inside_function_args`, source candidates from aggregate-arg provider.
  - Else keep current clause-based behavior.
- Preserve existing replacement span logic so partial prefixes still replace correctly.

### 5) UX polish in completion labels/details
- Show concise details:
  - `column | INT` for numeric picks
  - `column | VARCHAR` for string picks
  - `* | count rows` for `COUNT(*)`
- Keep top list stable (deterministic ordering) to reduce cognitive load.

### 6) Tests (must add before merge)
- Extend `tests/sqlexplore/test_engine_commands.py` with cases:
  - `SELECT MAX(` suggests real columns; does not include `MAX(value)`.
  - `SELECT SUM(` ranks numeric column before string column.
  - `SELECT COUNT(` includes `*` and column options.
  - `SELECT MAX(co` filters to matching column prefix.
  - Quoted identifier path still works (`SELECT MAX("co`).
  - Regression: existing clause-aware completions still pass.

## Rollout Steps
1. Implement context detection + provider methods.
2. Add/adjust tests for aggregate-arg contexts.
3. Refactor aggregate snippets to schema-based columns.
4. Run `uv run ruff format .`, `uv run ruff check . --fix`, `uv run pyright`, `uv run pytest`.

## Acceptance Criteria
- In query editor, typing `SELECT MAX(` shows real columns from active table.
- Placeholder `value` is not shown in aggregate completions.
- Suggestion ordering reflects function/type relevance.
- Existing completion behavior for non-function contexts remains intact.
- Test suite passes with new coverage for aggregate argument contexts.
