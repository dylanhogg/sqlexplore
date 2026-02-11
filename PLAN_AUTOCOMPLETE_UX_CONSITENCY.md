# PR Plan: Autocomplete UX Consistency

## Name
`Autocomplete UX Consistency: context-driven triggers, unified mode, predictable navigation`

## Scope
1. Unify trigger semantics around engine context.
2. Replace dual bool state with explicit completion mode.
3. Make visible-menu navigation consistent.
4. Improve trigger coverage for expected SQL/slash contexts.
5. Tighten fallback behavior to reduce irrelevant suggestions.
6. Clarify acceptance controls in-UI.
7. Expand regression coverage and docs.

## Primary Acceptance Criteria
1. Menu auto-opens when engine says context is completable, incl `SELECT ` and `/top `.
2. If menu is visible, `Up/Down` always navigates menu.
3. `Tab` always accepts selected item when menu visible; otherwise indents.
4. Slash-arg contexts do not fall back to command-name suggestions once a known command is established.
5. UI shows explicit completion controls hint when menu is visible.
6. Existing click selection remains stable; no stale-index `IndexError`.

## Implementation Workstreams

### 1) Unify trigger semantics around engine context
1. Change completion provider contract from `list[CompletionItem]` to result object with: `items`, `should_auto_open`, `context_mode`, `reason`.
2. Add dataclass in `/Users/dylan/_gitdrh/sqlexplore/src/sqlexplore/engine.py` (e.g., `CompletionResult`).
3. Add method `SqlExplorerEngine.completion_result(...)`.
4. Keep temporary compatibility wrapper `completion_items(...)` during migration.
5. Update `/Users/dylan/_gitdrh/sqlexplore/src/sqlexplore/tui.py` to use engine `should_auto_open` instead of `_has_auto_completion_prefix`.

### 2) Replace `_completion_open` + `_completion_manual_open` with explicit mode enum
1. Add enum in `/Users/dylan/_gitdrh/sqlexplore/src/sqlexplore/tui.py`: `closed | auto | manual`.
2. Replace transitions in `_refresh_completion_state`, `dismiss_completion_menu`, `on_focus`, `on_blur`, `_on_key`.
3. Remove regex-based `_has_auto_completion_prefix` after engine-driven gating is complete.

### 3) Make visible-menu navigation consistent
1. In `/Users/dylan/_gitdrh/sqlexplore/src/sqlexplore/tui.py`, when mode != `closed`, `Up/Down` always navigates menu.
2. History/cursor `Up/Down` runs only when mode == `closed`.
3. Keep `Esc` as explicit close action.
4. Keep current click-selection guards and index safety path.

### 4) Improve trigger coverage for expected contexts
1. In `/Users/dylan/_gitdrh/sqlexplore/src/sqlexplore/engine.py`, set `should_auto_open=True` for:
   - Slash command name and slash argument positions (`/cmd `, `/cmd a`, piped helper args).
   - SQL clause boundaries with candidates (`SELECT `, `FROM `, `WHERE `, `GROUP BY `, `ORDER BY `, `LIMIT `).
   - Aggregate function arg contexts (`SUM(`, `COUNT(`).
2. Keep `should_auto_open=False` inside unterminated single-quoted literal.

### 5) Tighten fallback behavior
1. In helper mode in `/Users/dylan/_gitdrh/sqlexplore/src/sqlexplore/engine.py`, remove fallback to command-name candidates when a known helper command has no arg matches.
2. Keep command-name suggestions for unknown command-token typing only.

### 6) Clarify acceptance controls
1. In `/Users/dylan/_gitdrh/sqlexplore/src/sqlexplore/tui.py`, show visible hint near completion menu:
   - `Tab accept • Esc close • Up/Down navigate`
2. Keep `Enter` behavior unchanged in this PR (newline, not accept).
3. Update help text in `/Users/dylan/_gitdrh/sqlexplore/src/sqlexplore/engine.py` to match new navigation semantics.

### 7) Tests + regression hardening
1. Update/add app interaction tests in `/Users/dylan/_gitdrh/sqlexplore/tests/sqlexplore/test_app.py`:
   - auto-open on `SELECT ` and `/top `
   - visible-menu `Up/Down` always navigate
   - history `Up/Down` only when menu closed
   - `Tab` accept vs indent fallback
   - completion hint visibility when menu visible
2. Add engine tests in `/Users/dylan/_gitdrh/sqlexplore/tests/sqlexplore/test_engine_commands.py`:
   - `CompletionResult.should_auto_open` across SQL/slash contexts
   - no helper-name fallback in known command arg mode
   - no auto-open in unterminated single-quoted literal
3. Keep existing click-selection regressions.

## Docs / Changelog
1. Update `/Users/dylan/_gitdrh/sqlexplore/README.md` autocomplete behavior section.
2. Keep `/help` text in sync with final key behavior.
3. Add short interaction-model note for consistency expectations.

## Suggested Commit Sequence
1. Engine result model + auto-open semantics.
2. TUI mode-enum refactor + state transitions.
3. Navigation consistency changes.
4. Fallback tightening.
5. Completion hint UI + help text sync.
6. Test suite updates.
7. README update.

## Pre-Implementation Checklist
1. Confirm no external caller depends on `completion_items(...)` exact signature.
2. Confirm `Enter` remains newline/no-accept for this PR.
3. Confirm completion-hint placement (under menu vs inline row) before coding.

