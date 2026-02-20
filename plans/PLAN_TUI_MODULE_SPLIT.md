# Plan: Split `tui.py` Into Focused UI Modules

## Goal
Improve readability, navigation, and maintainability by splitting `/Users/dylan/_gitdrh/sqlexplore/src/sqlexplore/ui/tui.py` into focused files, while preserving all behavior and public imports.

## Scope
- In scope: move `SqlExplorerTui`, `SqlQueryEditor`, `ResultsTable`, `ResultsPreview`, `PaneSplitter`, and related local types/helpers into dedicated modules.
- In scope: keep existing runtime behavior, keyboard/mouse interactions, and tests unchanged.
- In scope: keep compatibility for current imports from `sqlexplore.ui.tui`.
- Out of scope: feature changes, visual redesign, new shortcuts, performance rewrites beyond import/module boundaries.

## Non-Negotiables
- No user-visible behavior changes.
- No test regressions.
- No import breakage for:
  - `/Users/dylan/_gitdrh/sqlexplore/src/sqlexplore/app.py`
  - `/Users/dylan/_gitdrh/sqlexplore/tests/sqlexplore/test_app.py`
- Keep modules small and cohesive; avoid circular imports.

## Target Module Layout
- `/Users/dylan/_gitdrh/sqlexplore/src/sqlexplore/ui/tui.py`
  - compatibility facade/re-exports only (thin shim)
- `/Users/dylan/_gitdrh/sqlexplore/src/sqlexplore/ui/tui_app.py`
  - `SqlExplorerTui`
- `/Users/dylan/_gitdrh/sqlexplore/src/sqlexplore/ui/query_editor.py`
  - `SqlQueryEditor`, completion mode helpers
- `/Users/dylan/_gitdrh/sqlexplore/src/sqlexplore/ui/results_table.py`
  - `ResultsTable`, column resize state/types
- `/Users/dylan/_gitdrh/sqlexplore/src/sqlexplore/ui/results_preview.py`
  - `ResultsPreview`, preview content alias
- `/Users/dylan/_gitdrh/sqlexplore/src/sqlexplore/ui/pane_splitter.py`
  - `PaneSplitter`, pane splitter drag state
- `/Users/dylan/_gitdrh/sqlexplore/src/sqlexplore/ui/tui_shared.py`
  - shared constants, type aliases, pure helper functions used across modules

## Phase 1: Extract Shared, Pure Pieces First
Files:
- new: `src/sqlexplore/ui/tui_shared.py`
- edit: `src/sqlexplore/ui/tui.py`

Steps:
1. Move pure helpers/constants/type aliases to `tui_shared.py` (no widget/app imports).
2. Keep signatures unchanged.
3. Update `tui.py` to import from `tui_shared.py`.
4. Run lint/type/tests.

Why first:
- Reduces copy/paste in later extraction.
- Low risk because pure functions are easiest to validate.

## Phase 2: Extract Widget Classes
Files:
- new: `query_editor.py`, `results_table.py`, `results_preview.py`, `pane_splitter.py`
- edit: `tui.py`, `tests/sqlexplore/test_app.py` (only if import paths change)

Steps:
1. Move each widget class with only its direct dependencies.
2. Keep class names and constructor signatures unchanged.
3. Move each widget’s private dataclasses/state to same module (or `tui_shared.py` if reused).
4. In `tui.py`, temporarily re-export:
   - `SqlQueryEditor`
   - `ResultsTable`
   - `ResultsPreview`
   - `PaneSplitter`
5. Run lint/type/tests.

## Phase 3: Extract `SqlExplorerTui`
Files:
- new: `tui_app.py`
- edit: `tui.py`, `src/sqlexplore/app.py`

Steps:
1. Move `SqlExplorerTui` to `tui_app.py`.
2. Keep `tui.py` as thin facade importing/re-exporting from new modules.
3. Optional final cleanup: switch internal imports (e.g. `app.py`) to direct modules, but retain `tui.py` exports for backward compatibility.
4. Run lint/type/tests.

## Phase 4: Compatibility + Cleanup
Files:
- edit: `tui.py`, any direct imports touched in `src/` and `tests/`

Steps:
1. Ensure `sqlexplore.ui.tui` remains stable import surface.
2. Add explicit `__all__` in `tui.py` facade to document exported symbols.
3. Remove dead imports and duplicate constants left after extraction.
4. Keep file/module names simple and consistent.

## Validation Per Phase
- `uv run ruff format .`
- `uv run ruff check . --fix`
- `uv run pyright`
- `uv run pytest`

## Risks and Mitigations
- Risk: circular imports between `tui_app` and widget modules.
  - Mitigation: keep shared types/helpers in `tui_shared.py`; widget modules should not import `SqlExplorerTui`.
- Risk: subtle behavior changes from moved private helpers.
  - Mitigation: preserve function signatures and run full suite after each phase.
- Risk: breaking external imports.
  - Mitigation: keep `tui.py` shim with re-exports until a deliberate deprecation cycle.

## Exit Criteria
- `tui.py` is a thin compatibility layer.
- Large classes live in dedicated files.
- Full test/lint/type suite passes.
- Existing `from sqlexplore.ui.tui import ...` imports continue to work.
