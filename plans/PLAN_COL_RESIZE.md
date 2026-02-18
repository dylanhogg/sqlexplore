# Plan: Mouse Column Resizing In Results Pane (Single PR)

## Goal
Add mouse-driven column resizing in the Textual Results table while preserving all existing behavior (sorting, keyboard nav, preview updates, copy actions, JSON rendering/highlighting, and overall query flow).

## Scope
- In scope: resize DataTable columns by dragging header boundaries with mouse.
- In scope: keep resized widths stable across in-place redraws of same result set.
- Out of scope: persisted widths across app restarts; row resizing; non-Results tables.

## Non-Negotiables
- No regression in current header click sorting (`asc/desc` toggle).
- No regression in Results preview updates and cell selection behavior.
- No regression in copy shortcuts (`F2`, `F8`) and activity log semantics.
- Cross-platform terminal compatibility (macOS/Linux/Windows) via Textual events only.

## Phase 1 (Single PR): Implement + Harden

### 1. Add resize interaction to `ResultsTable`
File: `src/sqlexplore/ui/tui.py`

- Extend `ResultsTable` with minimal drag state:
  - active column index/key
  - drag start x
  - starting render/content width
  - `did_drag` flag (to distinguish resize vs click)
- Use Textual mouse events (`MouseDown`, `MouseMove`, `MouseUp`) and `capture_mouse` / `release_mouse`.
- Hit-test only header row edge zones (small boundary threshold, e.g. 1-2 cells) to start resize.
- While dragging:
  - compute delta from start x
  - clamp to min width
  - apply width change directly to target DataTable column
  - keep updates incremental (no full table rebuild on every mouse move)
- End drag cleanly on mouse up, blur, or leave-capture edge case.

### 2. Keep header-click sorting behavior intact
File: `src/sqlexplore/ui/tui.py`

- Prevent resize drags from triggering `HeaderSelected` sort actions.
- Keep normal header click behavior unchanged when no drag occurred.
- Preserve existing `on_data_table_header_selected` sorting logic and logs.

### 3. Preserve widths across redraws of same result
File: `src/sqlexplore/ui/tui.py`

- Add width override state on app side (column-index keyed for current result).
- When `_redraw_results_table()` rebuilds columns, apply override widths via DataTable-native column creation path.
- Keep overrides across sort + JSON toggle redraws.
- Reset/realign overrides when a new result schema lands (column count/order mismatch).

### 4. Performance + robustness
File: `src/sqlexplore/ui/tui.py`

- Avoid full `_redraw_results_table()` during live drag.
- Clamp to sane minimum width to prevent unusable columns.
- Keep code path pure UI; no blocking operations.
- Prefer Textual-native structures/events; isolate any unavoidable internals behind small helper methods.

### 5. Tests
File: `tests/sqlexplore/test_app.py`

Add focused tests:
- Header drag resizes target column width.
- Resize drag does not sort.
- Header click still sorts asc/desc (existing test remains).
- Resized widths persist after redraw triggers (sort toggle, JSON toggle).
- Width clamp works at minimum boundary.
- Existing Results interactions (preview/copy/highlighting) remain unchanged in impacted paths.

## Validation
- `uv run ruff format .`
- `uv run ruff check . --fix`
- `uv run pyright`
- `uv run pytest`

## Exit Criteria
- Mouse drag resizing works reliably in Results header.
- Normal header click sorting unchanged.
- No regressions in existing Results pane functionality.
- Full lint/type/test suite green.
