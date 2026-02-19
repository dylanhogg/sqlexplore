# Plan: Mouse Vertical Window Pane Resizing (Single PR)

## Goal
Add mouse-driven vertical resizing for 4 workspace panes in the Textual TUI while preserving all current behavior:
- Pane 1: Query
- Pane 2: Results
- Pane 3: Cell Detail (`#results_preview`)
- Pane 4: Activity

## Scope
- In scope: mouse drag resizing of pane heights in `src/sqlexplore/ui/tui.py`.
- In scope: keep minimum heights for Query, Cell Detail, Activity at current defaults.
- In scope: define and enforce a comparable minimum for Results.
- In scope: when a pane grows, shrink Results first; if Results is already at minimum, shrink the next largest pane.
- In scope: refresh-heavy UI work on mouse release (same spirit as existing column resize).
- Out of scope: persisting pane sizes across app restarts; keyboard-driven pane resizing.

## Non-Negotiables
- No regressions in query run flow, sorting, preview behavior, copy shortcuts, completion menu, sidebar toggle, and activity logging.
- Cross-platform terminal support using Textual-native mouse/events only (`MouseDown`, `MouseMove`, `MouseUp`, capture/release).
- Keep resize logic deterministic and bounded by min-height constraints.

## Phase 1 (Single PR): Implement + Harden

### 1. Add pane sizing model + min-height constants
File: `src/sqlexplore/ui/tui.py`

- Introduce pane ids/state for the 4 resizable panes.
- Add explicit minimum-height constants:
  - Query min = current default (5)
  - Cell Detail min = current default (5)
  - Activity min = current default (5)
  - Results min = same baseline (5)
- Track current explicit pane heights in app state (line-based integer heights).
- Keep all helper methods small and single-purpose:
  - resolve current heights
  - compute shrink capacity per pane
  - apply clamped height updates

### 2. Add vertical splitter widgets between panes
File: `src/sqlexplore/ui/tui.py`

- Add 3 thin draggable splitters (1 row each) between pane boundaries.
- Use a lightweight Textual widget for splitters (e.g. `Static` subclass) with local drag state:
  - active splitter id/index
  - drag start screen y
  - last applied delta
- Use `capture_mouse` / `release_mouse` and stop event propagation during active drag.
- Keep visual treatment minimal and theme-consistent.

### 3. Implement resize allocation policy
File: `src/sqlexplore/ui/tui.py`

- On drag, map movement to a requested delta for one pane to grow/shrink.
- Enforce allocation rule for pane growth:
  - First donor is Results pane (unless Results is target or already at min).
  - If Results cannot donate, donors are remaining panes ordered by current height descending (next largest first).
  - Never violate pane minimums.
- For pane shrink (freeing space), return space to Results first (to preserve default “Results largest” behavior).
- If requested delta exceeds available shrink capacity, clamp to max feasible delta.

### 4. Performance behavior: commit-heavy refresh on mouse release
File: `src/sqlexplore/ui/tui.py`

- During drag: update only pane heights/layout, avoid expensive table/data recomputation.
- On mouse release:
  - finalize persisted pane heights
  - trigger minimal layout/content refresh for affected panes
  - do not call full query/result recompute paths
- Mirror robustness patterns used by column resize:
  - end drag safely on mouse up / blur / capture loss
  - avoid accidental click side effects from drag gestures

### 5. Keep compose/layout clean and explicit
File: `src/sqlexplore/ui/tui.py`

- Refactor workspace compose section just enough to insert splitters without changing feature semantics.
- Ensure existing ids used across app/tests remain stable (`#query_editor`, `#results_table`, `#results_preview`, `#activity_log`).
- Keep Results as the default largest pane on initial mount.

### 6. Tests
File: `tests/sqlexplore/test_app.py`

Add targeted tests for pane resizing:
- Mouse drag on each splitter changes adjacent pane heights as expected.
- Query/Cell Detail/Activity cannot shrink below minimum default height.
- Results cannot shrink below its configured minimum.
- Expanding non-Results pane shrinks Results first.
- If Results already at minimum, expanding non-Results pane shrinks next largest pane.
- Resizing Results pane itself works and respects minimums.
- Existing interaction paths remain intact (sorting, column resize, preview/copy, logging, shortcuts).

## Validation
- `uv run ruff format .`
- `uv run ruff check . --fix`
- `uv run pyright`
- `uv run pytest`

## Exit Criteria
- All 4 panes are vertically resizable with mouse drag.
- Min-height constraints are always respected.
- Space-allocation rule is enforced exactly (Results first, then next largest).
- Existing behavior is unchanged outside pane-size adjustments.
- Full lint/type/test suite passes.
