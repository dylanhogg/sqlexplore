# Refactor Plan

## PR1: Engine Command Core Simplification (low risk)
- Scope: `src/sqlexplore/engine.py`
- Extract shared command helpers for:
  - usage parsing
  - column resolution
  - numeric column validation
  - optional `| where` handling
- Refactor `/hist`, `/crosstab`, `/corr`, `/dupes`, `/summary` command flow to reuse shared helpers.
- Keep unchanged:
  - command names/usages
  - SQL generated
  - response/error messages
- Test gate:
  - `uv run pytest -q tests/sqlexplore/test_engine_commands.py`
  - `uv run pytest -q`

## PR2: Completion System Consolidation (medium risk)
- Scope: `src/sqlexplore/engine.py`
- Unify repeated completion builders:
  - column completion item generation
  - dedupe/merge/rank logic
  - helper arg completion branch handling
- Reduce `_complete_*` duplication using compact rule-driven helpers.
- Keep unchanged:
  - completion labels/insert text
  - scoring outcomes
  - auto-open triggers/reasons
- Test gate:
  - completion-focused tests in `tests/sqlexplore/test_engine_commands.py`
  - TUI completion tests in `tests/sqlexplore/test_app.py`
  - full suite

## PR3: TUI/App/Test Cleanup + Shared Utilities (low-medium risk)
- Scope:
  - `src/sqlexplore/tui.py`
  - `src/sqlexplore/app.py`
  - `src/sqlexplore/image_cells.py`
  - `tests/sqlexplore/*`
- Consolidate repeated TUI branches:
  - cell render path (JSON/image/scalar/link)
  - preview update path
  - cursor-motion boilerplate
- Extract shared utility helpers used in multiple modules.
- Reduce repeated test scaffolding via fixtures/factories.
- Keep unchanged:
  - visual output behavior
  - keyboard shortcuts
  - logging text/content
- Test gate:
  - `uv run pytest -q tests/sqlexplore/test_app.py`
  - `uv run pytest -q tests/sqlexplore/test_main_data_source.py`
  - full suite
