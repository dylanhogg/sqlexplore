# Struct Type Support Plan

Plan split into 3 workstreams (as requested).

## 1. Struct Type Metadata Backbone

1. Add STRUCT type helpers in `src/sqlexplore/engine.py`:
   - `is_struct_type(type_name: str)`
   - `parse_struct_fields(type_name: str)`
   - `flatten_struct_paths(...)`
2. Cache parsed type trees (`@cache`) so autocomplete/rendering do not re-parse on each keystroke/redraw.
3. Store per-column struct metadata on engine refresh (for example, `self.struct_fields_by_column`).

## 2. Visualization (STRUCT Similar to JSON Highlighting)

1. Extend `src/sqlexplore/tui.py` rendering pipeline:
   - `_detect_json_columns` -> generalized typed detection including STRUCT columns.
2. For STRUCT values (DuckDB returns Python `dict`), render via compact JSON text (`json.dumps(..., separators=(",", ":"))`) + `JSONHighlighter`.
3. Keep fallback to `format_scalar` for non-dict/invalid values so rendering is safe.
4. Preserve sort behavior by continuing to sort raw row data, not rendered `Text`.

## 3. Autocomplete for STRUCT Fields

1. Extend completion context in `src/sqlexplore/engine.py` to detect dotted paths before cursor (for example, `address.`, `address.cit`).
2. Resolve first segment to a known struct column; walk nested struct tree; suggest field names + types.
3. Return replacement range for only the suffix after the last dot, so accept/Tab inserts just the field token.
4. Integrate field suggestions into SQL clause completion and aggregate-arg completion (when inside `COUNT(`, etc).
5. Optional follow-up: include struct paths in helper command completions (`/top`, `/group`, etc) if helper commands should be struct-aware too.

## 4. Long STRUCT Values Viewable in Results Pane

1. Add a cell preview region in `src/sqlexplore/tui.py` under results table (still in Results pane layout).
2. Update preview on `DataTable` cell highlight/selection with full, untruncated value.
3. For STRUCT preview, pretty-print JSON (`indent=2`) with wrapping; keep table cells compact/truncated for scan speed.
4. Add one keybinding for explicit full-cell view/copy (for example, `Enter` or `Ctrl+Y`) to reduce friction on very long structs.

## 5. Tests and Docs

1. Add app tests in `tests/sqlexplore/test_app.py`:
   - STRUCT highlight applied
   - preview shows full long struct
   - sort still works with styled struct cells
2. Add engine tests in `tests/sqlexplore/test_engine_commands.py`:
   - dotted-path completion
   - nested field completion
   - prefix filtering after dot
3. Update `README.md`:
   - STRUCT rendering behavior
   - struct autocomplete behavior
   - long-cell preview UX/keybinding

## Suggested Delivery Order

1. Metadata backbone
2. Rendering
3. Long-value preview
4. Autocomplete
5. Tests and docs

