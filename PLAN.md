# SQL + Helper Command Autocomplete Plan

## Goal
Make writing SQL and using helper commands feel natural, intuitive, and fast by introducing a unified, context-aware autocomplete system in the Query pane with a dropdown of valid next options as the user types.

## Non-Goals (v1)
- No LLM-driven autocomplete.
- No heavy multi-table query planner logic.
- No bloated feature surface that reduces clarity.

## Guiding Principles
- One interaction model for SQL and helper commands.
- Suggestions must be context-aware, not just prefix-based.
- Low latency and predictable keyboard behavior.
- Keep architecture simple and testable.

## Current State Summary
- Completion today is token-prefix only with a single inline suggestion.
- No dropdown menu of alternatives.
- Helper command parsing and docs are spread across conditional branches.
- SQL suggestions are not clause-aware.

## Proposed Architecture

### 1) Unified Completion Domain Model
Create shared completion structures used by both SQL and helper command paths.

- `CompletionContext`: current text, cursor position, active mode, clause hints.
- `CompletionItem`: label, insert text, kind, detail, score, replacement span.
- `CompletionEngine`: `get_items(text, cursor, schema_state) -> list[CompletionItem]`.

Outcome:
- One backend powers inline ghost suggestion and dropdown list.
- UI and logic are decoupled for easier testing.

### 2) Helper Command Registry (Single Source of Truth)
Replace ad hoc command branching with typed command specs.

- `CommandSpec`: name, usage, description, argument schema, executor callback.
- Generate `/help` text from registry.
- Use argument schema for valid-next suggestions while typing command args.

Examples:
- After `/` -> show all commands.
- After `/top ` -> suggest columns.
- After `/group <cols> | ` -> suggest aggregate snippets.

Outcome:
- Helper command UX becomes complete and consistent.
- Command behavior, docs, and autocomplete stay in sync.

### 3) SQL Context Analysis
Use `sqlglot` as the primary parser for SQL context detection.

- Detect active clause (`SELECT`, `FROM`, `WHERE`, `GROUP BY`, `ORDER BY`, etc.).
- Suggest columns/functions/tables based on clause semantics.
- Handle incomplete SQL with graceful fallback heuristics (never return empty due to partial parse).

Dependency choice:
- Add `sqlglot`.
- Do not add `sqlparse` in v1 unless needed later for formatting/splitting.

Outcome:
- Suggestions are obvious and contextually valid.

### 4) Query Pane Dropdown UX (Textual)
Add a completion dropdown anchored to editor cursor context.

- Shows top N ranked items (kind + label + short detail).
- Keyboard behavior:
  - `Up/Down`: navigate suggestions
  - `Tab` or `Enter`: accept selected item
  - `Esc`: close dropdown
  - `Ctrl+Space`: force-open suggestions
- Keep inline ghost suggestion for top candidate when useful.

Outcome:
- Fast, modern query authoring feel in terminal UI.

### 5) Ranking + Relevance
Implement deterministic scoring:

- Context validity (highest weight)
- Prefix/fuzzy match quality
- Item kind priority (columns/functions/keywords/commands/snippets)
- Recency boost from prior accepted items (optional lightweight enhancement)

Constraints:
- Cap suggestion count (e.g., 10).
- Debounce recompute (e.g., 50-80ms) to keep UI responsive.

Outcome:
- Useful options appear first; experience remains fast.

## UX Spec (v1)

### Unified Behavior
- Typing SQL or `/commands` uses same dropdown mechanics and keybindings.
- Valid next options are always visible when context is recognized.
- Unknown or invalid states degrade gracefully to helpful generic suggestions.

### Visual Consistency
- Suggestion rows include:
  - primary text (`label`)
  - small type badge (`keyword`, `column`, `function`, `command`, `snippet`)
  - optional detail (`usage`/brief signature)
- Styling aligns with existing Query pane and app theme.

### Simplicity Guardrails
- Keep defaults sensible.
- Avoid adding configuration complexity until usage justifies it.

## Implementation Phases

### Phase 1: Core Completion Infrastructure
Deliverables:
- Completion datatypes + engine interface.
- Helper command registry and migration from branch-heavy parsing.
- Unit tests for command parsing and next-option derivation.

Acceptance:
- Existing helper command behavior preserved.
- `/help` generated from command registry.

### Phase 2: SQL Context Intelligence
Deliverables:
- `sqlglot` integration for context extraction.
- Clause-aware suggestion providers.
- Fallback heuristics when SQL is incomplete.
- Unit tests for clause detection and suggested item categories.

Acceptance:
- Obvious clause-aware suggestions appear reliably.
- No regressions in query execution paths.

### Phase 3: Dropdown UI + Interaction
Deliverables:
- Textual dropdown component integrated with query editor.
- Unified key handling and insertion behavior.
- Integration tests with Textual pilot for keyboard interactions.

Acceptance:
- Dropdown appears/disappears correctly while typing.
- `Up/Down`, `Tab/Enter`, `Esc`, `Ctrl+Space` work consistently.

### Phase 4: Polish + Performance
Deliverables:
- Ranking tuning pass.
- Debounce/perf optimizations.
- UX polish for edge cases (quoted identifiers, spacing, casing).
- Final docs update for keybindings and autocomplete behavior.

Acceptance:
- Perceived latency remains low.
- Feature feels unified, complete, and consistent.
- Helper commands and SQL autocomplete should work correctly together.

## Test Strategy

### Unit Tests
- Completion context extraction.
- Helper command argument stage transitions.
- SQL clause-to-suggestion mapping.
- Quoted identifier and case-preserving insert behavior.

### Integration Tests
- Dropdown visibility lifecycle.
- Selection navigation and commit insertion.
- Conflict handling with existing shortcuts/history navigation.

### Regression Tests
- Preserve current command outcomes and SQL execution behavior.
- Preserve trailing-space rendering and existing editor shortcuts.

## Risks and Mitigations
- Parser errors on incomplete SQL: use fallback heuristic pipeline.
- UI complexity creep: strict scope for v1 + phased rollout.
- Performance regressions with many columns: cache schema-derived items and debounce updates.

## Definition of Done
- Query pane shows valid next options in a dropdown while typing SQL/helper commands.
- Autocomplete behavior is unified across SQL and helper commands.
- Core flows are keyboard-first, consistent, and fast.
- Tests cover major states and prevent regressions.
- Documentation explains behavior and keybindings clearly.

## Suggested First PR Sequence
1. Completion core + helper command registry + tests.
2. SQL context provider (`sqlglot`) + suggestion ranking + tests.
3. Textual dropdown integration + keyboard UX + integration tests.
4. Performance/polish + docs refresh.
