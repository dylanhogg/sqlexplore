# Plan: Minimal DuckDB Context + 1x Auto-Fix Retry

## Goal
Improve `/llm query ...` SQL validity for DuckDB, esp regex/json/struct/date quirks, with minimal code and prompt growth.

## Scope
- Context engineering: inject small DuckDB-specific guidance into prompt.
- Retry loop: max 1 retry when SQL validation or DuckDB execution fails.
- User visibility: clear info message when retry happens.

## Non-goals
- Full docs ingestion/indexing/RAG.
- Multi-step agent loop.
- More than 1 retry.

## Root cause (current)
- Prompt has schema + sample rows, but weak dialect/function guardrails.
- Validation is parse/table-shape only; runtime dialect/function errors not fed back.
- No repair path after first failed SQL.

## Implementation Plan

### 1. Add tiny DuckDB guidance pack (curated, static)
- New file: `src/sqlexplore/llm/duckdb_guidance.py`
- Add compact constants, not full docs. Keep ~1-2KB text total:
  - Core dialect rules.
  - Regex function map (`regexp_matches`, `regexp_extract`, `regexp_replace`, pattern matching notes).
  - JSON vs STRUCT rules:
    - JSON columns -> JSON funcs.
    - STRUCT columns -> `struct_extract(col, 'field')` or `col.field`.
  - Date/time function reminders.
  - Friendly SQL + quirks reminders.
- Keep source links in this file (from `DOCS_DUCKDB.md`) for maintainability.

### 2. Add topic-aware context selection
- File: `src/sqlexplore/llm/llm_sql.py`
- Add helper to choose guidance blocks by:
  - NL query keywords (`regex`, `json`, `struct`, `date`, `time`, `timestamp`, etc.).
  - Schema type hints (`JSON`, `STRUCT`, `MAP`, `LIST`, temporal types).
- Inject selected guidance into `build_prompt(...)`.
- Add hard prompt budget (char cap) to keep minimal.

### 3. Add SQL repair prompt builder
- File: `src/sqlexplore/llm/llm_sql.py`
- New helper `build_repair_prompt(...)` includes:
  - Original NL request.
  - Previous SQL.
  - Validation/runtime error text.
  - Same schema/sample + selected DuckDB guidance.
  - Strict constraints: one SELECT statement, DuckDB only, table/column limits.

### 4. Add 1x retry orchestration in `/llm`
- File: `src/sqlexplore/commands/handlers.py`
- In `cmd_llm(...)`:
  - Keep first generation path.
  - If `validate_generated_sql(...)` fails: run repair once.
  - If execution returns `status="error"` with retryable DuckDB errors (Parser/Catalog/Binder): run repair once.
  - Max retries constant: `MAX_LLM_SQL_RETRIES = 1`.
  - Never retry provider/auth failures.

### 5. User-visible info logging for retry
- File: `src/sqlexplore/commands/handlers.py`
- On retry attempt:
  - `logger.info(...)` with reason.
  - Append user-facing note in success message, e.g. `Info: LLM auto-retry fixed SQL after DuckDB error (1 retry).`
- On retry failure:
  - Return existing error response with concise reason + final generated SQL.

### 6. Tests
- `tests/sqlexplore/test_llm_sql.py`
  - Guidance selection tests (regex/json/struct/temporal).
  - Prompt includes selected guidance and stays bounded.
  - Repair prompt includes prior SQL + error.
- `tests/sqlexplore/test_engine_commands.py`
  - Retry on validation failure.
  - Retry on runtime Catalog/Binder/Parser error.
  - No retry on provider error.
  - Max 1 retry enforced.
  - Success-after-retry includes user-visible info message.

## Suggested rollout order
1. Guidance pack + selection + prompt wiring.
2. Repair prompt helper.
3. `cmd_llm` 1x retry flow.
4. Tests.
5. Run `uv run pytest tests/sqlexplore/test_llm_sql.py tests/sqlexplore/test_engine_commands.py`.

## Minimal acceptance criteria
- Regex/json/struct prompts include DuckDB-specific function hints.
- Invalid first SQL can auto-repair once.
- Retry path clearly visible to user in response message.
- No multi-retry loops.
