# LiteLLM NL->SQL Integration Plan

## Goal
Add helper command: `/llm query <natural language query>` that:
1. Builds LLM context from user query + schema + first 3 rows.
2. Generates DuckDB SQL.
3. Executes via existing SQL path.
4. Returns clear user error when LLM key is missing.

Keep design small, reuse existing command/engine flow.

## Stage 1: Minimal config + dependency
- Add `litellm` dependency in `pyproject.toml`.
- Define env contract:
  - `SQLEXPLORE_LLM_MODEL` (optional, default e.g. `openai/gpt-5-mini`).
  - Appropriate LLM API Key must exist in env when `/llm` is called.
- Add one small config helper in new module (e.g. `src/sqlexplore/llm/llm_sql.py`):
  - Resolve model.
  - Validate key presence (check `LITELLM_API_KEY` + common provider keys).
  - Return explicit error message if missing.

## Stage 2: NL->SQL service (new module, pure functions)
- Implement in `src/sqlexplore/llm/llm_sql.py`:
  - `build_schema_context(engine)`: reuse `engine.schema_rows`.
  - `fetch_sample_rows(engine, n=3)`: `SELECT * FROM "<table>" LIMIT 3`.
  - `build_prompt(...)`: include strict constraints:
    - DuckDB dialect.
    - Use only known table/columns.
    - Return SQL only (single statement).
  - `generate_sql(...)`: call LiteLLM `completion(...)`.
  - `validate_generated_sql(...)`: parse with existing `sqlglot`; reject non-`SELECT`/`WITH`, multi-statement, unknown table refs.
- Keep module independent from TUI; only depends on `CommandEngine` shape and small helpers.

## Stage 3: Command wiring (max reuse existing flow)
- In `src/sqlexplore/commands/handlers.py`:
  - Add `USAGE_LLM = "/llm query <natural language query>"`.
  - Add `cmd_llm(engine, args)`.
  - Parse subcommand (`query`) + NL payload.
  - On success: call `run_generated_sql(engine, sql)` (reuse existing execution + activity log behavior).
  - On failure: return `response(status="error", message=...)` with clear reason (missing key, provider error, invalid SQL).
- In `src/sqlexplore/commands/registry.py`:
  - Register `/llm` command in `build_command_specs(...)`.
  - Description: “Generate DuckDB SQL from natural language.”
- No new run path in engine/app. Reuse `run_input -> run_command`.

## Stage 4: Safety + UX polish
- Error mapping in one place:
  - Missing key -> “LLM API key not found in environment.”
  - Provider auth/network failures -> short actionable message.
  - Invalid SQL from model -> include short reason + ask user to rephrase.
- Keep generated SQL visible through existing `generated_sql` rendering.
- Ensure `/help` auto-lists new usage via registry (already automatic).

## Stage 5: Tests (focused, no network)
- Add command tests in `tests/sqlexplore/test_engine_commands.py`:
  - `/llm` usage errors (`/llm`, `/llm query` without text).
  - Missing key returns user-facing error.
  - Happy path: monkeypatch LiteLLM call, assert generated SQL executed and result returned.
  - Model returns invalid SQL -> error path.
- Add unit tests for new module:
  - Context includes schema + exactly first 3 rows.
  - SQL validator rejects multi-statement/non-SELECT.
- Keep tests offline by mocking LiteLLM call.

## Stage 6: Delivery checklist
- Run:
  - `uv run ruff format .`
  - `uv run ruff check . --fix`
  - `uv run pyright`
  - `uv run pytest`
- Verify manually:
  - `/help` shows `/llm query ...`
  - `/llm query top 5 cities by count` prints generated SQL + results.
  - Unset key then call `/llm ...` -> clear error.

## Notes on simplicity/reuse
- Reuses existing command registry, command dispatch, SQL execution, result rendering, and activity logging.
- New logic isolated to one small `llm/llm_sql.py` module + one handler entrypoint.
- Avoids touching TUI/editor internals unless later needed for richer completions.
