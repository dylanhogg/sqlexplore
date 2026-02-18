from dataclasses import dataclass

import pytest

import sqlexplore.commands.llm_runner as llm_runner_module
from sqlexplore.commands.llm_runner import (
    GenerateSqlFn,
    LlmRunnerConfig,
    LlmRunnerDeps,
    ValidateGeneratedSqlFn,
    is_retryable_duckdb_sql_error,
    run_llm_query_with_retry,
)
from sqlexplore.commands.protocols import CommandEngine
from sqlexplore.core.engine_models import EngineResponse, HistoryQueryType, QueryHistoryEntry
from sqlexplore.llm.llm_sql import LlmTableContext, SampleRows, TableSampleRows


def test_run_llm_query_with_retry_default_deps_pass_allowed_tables(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, tuple[str, ...] | None] = {}

    def fake_build_prompt(
        user_query: str,
        table_name: str,
        schema_context: str,
        sample_rows: SampleRows,
        allowed_table_names: tuple[str, ...] | None = None,
        table_context: LlmTableContext | None = None,
    ) -> str:
        _ = user_query
        _ = table_name
        _ = schema_context
        _ = sample_rows
        captured["prompt"] = allowed_table_names
        captured["prompt_ctx_tables"] = table_context.allowed_table_names if table_context is not None else None
        return "prompt"

    def fake_build_repair_prompt(
        user_query: str,
        previous_sql: str,
        error_message: str,
        table_name: str,
        schema_context: str,
        sample_rows: SampleRows,
        allowed_table_names: tuple[str, ...] | None = None,
        table_context: LlmTableContext | None = None,
    ) -> str:
        _ = user_query
        _ = previous_sql
        _ = error_message
        _ = table_name
        _ = schema_context
        _ = sample_rows
        captured["repair"] = allowed_table_names
        captured["repair_ctx_tables"] = table_context.allowed_table_names if table_context is not None else None
        return "repair"

    def fake_validate(sql: str, table_name: str, allowed_table_names: tuple[str, ...] | None = None) -> str | None:
        _ = sql
        _ = table_name
        captured["validate"] = allowed_table_names
        return None

    def fake_build_table_context(
        engine: CommandEngine,
        allowed_table_names: tuple[str, ...] | None = None,
        sample_rows_per_table: int = 3,
        max_tables_with_samples: int = 3,
    ) -> LlmTableContext:
        _ = engine
        _ = sample_rows_per_table
        _ = max_tables_with_samples
        return LlmTableContext(
            active_table_name="users",
            allowed_table_names=tuple(allowed_table_names or ("users",)),
            schema_context='Schema:\n- Table "users":\n  - id: BIGINT (nullable=YES)',
            active_sample_rows=SampleRows(columns=("id",), rows=((1,),)),
            sample_rows_by_table=(
                TableSampleRows(table_name="users", sample_rows=SampleRows(columns=("id",), rows=((1,),))),
            ),
        )

    def fake_generate_sql(prompt: str, model: str) -> str:
        _ = prompt
        _ = model
        return 'SELECT * FROM "users"'

    monkeypatch.setattr(llm_runner_module, "build_llm_table_context", fake_build_table_context)
    monkeypatch.setattr(llm_runner_module, "build_prompt", fake_build_prompt)
    monkeypatch.setattr(llm_runner_module, "build_repair_prompt", fake_build_repair_prompt)
    monkeypatch.setattr(llm_runner_module, "validate_generated_sql", fake_validate)
    monkeypatch.setattr(llm_runner_module, "generate_sql", fake_generate_sql)

    engine = _FakeEngine(responses=[EngineResponse(status="ok", message="done")])
    engine.table_name = "users"
    engine.table_names = ("users", "events")
    out = run_llm_query_with_retry(engine, "q", "m")
    assert out.status == "response"
    assert captured == {
        "prompt": ("users", "events"),
        "prompt_ctx_tables": ("users", "events"),
        "validate": ("users", "events"),
    }


class _FakeEngine:
    conn = None
    table_name = "data"
    table_names: tuple[str, ...] = ("data",)
    default_limit = 10
    max_rows_display = 50
    max_value_chars = 80
    columns = ["x"]
    column_types = {"x": "VARCHAR"}
    executed_sql: list[str]
    query_history: list[QueryHistoryEntry]
    last_sql = ""
    last_result_sql: str | None = None

    def __init__(self, responses: list[EngineResponse]) -> None:
        self._responses = list(responses)
        self.run_sql_calls: list[tuple[str, HistoryQueryType]] = []
        self.executed_sql = []
        self.query_history = []

    @property
    def schema_rows(self) -> list[tuple[str, str, str]]:
        return [("x", "VARCHAR", "YES")]

    def help_text(self) -> str:
        return ""

    def command_specs(self) -> list[object]:
        return []

    def lookup_command(self, raw_name: str) -> object | None:
        _ = raw_name
        return None

    def resolve_column(self, raw_column: str) -> str | None:
        return raw_column if raw_column in self.columns else None

    def resolve_table_name(self, raw_table_name: str) -> str | None:
        normalized = raw_table_name.strip().casefold()
        for table_name in self.table_names:
            if table_name.casefold() == normalized:
                return table_name
        return None

    def switch_table(self, raw_table_name: str) -> str | None:
        resolved = self.resolve_table_name(raw_table_name)
        if resolved is None:
            return None
        self.table_name = resolved
        return resolved

    def run_sql(
        self,
        sql_text: str,
        remember: bool = True,
        add_to_query_history: bool = True,
        query_type: HistoryQueryType = "user_entered_sql",
    ) -> EngineResponse:
        _ = remember
        _ = add_to_query_history
        self.run_sql_calls.append((sql_text, query_type))
        if self._responses:
            return self._responses.pop(0)
        return EngineResponse(status="ok", message="ok")

    def run_input(self, raw_input: str, add_to_query_history: bool = True) -> EngineResponse:
        _ = raw_input
        _ = add_to_query_history
        return EngineResponse(status="info", message="")

    def row_count(self) -> int:
        return 0

    def table_response(self, columns: list[str], rows: list[tuple[object, ...]], message: str) -> EngineResponse:
        _ = columns
        _ = rows
        return EngineResponse(status="ok", message=message)


@dataclass(slots=True)
class _Capture:
    prompts: list[str]
    repair_inputs: list[tuple[str, str]]


def _build_deps(
    generate_sql_fn: GenerateSqlFn,
    validate_sql_fn: ValidateGeneratedSqlFn,
    capture: _Capture,
) -> LlmRunnerDeps:
    def build_schema(engine: CommandEngine) -> str:
        _ = engine
        return "Schema:\n- x: VARCHAR"

    def fetch_rows(engine: CommandEngine) -> SampleRows:
        _ = engine
        return SampleRows(columns=("x",), rows=(("a",),))

    def build_prompt_fn(user_query: str, table_name: str, schema_context: str, sample_rows: SampleRows) -> str:
        _ = user_query
        _ = table_name
        _ = schema_context
        _ = sample_rows
        return "initial-prompt"

    def build_repair_prompt_fn(
        user_query: str,
        previous_sql: str,
        error_message: str,
        table_name: str,
        schema_context: str,
        sample_rows: SampleRows,
    ) -> str:
        _ = user_query
        _ = table_name
        _ = schema_context
        _ = sample_rows
        capture.repair_inputs.append((previous_sql, error_message))
        return f"repair-prompt::{error_message}"

    def generate_fn(prompt: str, model: str) -> str:
        capture.prompts.append(prompt)
        return generate_sql_fn(prompt, model)

    return LlmRunnerDeps(
        build_schema_context=build_schema,
        fetch_sample_rows=fetch_rows,
        build_prompt=build_prompt_fn,
        build_repair_prompt=build_repair_prompt_fn,
        generate_sql=generate_fn,
        validate_generated_sql=validate_sql_fn,
    )


def test_run_llm_query_with_retry_returns_provider_error() -> None:
    engine = _FakeEngine(responses=[])
    capture = _Capture(prompts=[], repair_inputs=[])

    def raise_provider(prompt: str, model: str) -> str:
        _ = prompt
        _ = model
        raise RuntimeError("provider down")

    def always_valid(sql: str, table_name: str) -> str | None:
        _ = sql
        _ = table_name
        return None

    deps = _build_deps(raise_provider, always_valid, capture)
    out = run_llm_query_with_retry(engine, "q", "m", deps=deps)
    assert out.status == "provider_error"
    assert out.retry_count == 0
    assert engine.run_sql_calls == []
    assert capture.prompts == ["initial-prompt"]


def test_run_llm_query_with_retry_retries_validation_once_then_succeeds() -> None:
    engine = _FakeEngine(responses=[EngineResponse(status="ok", message="done")])
    capture = _Capture(prompts=[], repair_inputs=[])
    sql_sequence = iter(['DELETE FROM "data"', 'SELECT * FROM "data" LIMIT 1'])

    def next_sql(prompt: str, model: str) -> str:
        _ = prompt
        _ = model
        return next(sql_sequence)

    def validate(sql: str, table_name: str) -> str | None:
        _ = table_name
        if sql.startswith("DELETE"):
            return "Generated SQL must be SELECT or WITH ... SELECT."
        return None

    deps = _build_deps(next_sql, validate, capture)
    out = run_llm_query_with_retry(engine, "q", "m", deps=deps)
    assert out.status == "response"
    assert out.retry_count == 1
    assert out.response is not None
    assert out.response.status == "ok"
    assert out.response.generated_sql == 'SELECT * FROM "data" LIMIT 1'
    assert capture.prompts == ["initial-prompt", "repair-prompt::Generated SQL must be SELECT or WITH ... SELECT."]
    assert capture.repair_inputs == [('DELETE FROM "data"', "Generated SQL must be SELECT or WITH ... SELECT.")]
    assert engine.run_sql_calls == [('SELECT * FROM "data" LIMIT 1', "llm_generated_sql")]


def test_run_llm_query_with_retry_caps_validation_retries() -> None:
    engine = _FakeEngine(responses=[])
    capture = _Capture(prompts=[], repair_inputs=[])
    sql_sequence = iter(["bad-1", "bad-2"])

    def next_sql(prompt: str, model: str) -> str:
        _ = prompt
        _ = model
        return next(sql_sequence)

    def always_invalid(sql: str, table_name: str) -> str:
        _ = sql
        _ = table_name
        return "still invalid"

    deps = _build_deps(next_sql, always_invalid, capture)
    out = run_llm_query_with_retry(engine, "q", "m", config=LlmRunnerConfig(max_retries=1), deps=deps)
    assert out.status == "invalid_sql"
    assert out.retry_count == 1
    assert out.invalid_sql_detail == "still invalid"
    assert out.generated_sql == "bad-2"
    assert engine.run_sql_calls == []


def test_run_llm_query_with_retry_retries_runtime_duckdb_error_once() -> None:
    engine = _FakeEngine(
        responses=[
            EngineResponse(status="error", message="Catalog Error: no such function"),
            EngineResponse(status="ok", message="done"),
        ]
    )
    capture = _Capture(prompts=[], repair_inputs=[])
    sql_sequence = iter(["SELECT bad()", 'SELECT * FROM "data"'])

    def next_sql(prompt: str, model: str) -> str:
        _ = prompt
        _ = model
        return next(sql_sequence)

    def always_valid(sql: str, table_name: str) -> str | None:
        _ = sql
        _ = table_name
        return None

    deps = _build_deps(next_sql, always_valid, capture)
    out = run_llm_query_with_retry(engine, "q", "m", deps=deps)
    assert out.status == "response"
    assert out.retry_count == 1
    assert out.response is not None
    assert out.response.status == "ok"
    assert capture.prompts == ["initial-prompt", "repair-prompt::Catalog Error: no such function"]
    assert capture.repair_inputs == [("SELECT bad()", "Catalog Error: no such function")]
    assert engine.run_sql_calls == [
        ("SELECT bad()", "llm_generated_sql"),
        ('SELECT * FROM "data"', "llm_generated_sql"),
    ]


def test_run_llm_query_with_retry_does_not_retry_non_duckdb_runtime_error() -> None:
    engine = _FakeEngine(responses=[EngineResponse(status="error", message="IO Error: file missing")])
    capture = _Capture(prompts=[], repair_inputs=[])

    def valid_sql(prompt: str, model: str) -> str:
        _ = prompt
        _ = model
        return 'SELECT * FROM "data"'

    def always_valid(sql: str, table_name: str) -> str | None:
        _ = sql
        _ = table_name
        return None

    deps = _build_deps(valid_sql, always_valid, capture)
    out = run_llm_query_with_retry(engine, "q", "m", deps=deps)
    assert out.status == "response"
    assert out.retry_count == 0
    assert out.response is not None
    assert out.response.status == "error"
    assert capture.prompts == ["initial-prompt"]
    assert capture.repair_inputs == []
    assert engine.run_sql_calls == [('SELECT * FROM "data"', "llm_generated_sql")]


def test_run_llm_query_with_retry_rejects_negative_retries() -> None:
    engine = _FakeEngine(responses=[])
    capture = _Capture(prompts=[], repair_inputs=[])

    def valid_sql(prompt: str, model: str) -> str:
        _ = prompt
        _ = model
        return 'SELECT * FROM "data"'

    def always_valid(sql: str, table_name: str) -> str | None:
        _ = sql
        _ = table_name
        return None

    deps = _build_deps(valid_sql, always_valid, capture)
    with pytest.raises(ValueError, match="non-negative retries"):
        run_llm_query_with_retry(engine, "q", "m", config=LlmRunnerConfig(max_retries=-1), deps=deps)


def test_is_retryable_duckdb_sql_error_supports_exception_names() -> None:
    markers = ("catalog error:", "catalogexception", "parserexception")
    assert is_retryable_duckdb_sql_error("CatalogException: missing function", markers) is True
    assert is_retryable_duckdb_sql_error("ParserException: bad sql", markers) is True
    assert is_retryable_duckdb_sql_error("IO Error: bad file", markers) is False
