from pathlib import Path
from typing import Any

from sqlexplore.core.engine import DataSourceBinding, SqlExplorerEngine
from sqlexplore.llm.llm_sql import (
    DEFAULT_LLM_MODEL,
    DEFAULT_SAMPLE_ROWS,
    LITELLM_API_KEY_ENV_VAR,
    LLM_API_KEY_ENV_VARS,
    SQLEXPLORE_LLM_MODEL_ENV_VAR,
    LlmTableContext,
    SampleRows,
    TableSampleRows,
    build_llm_table_context,
    build_prompt,
    build_repair_prompt,
    build_schema_context,
    fetch_sample_rows,
    generate_sql,
    resolve_llm_api_key_env_var,
    resolve_llm_model,
    select_duckdb_guidance,
    validate_generated_sql,
    validate_llm_api_key,
)


def _build_engine(
    tmp_path: Path,
    csv_text: str = "city,count\nseattle,10\nportland,8\nseattle,7\nboise,2\n",
) -> SqlExplorerEngine:
    csv_path = tmp_path / "data.csv"
    csv_path.write_text(csv_text, encoding="utf-8")
    return SqlExplorerEngine(
        data_path=csv_path,
        table_name="data",
        database=":memory:",
        default_limit=10,
        max_rows_display=100,
        max_value_chars=80,
    )


def _build_tables_engine(tmp_path: Path) -> SqlExplorerEngine:
    users_path = tmp_path / "users.csv"
    events_path = tmp_path / "events.csv"
    users_path.write_text("id,name\n1,Alice\n2,Bob\n", encoding="utf-8")
    events_path.write_text("user_id,event\n1,login\n2,logout\n", encoding="utf-8")
    return SqlExplorerEngine(
        data_path=users_path,
        table_name="data",
        database=":memory:",
        default_limit=10,
        max_rows_display=100,
        max_value_chars=80,
        data_sources=(
            DataSourceBinding(path=users_path, table_name="users"),
            DataSourceBinding(path=events_path, table_name="events"),
        ),
        load_mode="tables",
        active_table="users",
    )


def test_resolve_llm_model_uses_default_when_missing_or_blank() -> None:
    assert resolve_llm_model({}) == DEFAULT_LLM_MODEL
    assert resolve_llm_model({SQLEXPLORE_LLM_MODEL_ENV_VAR: "   "}) == DEFAULT_LLM_MODEL


def test_resolve_llm_model_uses_env_value() -> None:
    env = {SQLEXPLORE_LLM_MODEL_ENV_VAR: "anthropic/claude-3-5-sonnet"}
    assert resolve_llm_model(env) == "anthropic/claude-3-5-sonnet"


def test_resolve_llm_api_key_env_var_prefers_litellm_key() -> None:
    env = {
        "OPENAI_API_KEY": "openai-key",
        LITELLM_API_KEY_ENV_VAR: "litellm-key",
    }
    assert resolve_llm_api_key_env_var(env) == LITELLM_API_KEY_ENV_VAR


def test_validate_llm_api_key_accepts_common_provider_key() -> None:
    assert validate_llm_api_key({"OPENAI_API_KEY": "key"}) is None


def test_validate_llm_api_key_returns_explicit_message_when_missing() -> None:
    message = validate_llm_api_key({})
    assert message is not None
    assert "LLM API key not found in environment." in message
    assert LITELLM_API_KEY_ENV_VAR in message
    for env_var in LLM_API_KEY_ENV_VARS:
        assert env_var in message


def test_build_schema_context_includes_schema_rows(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        context = build_schema_context(engine)
    finally:
        engine.close()
    assert "Schema:" in context
    assert "city:" in context
    assert "count:" in context
    assert "nullable=" in context


def test_fetch_sample_rows_returns_first_three_rows_by_default(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        samples = fetch_sample_rows(engine)
    finally:
        engine.close()
    assert samples.columns == ("city", "count")
    assert len(samples.rows) == DEFAULT_SAMPLE_ROWS
    assert samples.rows[0] == ("seattle", 10)
    assert samples.rows[-1] == ("seattle", 7)
    assert ("boise", 2) not in samples.rows


def test_build_prompt_includes_constraints_schema_and_sample_rows() -> None:
    prompt = build_prompt(
        user_query="top 2 cities by count",
        table_name="data",
        schema_context="Schema:\n- city: VARCHAR (nullable=YES)\n- count: BIGINT (nullable=YES)",
        sample_rows=SampleRows(
            columns=("city", "count"),
            rows=(("seattle", 10), ("portland", 8), ("seattle", 7)),
        ),
    )
    assert "Use DuckDB dialect." in prompt
    assert 'Use only table "data".' in prompt
    assert "Return SQL only." in prompt
    assert "Return exactly one statement." in prompt
    assert "Statement must be SELECT or WITH ... SELECT." in prompt
    assert "User request:\ntop 2 cities by count" in prompt
    assert "DuckDB guidance:" in prompt
    assert "Schema:\n- city: VARCHAR" in prompt
    assert "First rows:" in prompt
    assert "seattle, 10" in prompt


def test_build_prompt_allows_multiple_loaded_tables() -> None:
    prompt = build_prompt(
        user_query="join users to events and count events per user",
        table_name="users",
        schema_context="Schema:\n- id: BIGINT (nullable=YES)\n- name: VARCHAR (nullable=YES)",
        sample_rows=SampleRows(
            columns=("id", "name"),
            rows=((1, "Alice"), (2, "Bob")),
        ),
        allowed_table_names=("users", "events"),
    )
    assert 'Use only loaded tables: "users", "events".' in prompt
    assert 'Use "users" when one table is sufficient.' in prompt


def test_build_prompt_uses_table_context_for_schema_and_samples() -> None:
    schema_context = (
        'Schema:\n- Table "users":\n  - id: BIGINT (nullable=YES)\n- Table "events":\n  - event: VARCHAR (nullable=YES)'
    )
    table_context = LlmTableContext(
        active_table_name="users",
        allowed_table_names=("users", "events"),
        schema_context=schema_context,
        active_sample_rows=SampleRows(columns=("id", "name"), rows=((1, "Alice"),)),
        sample_rows_by_table=(
            TableSampleRows(table_name="users", sample_rows=SampleRows(columns=("id",), rows=((1,),))),
            TableSampleRows(table_name="events", sample_rows=SampleRows(columns=("event",), rows=(("login",),))),
        ),
    )
    prompt = build_prompt(
        user_query="join users and events",
        table_name="users",
        schema_context="Schema:\n- id: BIGINT (nullable=YES)",
        sample_rows=SampleRows(columns=("id",), rows=((1,),)),
        table_context=table_context,
    )
    assert 'Use only loaded tables: "users", "events".' in prompt
    assert 'Table "events"' in prompt
    assert "Sample rows by table:" in prompt


def test_build_llm_table_context_includes_schema_for_all_allowed_tables(tmp_path: Path) -> None:
    engine = _build_tables_engine(tmp_path)
    try:
        table_context = build_llm_table_context(
            engine,
            allowed_table_names=("users", "events"),
            sample_rows_per_table=2,
            max_tables_with_samples=2,
        )
    finally:
        engine.close()
    assert table_context.allowed_table_names == ("users", "events")
    assert table_context.active_table_name == "users"
    assert '- Table "users":' in table_context.schema_context
    assert '- Table "events":' in table_context.schema_context
    assert table_context.sample_rows_by_table[0].table_name == "users"
    assert table_context.sample_rows_by_table[1].table_name == "events"


def test_select_duckdb_guidance_adds_regex_json_struct_and_temporal_sections() -> None:
    guidance = select_duckdb_guidance(
        user_query="extract regex from json field and parse timestamp",
        schema_context="Schema:\n- payload: JSON (nullable=YES)\n- meta: STRUCT(x VARCHAR) (nullable=YES)",
    )
    assert "DuckDB basics:" in guidance
    assert "Regex:" in guidance
    assert "JSON:" in guidance
    assert "STRUCT:" in guidance
    assert "Date/time:" in guidance


def test_select_duckdb_guidance_respects_max_chars() -> None:
    guidance = select_duckdb_guidance(
        user_query="regex json struct time",
        schema_context="Schema:\n- payload: JSON (nullable=YES)\n- ts: TIMESTAMP (nullable=YES)",
        max_chars=120,
    )
    assert len(guidance) <= 120


def test_build_repair_prompt_includes_previous_sql_and_error() -> None:
    prompt = build_repair_prompt(
        user_query="extract number from json payload",
        previous_sql='SELECT json_extract(x, "$.n") FROM "data"',
        error_message="Catalog Error: Scalar Function with name json_extract does not exist!",
        table_name="data",
        schema_context="Schema:\n- x: VARCHAR (nullable=YES)",
        sample_rows=SampleRows(columns=("x",), rows=(('{"n": 1}',),)),
    )
    assert "Fix this SQL for DuckDB." in prompt
    assert 'Previous SQL:\nSELECT json_extract(x, "$.n") FROM "data"' in prompt
    assert "Error:\nCatalog Error: Scalar Function with name json_extract does not exist!" in prompt
    assert "Return corrected SQL only." in prompt


def test_generate_sql_uses_litellm_completion(monkeypatch: Any) -> None:
    captured: dict[str, Any] = {}

    class _Message:
        def __init__(self, content: str) -> None:
            self.content = content

    class _Choice:
        def __init__(self, content: str) -> None:
            self.message = _Message(content)

    class _Response:
        def __init__(self, content: str) -> None:
            self.choices = [_Choice(content)]

    def fake_completion(**kwargs: Any) -> _Response:
        captured.update(kwargs)
        return _Response('```sql\nSELECT * FROM "data" LIMIT 3\n```')

    monkeypatch.setattr("sqlexplore.llm.llm_sql.litellm.completion", fake_completion)
    sql = generate_sql(prompt="prompt", model="openai/gpt-5-mini")
    assert sql == 'SELECT * FROM "data" LIMIT 3'
    assert captured["model"] == "openai/gpt-5-mini"
    assert captured["messages"][1]["content"] == "prompt"
    assert captured["temperature"] == 0


def test_validate_generated_sql_rejects_empty_multi_statement_and_non_select() -> None:
    assert validate_generated_sql(" ", "data") == "Generated SQL is empty."
    assert validate_generated_sql("SELECT 1; SELECT 2", "data") == "Generated SQL must be a single statement."
    assert validate_generated_sql('DELETE FROM "data"', "data") == "Generated SQL must be SELECT or WITH ... SELECT."


def test_validate_generated_sql_rejects_unknown_table_and_accepts_known_refs() -> None:
    err = validate_generated_sql("SELECT * FROM other_table", "data")
    assert err is not None
    assert "unknown table(s)" in err
    assert "other_table" in err

    assert validate_generated_sql('SELECT * FROM "data" LIMIT 5', "data") is None
    assert validate_generated_sql('WITH x AS (SELECT * FROM "data") SELECT * FROM x WHERE count > 5', "data") is None


def test_validate_generated_sql_allows_loaded_tables_set_in_tables_mode() -> None:
    assert (
        validate_generated_sql(
            'SELECT u.id, e.event FROM "users" u JOIN "events" e ON u.id = e.user_id',
            "users",
            allowed_table_names=("users", "events"),
        )
        is None
    )
    err = validate_generated_sql(
        'SELECT * FROM "payments"',
        "users",
        allowed_table_names=("users", "events"),
    )
    assert err is not None
    assert "payments" in err
    assert 'Allowed tables: "users", "events".' in err
