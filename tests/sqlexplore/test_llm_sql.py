from pathlib import Path
from typing import Any

from sqlexplore.core.engine import SqlExplorerEngine
from sqlexplore.llm.llm_sql import (
    DEFAULT_LLM_MODEL,
    DEFAULT_SAMPLE_ROWS,
    LITELLM_API_KEY_ENV_VAR,
    LLM_API_KEY_ENV_VARS,
    SQLEXPLORE_LLM_MODEL_ENV_VAR,
    SampleRows,
    build_prompt,
    build_schema_context,
    fetch_sample_rows,
    generate_sql,
    resolve_llm_api_key_env_var,
    resolve_llm_model,
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
    assert "Schema:\n- city: VARCHAR" in prompt
    assert "First rows:" in prompt
    assert "seattle, 10" in prompt


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
