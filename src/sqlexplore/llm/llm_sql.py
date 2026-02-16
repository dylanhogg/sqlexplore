import os
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol, cast

import litellm
import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError

from sqlexplore.completion.helpers import quote_ident

# drop_params handles dropping non applicable params (e.g. temperature parameter for GPT-5 models)
litellm.drop_params = True

DEFAULT_LLM_MODEL = "openai/gpt-5-mini"
SQLEXPLORE_LLM_MODEL_ENV_VAR = "SQLEXPLORE_LLM_MODEL"
LITELLM_API_KEY_ENV_VAR = "LITELLM_API_KEY"
COMMON_PROVIDER_API_KEY_ENV_VARS = (
    "OPENAI_API_KEY",
    "ANTHROPIC_API_KEY",
    "AZURE_OPENAI_API_KEY",
    "GOOGLE_API_KEY",
    "GEMINI_API_KEY",
    "MISTRAL_API_KEY",
    "COHERE_API_KEY",
    "GROQ_API_KEY",
    "TOGETHERAI_API_KEY",
    "OPENROUTER_API_KEY",
    "DEEPSEEK_API_KEY",
)
LLM_API_KEY_ENV_VARS = (LITELLM_API_KEY_ENV_VAR, *COMMON_PROVIDER_API_KEY_ENV_VARS)
DEFAULT_SAMPLE_ROWS = 3


class LlmSqlEngine(Protocol):
    conn: Any
    table_name: str

    @property
    def schema_rows(self) -> list[tuple[Any, ...]]: ...


@dataclass(frozen=True, slots=True)
class SampleRows:
    columns: tuple[str, ...]
    rows: tuple[tuple[Any, ...], ...]


CompletionFn = Callable[..., Any]


def _env_values(env: Mapping[str, str] | None = None) -> Mapping[str, str]:
    return os.environ if env is None else env


def resolve_llm_model(env: Mapping[str, str] | None = None) -> str:
    model = _env_values(env).get(SQLEXPLORE_LLM_MODEL_ENV_VAR, "").strip()
    return model or DEFAULT_LLM_MODEL


def resolve_llm_api_key_env_var(env: Mapping[str, str] | None = None) -> str | None:
    values = _env_values(env)
    for env_var in LLM_API_KEY_ENV_VARS:
        if values.get(env_var, "").strip():
            return env_var
    return None


def validate_llm_api_key(env: Mapping[str, str] | None = None) -> str | None:
    if resolve_llm_api_key_env_var(env) is not None:
        return None
    env_vars = ", ".join(LLM_API_KEY_ENV_VARS)
    return f"LLM API key not found in environment. Set one of: {env_vars}."


def build_schema_context(engine: LlmSqlEngine) -> str:
    lines = ["Schema:"]
    for row in engine.schema_rows:
        assert len(row) >= 2
        column_name = str(row[0])
        column_type = str(row[1])
        nullable = str(row[2]) if len(row) > 2 else "?"
        lines.append(f"- {column_name}: {column_type} (nullable={nullable})")
    return "\n".join(lines)


def fetch_sample_rows(engine: LlmSqlEngine, n: int = DEFAULT_SAMPLE_ROWS) -> SampleRows:
    assert n > 0
    table_name = quote_ident(engine.table_name)
    query = f"SELECT * FROM {table_name} LIMIT {n}"
    result = engine.conn.execute(query)
    description_raw = getattr(result, "description", ())
    description = cast(Sequence[Sequence[Any]], description_raw or ())
    columns = tuple(str(item[0]) for item in description if item)
    fetched_rows = cast(Sequence[Sequence[Any]], result.fetchall())
    rows = tuple(tuple(row) for row in fetched_rows)
    return SampleRows(columns=columns, rows=rows)


def _format_sample_rows(sample_rows: SampleRows) -> str:
    if not sample_rows.columns:
        return "(no columns)"
    if not sample_rows.rows:
        return "(no rows)"
    lines = [", ".join(sample_rows.columns)]
    for row in sample_rows.rows:
        values = ", ".join(str(value) for value in row)
        lines.append(values)
    return "\n".join(lines)


def build_prompt(
    user_query: str,
    table_name: str,
    schema_context: str,
    sample_rows: SampleRows,
) -> str:
    payload = user_query.strip()
    assert payload
    columns = ", ".join(sample_rows.columns) if sample_rows.columns else "(none)"
    formatted_sample_rows = _format_sample_rows(sample_rows)
    return (
        "Generate SQL for DuckDB.\n"
        "Constraints:\n"
        "- Use DuckDB dialect.\n"
        f'- Use only table "{table_name}".\n'
        f"- Use only known columns: {columns}.\n"
        "- Return SQL only.\n"
        "- Return exactly one statement.\n"
        "- Statement must be SELECT or WITH ... SELECT.\n\n"
        f"User request:\n{payload}\n\n"
        f"{schema_context}\n\n"
        "First rows:\n"
        f"{formatted_sample_rows}"
    )


def _strip_markdown_code_fence(raw_text: str) -> str:
    stripped = raw_text.strip()
    if not stripped.startswith("```"):
        return stripped
    lines = stripped.splitlines()
    if len(lines) < 3:
        return stripped
    if lines[-1].strip() != "```":
        return stripped
    return "\n".join(lines[1:-1]).strip()


def _llm_response_content(response: Any) -> str:
    choices = getattr(response, "choices", None)
    if not choices:
        raise ValueError("LLM response missing choices.")
    first_choice = choices[0]
    message = getattr(first_choice, "message", None)
    content = getattr(message, "content", "")
    if not isinstance(content, str):
        raise ValueError("LLM response content is empty.")
    text = _strip_markdown_code_fence(content).strip()
    if not text:
        raise ValueError("LLM response content is empty.")
    return text


def generate_sql(prompt: str, model: str) -> str:
    litellm_completion = cast(CompletionFn, getattr(litellm, "completion"))
    response = litellm_completion(
        model=model,
        messages=[
            {"role": "system", "content": "You write DuckDB SQL from natural language requests."},
            {"role": "user", "content": prompt},
        ],
        temperature=0,
    )
    return _llm_response_content(response)


def _has_with_clause(statement: exp.Expression) -> bool:
    return statement.args.get("with") is not None


def _validate_statement_shape(statement: exp.Expression) -> str | None:
    if isinstance(statement, exp.Select):
        return None
    if _has_with_clause(statement):
        return None
    return "Generated SQL must be SELECT or WITH ... SELECT."


def _unknown_table_refs(statement: exp.Expression, table_name: str) -> list[str]:
    cte_names = {
        cte.alias_or_name.casefold()
        for cte in statement.find_all(exp.CTE)
        if cte.alias_or_name and cte.alias_or_name.strip()
    }
    allowed_table_names = {table_name.casefold(), *cte_names}
    unknown: set[str] = set()
    for table in statement.find_all(exp.Table):
        if table.args.get("db") is not None or table.args.get("catalog") is not None:
            unknown.add(str(table))
            continue
        name = table.name.strip()
        if name.casefold() in allowed_table_names:
            continue
        unknown.add(str(table))
    return sorted(unknown)


def validate_generated_sql(sql: str, table_name: str) -> str | None:
    payload = sql.strip()
    if not payload:
        return "Generated SQL is empty."
    try:
        parse_sql = cast(Callable[..., list[exp.Expression | None]], getattr(sqlglot, "parse"))
        statements = parse_sql(payload, read="duckdb")
    except ParseError as exc:
        return f"Generated SQL is invalid DuckDB SQL: {exc}"
    if len(statements) != 1:
        return "Generated SQL must be a single statement."
    statement = statements[0]
    if statement is None:
        return "Generated SQL is invalid DuckDB SQL."
    shape_error = _validate_statement_shape(statement)
    if shape_error is not None:
        return shape_error
    unknown_tables = _unknown_table_refs(statement, table_name)
    if not unknown_tables:
        return None
    tables_csv = ", ".join(unknown_tables)
    return f'Generated SQL references unknown table(s): {tables_csv}. Allowed table: "{table_name}".'
