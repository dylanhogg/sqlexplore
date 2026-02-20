from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from sqlexplore.core.engine_models import QueryHistoryEntry
from sqlexplore.core.result_utils import sql_literal
from sqlexplore.core.sql_templates import DEFAULT_LOAD_QUERY_TEMPLATE, TXT_LOAD_QUERY_TEMPLATE, render_load_query

SQL_QUERY_TYPES = frozenset({"user_entered_sql", "command_generated_sql", "llm_generated_sql"})
GENERATED_SQL_TYPES = frozenset({"command_generated_sql", "llm_generated_sql"})


@dataclass(frozen=True, slots=True)
class ReaderSpec:
    function_name: str
    args: str
    query_template: str = DEFAULT_LOAD_QUERY_TEMPLATE


@dataclass(frozen=True, slots=True)
class ReplayStep:
    kind: Literal["sql", "comment"]
    text: str
    title: str
    detail_lines: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class MarimoSessionExport:
    session_id: str
    data_paths: tuple[Path, ...]
    table_name: str
    database: str
    query_history: tuple[QueryHistoryEntry, ...]


def export_session_to_marimo(export: MarimoSessionExport, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = (output_dir / f"marimo_{export.session_id}.py").resolve()
    notebook_text = build_marimo_notebook(export)
    output_path.write_text(notebook_text, encoding="utf-8")
    return output_path


def build_marimo_notebook(export: MarimoSessionExport) -> str:
    setup_sql = _setup_sql_statements(export.data_paths, export.table_name)
    replay_steps = _build_replay_steps(export.query_history)
    lines = [
        "import marimo",
        "",
        "app = marimo.App()",
        "",
        "@app.cell",
        "def _():",
        "    import duckdb",
        "    return (duckdb,)",
        "",
    ]
    lines.extend(_build_setup_cell(export, setup_sql))
    lines.extend(_build_replay_cells(replay_steps))
    lines.extend(
        [
            'if __name__ == "__main__":',
            "    app.run()",
            "",
        ]
    )
    return "\n".join(lines)


def _build_setup_cell(export: MarimoSessionExport, setup_sql: list[str]) -> list[str]:
    lines = [
        "@app.cell",
        "def _(duckdb):",
        f"    conn = duckdb.connect(database={json.dumps(export.database)})",
        f"    session_id = {json.dumps(export.session_id)}",
        f"    table_name = {json.dumps(export.table_name)}",
        f"    data_paths = {json.dumps([str(path.resolve()) for path in export.data_paths])}",
        "    setup_sql = [",
    ]
    for statement in setup_sql:
        lines.append(f"        {json.dumps(statement)},")
    lines.extend(
        [
            "    ]",
            "    for sql in setup_sql:",
            "        conn.execute(sql)",
            "    return conn, data_paths, session_id, table_name",
            "",
        ]
    )
    return lines


def _build_replay_cells(replay_steps: list[ReplayStep]) -> list[str]:
    lines: list[str] = []
    for index, step in enumerate(replay_steps, start=1):
        lines.extend(_render_replay_cell(index, step))
    return lines


def _render_replay_cell(index: int, step: ReplayStep) -> list[str]:
    if step.kind == "sql":
        sql_text = _triple_quote_safe(step.text)
        heading_lines = _render_heading_lines(index, step)
        return [
            "@app.cell",
            "def _(conn):",
            *heading_lines,
            f'    _sql = """{sql_text}"""',
            "    _result = conn.execute(_sql).df()",
            "    _result",
            "    return",
            "",
        ]
    heading_lines = _render_heading_lines(index, step)
    return [
        "@app.cell",
        "def _():",
        *heading_lines,
        "    return",
        "",
    ]


def _build_replay_steps(query_history: tuple[QueryHistoryEntry, ...]) -> list[ReplayStep]:
    steps: list[ReplayStep] = []
    for index, entry in enumerate(query_history):
        query_text = entry.query_text.strip()
        if not query_text:
            continue
        command_name = query_text.split(maxsplit=1)[0].casefold()
        if command_name == "/save-marimo":
            continue
        if entry.query_type in SQL_QUERY_TYPES:
            title, detail_lines = _describe_sql_step(query_history, index, entry)
            steps.append(ReplayStep(kind="sql", text=query_text, title=title, detail_lines=detail_lines))
            continue
        if entry.query_type != "user_entered_command":
            continue
        if _skip_redundant_command_entry(query_history, index):
            continue
        steps.append(
            ReplayStep(
                kind="comment",
                text=query_text,
                title="command-only entry (non-replayable)",
                detail_lines=(f"command: {query_text}", f"status: {entry.query_status}"),
            )
        )
    return steps


def _describe_sql_step(
    query_history: tuple[QueryHistoryEntry, ...],
    index: int,
    entry: QueryHistoryEntry,
) -> tuple[str, tuple[str, ...]]:
    if entry.query_type == "user_entered_sql":
        return "user-entered SQL", ("origin: typed directly in sqlexplore",)
    command_entry = _origin_command_entry(query_history, index)
    if entry.query_type == "command_generated_sql":
        return "helper-command SQL", _helper_details(command_entry)
    if entry.query_type == "llm_generated_sql":
        return "LLM-generated SQL", _llm_details(command_entry)
    return "SQL", ()


def _helper_details(command_entry: QueryHistoryEntry | None) -> tuple[str, ...]:
    if command_entry is None:
        return ("source command: unavailable",)
    return (
        f"source command: {command_entry.query_text.strip()}",
        f"status: {command_entry.query_status}",
    )


def _llm_details(command_entry: QueryHistoryEntry | None) -> tuple[str, ...]:
    if command_entry is None:
        return ("source command: unavailable", "llm prompt: unavailable")
    command_text = command_entry.query_text.strip()
    prompt = _llm_prompt_text(command_text)
    return (
        f"source command: {command_text}",
        f"llm prompt: {prompt}",
        f"status: {command_entry.query_status}",
    )


def _origin_command_entry(query_history: tuple[QueryHistoryEntry, ...], sql_index: int) -> QueryHistoryEntry | None:
    next_index = sql_index + 1
    if next_index < len(query_history):
        candidate = query_history[next_index]
        if candidate.query_type == "user_entered_command":
            return candidate
    prev_index = sql_index - 1
    if prev_index >= 0:
        candidate = query_history[prev_index]
        if candidate.query_type == "user_entered_command":
            return candidate
    return None


def _llm_prompt_text(command_text: str) -> str:
    normalized = command_text.strip()
    prefix = "/llm-query"
    if not normalized.casefold().startswith(prefix):
        return "unavailable"
    return normalized[len(prefix) :].strip() or "unavailable"


def _skip_redundant_command_entry(query_history: tuple[QueryHistoryEntry, ...], index: int) -> bool:
    if index == 0:
        return False
    current = query_history[index]
    previous = query_history[index - 1]
    if current.query_status != "success":
        return False
    return previous.query_type in GENERATED_SQL_TYPES


def _setup_sql_statements(data_paths: tuple[Path, ...], table_name: str) -> list[str]:
    if not data_paths:
        raise ValueError("Cannot export marimo notebook without data paths")
    table_ident = _quote_ident(table_name)
    setup_sql = [f"DROP VIEW IF EXISTS {table_ident}"]
    source_views: list[str] = []
    for source_index, data_path in enumerate(data_paths, start=1):
        source_view = f"{table_name}_src_{source_index}"
        source_ident = _quote_ident(source_view)
        source_views.append(source_view)
        setup_sql.append(f"DROP VIEW IF EXISTS {source_ident}")
        setup_sql.append(f"CREATE VIEW {source_ident} AS {_source_load_query(data_path.resolve())}")
    union_sql = " UNION ALL ".join(f"SELECT * FROM {_quote_ident(source_view)}" for source_view in source_views)
    setup_sql.append(f"CREATE VIEW {table_ident} AS {union_sql}")
    return setup_sql


def _source_load_query(data_path: Path) -> str:
    reader = _detect_reader(data_path)
    source_sql = f"{reader.function_name}({sql_literal(str(data_path))}{reader.args})"
    return render_load_query(source_sql, reader.query_template)


def _detect_reader(data_path: Path) -> ReaderSpec:
    suffix = data_path.suffix.lower()
    if suffix == ".csv":
        return ReaderSpec(function_name="read_csv_auto", args="")
    if suffix == ".tsv":
        return ReaderSpec(function_name="read_csv_auto", args=", delim='\\t'")
    if suffix in {".parquet", ".pq"}:
        return ReaderSpec(function_name="read_parquet", args="")
    if suffix == ".txt":
        return ReaderSpec(function_name="read_text", args="", query_template=TXT_LOAD_QUERY_TEMPLATE)
    raise ValueError(f"Unsupported source extension for marimo export: {data_path.suffix}")


def _quote_ident(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _comment_safe(text: str) -> str:
    return text.replace("\n", "\\n")


def _triple_quote_safe(text: str) -> str:
    return text.replace("\\", "\\\\").replace('"""', '\\"\\"\\"')


def _render_heading_lines(index: int, step: ReplayStep) -> list[str]:
    lines = [f"    # step {index}: {_comment_safe(step.title)}"]
    for detail_line in step.detail_lines:
        lines.append(f"    # {_comment_safe(detail_line)}")
    return lines
