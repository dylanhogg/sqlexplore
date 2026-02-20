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
MARIMO_SAVE_COMMAND = "/save-marimo"


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
    setup_sql_lines = [f"        {json.dumps(statement)}," for statement in setup_sql]
    lines = [
        "@app.cell",
        "def _(duckdb):",
        f"    conn = duckdb.connect(database={json.dumps(export.database)})",
        f"    session_id = {json.dumps(export.session_id)}",
        f"    table_name = {json.dumps(export.table_name)}",
        f"    data_paths = {json.dumps([str(path.resolve()) for path in export.data_paths])}",
        "    setup_sql = [",
        *setup_sql_lines,
        "    ]",
        "    for sql in setup_sql:",
        "        conn.execute(sql)",
        "    return conn, data_paths, session_id, table_name",
        "",
    ]
    return lines


def _build_replay_cells(replay_steps: list[ReplayStep]) -> list[str]:
    lines: list[str] = []
    for index, step in enumerate(replay_steps, start=1):
        lines.extend(_render_replay_cell(index, step))
    return lines


def _render_replay_cell(index: int, step: ReplayStep) -> list[str]:
    heading_lines = _render_heading_lines(index, step)
    if step.kind == "sql":
        sql_text = _triple_quote_safe(step.text)
        return _render_sql_cell_lines(heading_lines, sql_text)
    return _render_comment_cell_lines(heading_lines)


def _render_sql_cell_lines(heading_lines: list[str], sql_text: str) -> list[str]:
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


def _render_comment_cell_lines(heading_lines: list[str]) -> list[str]:
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
        step = _build_replay_step(query_history, index, entry)
        if step is not None:
            steps.append(step)
    return steps


def _build_replay_step(
    query_history: tuple[QueryHistoryEntry, ...],
    index: int,
    entry: QueryHistoryEntry,
) -> ReplayStep | None:
    query_text = entry.query_text.strip()
    if not query_text:
        return None
    if _is_save_marimo_command(query_text):
        return None
    if entry.query_type in SQL_QUERY_TYPES:
        return _build_sql_replay_step(query_history, index, entry, query_text)
    if entry.query_type != "user_entered_command":
        return None
    if _skip_redundant_command_entry(query_history, index):
        return None
    return ReplayStep(
        kind="comment",
        text=query_text,
        title="command-only entry (non-replayable)",
        detail_lines=(f"command: {query_text}", f"status: {entry.query_status}"),
    )


def _build_sql_replay_step(
    query_history: tuple[QueryHistoryEntry, ...],
    index: int,
    entry: QueryHistoryEntry,
    query_text: str,
) -> ReplayStep:
    title, detail_lines = _describe_sql_step(query_history, index, entry)
    return ReplayStep(kind="sql", text=query_text, title=title, detail_lines=detail_lines)


def _is_save_marimo_command(query_text: str) -> bool:
    command_name = query_text.split(maxsplit=1)[0].casefold()
    return command_name == MARIMO_SAVE_COMMAND


def _describe_sql_step(
    query_history: tuple[QueryHistoryEntry, ...],
    index: int,
    entry: QueryHistoryEntry,
) -> tuple[str, tuple[str, ...]]:
    if entry.query_type == "user_entered_sql":
        return "user-entered SQL", ("origin: typed directly in sqlexplore",)
    command_entry = _origin_command_entry(query_history, index)
    if entry.query_type == "command_generated_sql":
        return "helper-command SQL", _command_source_details(command_entry)
    if entry.query_type == "llm_generated_sql":
        return "LLM-generated SQL", _command_source_details(command_entry, include_llm_prompt=True)
    return "SQL", ()


def _command_source_details(
    command_entry: QueryHistoryEntry | None,
    *,
    include_llm_prompt: bool = False,
) -> tuple[str, ...]:
    if command_entry is None:
        if include_llm_prompt:
            return ("source command: unavailable", "llm prompt: unavailable")
        return ("source command: unavailable",)
    command_text = command_entry.query_text.strip()
    details = [f"source command: {command_text}"]
    if include_llm_prompt:
        details.append(f"llm prompt: {_llm_prompt_text(command_text)}")
    details.append(f"status: {command_entry.query_status}")
    return tuple(details)


def _origin_command_entry(query_history: tuple[QueryHistoryEntry, ...], sql_index: int) -> QueryHistoryEntry | None:
    for neighbor_index in (sql_index + 1, sql_index - 1):
        candidate = _history_entry_at(query_history, neighbor_index)
        if candidate is not None and candidate.query_type == "user_entered_command":
            return candidate
    return None


def _history_entry_at(query_history: tuple[QueryHistoryEntry, ...], index: int) -> QueryHistoryEntry | None:
    if index < 0 or index >= len(query_history):
        return None
    return query_history[index]


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
