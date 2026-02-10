from __future__ import annotations

import json
import math
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Literal, cast

import duckdb
import typer
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table
from rich.text import Text
from sqlglot.tokens import Tokenizer as SqlglotTokenizer
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.widgets import DataTable, Footer, Header, OptionList, RichLog, Static, TextArea

app = typer.Typer(
    help="Interactive DuckDB SQL explorer for CSV/TSV/Parquet files.",
    pretty_exceptions_enable=False,
    pretty_exceptions_show_locals=True,
)

ResultStatus = Literal["ok", "info", "error"]
CompletionKind = Literal[
    "helper_command",
    "sql_keyword",
    "table",
    "column",
    "function",
    "snippet",
    "value",
]
SqlClause = Literal["unknown", "select", "from", "join", "where", "group_by", "having", "order_by", "limit", "join_on"]
SQL_KEYWORDS = [
    "SELECT",
    "FROM",
    "DATA",
    "WHERE",
    "GROUP",
    "BY",
    "HAVING",
    "ORDER",
    "LIMIT",
    "AS",
    "AND",
    "OR",
    "NOT",
    "IN",
    "IS",
    "NULL",
    "COUNT",
    "SUM",
    "AVG",
    "MIN",
    "MAX",
    "DISTINCT",
    "CASE",
    "WHEN",
    "THEN",
    "ELSE",
    "END",
    "LIKE",
    "BETWEEN",
    "DESC",
    "ASC",
    "JOIN",
    "LEFT",
    "RIGHT",
    "INNER",
    "OUTER",
    "ON",
]
DEFAULT_HELPER_COMMANDS = (
    "/help",
    "/schema",
    "/sample",
    "/filter",
    "/sort",
    "/group",
    "/agg",
    "/top",
    "/profile",
    "/describe",
    "/history",
    "/rerun",
    "/rows",
    "/values",
    "/limit",
    "/save",
    "/last",
    "/clear",
    "/exit",
    "/quit",
)
_HELPER_PREFIX_RE = re.compile(r"(?<!\S)(/[A-Za-z_]*)$")
_IDENT_PREFIX_RE = re.compile(r"([A-Za-z_][A-Za-z0-9_$]*)$")
_QUOTED_PREFIX_RE = re.compile(r'("(?:""|[^"])*)$')
_SIMPLE_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
_QUALIFIED_FUNCTION_LABEL_RE = re.compile(
    r'^(?P<prefix>(?:"(?:""|[^"]+)"|[A-Za-z_][A-Za-z0-9_$]*)\.)+(?P<func>"(?:""|[^"]+)"|[A-Za-z_][A-Za-z0-9_$]*)\('
)
_QUOTED_FUNCTION_LABEL_RE = re.compile(r'^(?P<func>"(?:""|[^"]+)")\(')
_AGGREGATE_FUNCTIONS = frozenset({"COUNT", "SUM", "AVG", "MIN", "MAX"})


@dataclass(slots=True)
class QueryResult:
    sql: str
    columns: list[str]
    rows: list[tuple[Any, ...]]
    elapsed_ms: float
    total_rows: int
    truncated: bool


@dataclass(slots=True)
class EngineResponse:
    status: ResultStatus
    message: str
    result: QueryResult | None = None
    generated_sql: str | None = None
    should_exit: bool = False
    load_query: str | None = None
    clear_editor: bool = False


@dataclass(slots=True)
class CompletionItem:
    label: str
    insert_text: str
    kind: CompletionKind
    detail: str = ""
    replacement_start: int = 0
    replacement_end: int = 0
    score: int = 0


@dataclass(slots=True)
class CompletionContext:
    text: str
    cursor_row: int
    cursor_col: int
    line_before_cursor: str
    mode: Literal["sql", "helper"]
    prefix: str
    replacement_start: int
    replacement_end: int
    sql_clause: SqlClause = "unknown"
    helper_command: str | None = None
    helper_args: str = ""
    helper_has_trailing_space: bool = False
    completing_command_name: bool = False
    sql_function: str | None = None
    inside_function_args: bool = False


@dataclass(slots=True)
class CommandSpec:
    name: str
    usage: str
    description: str
    handler: Callable[[str], EngineResponse]
    completer: Callable[[str, bool], list[CompletionItem]] | None = None
    aliases: tuple[str, ...] = ()


@dataclass(slots=True)
class FileReader:
    function_name: str
    args: str


def _detect_reader(file_path: Path) -> FileReader:
    suffix = file_path.suffix.lower()
    if suffix == ".csv":
        return FileReader(function_name="read_csv_auto", args="")
    if suffix == ".tsv":
        return FileReader(function_name="read_csv_auto", args=", delim='\\t'")
    if suffix in {".parquet", ".pq"}:
        return FileReader(function_name="read_parquet", args="")
    raise typer.BadParameter("Only .csv, .tsv, and .parquet/.pq files are supported.")


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _is_simple_ident(name: str) -> bool:
    return bool(_SIMPLE_IDENT_RE.fullmatch(name))


def _normalize_function_ident(token: str) -> str:
    if not (token.startswith('"') and token.endswith('"') and len(token) > 1):
        return token
    unquoted = token[1:-1].replace('""', '"')
    return unquoted if _is_simple_ident(unquoted) else token


def _normalize_result_column_label(name: str) -> str:
    qualified_match = _QUALIFIED_FUNCTION_LABEL_RE.match(name)
    if qualified_match is not None:
        func_token = _normalize_function_ident(qualified_match.group("func"))
        return f"{func_token}({name[qualified_match.end() :]}"

    quoted_match = _QUOTED_FUNCTION_LABEL_RE.match(name)
    if quoted_match is not None:
        func_token = _normalize_function_ident(quoted_match.group("func"))
        return f"{func_token}({name[quoted_match.end() :]}"

    return name


def _result_columns(description: list[tuple[Any, ...]] | None) -> list[str]:
    if not description:
        return []
    return [_normalize_result_column_label(str(item[0])) for item in description]


def _split_pipe_sections(raw: str) -> list[str]:
    return [part.strip() for part in raw.split("|") if part.strip()]


def _parse_optional_positive_int(raw: str) -> int | None:
    lowered = raw.strip().lower()
    if lowered in {"off", "none"}:
        return None
    try:
        return max(1, int(lowered))
    except ValueError:
        return None


def _format_scalar(value: Any, max_chars: int | None = None) -> str:
    if value is None:
        return "NULL"
    text = str(value)
    if max_chars is None or len(text) <= max_chars:
        return text
    if max_chars <= 3:
        return text[:max_chars]
    return f"{text[: max_chars - 3]}..."


def _is_numeric_type(type_name: str) -> bool:
    upper = type_name.upper()
    return any(marker in upper for marker in ("INT", "DOUBLE", "FLOAT", "DECIMAL", "REAL", "NUMERIC"))


def _is_temporal_type(type_name: str) -> bool:
    upper = type_name.upper()
    return any(marker in upper for marker in ("DATE", "TIME", "TIMESTAMP", "INTERVAL"))


def _is_text_type(type_name: str) -> bool:
    upper = type_name.upper()
    return any(marker in upper for marker in ("CHAR", "TEXT", "STRING", "VARCHAR", "UUID", "JSON"))


def _sort_cell_key(value: Any) -> tuple[int, int, float | str]:
    if value is None:
        return (2, 0, "")
    if isinstance(value, bool):
        return (0, 0, float(int(value)))
    if isinstance(value, int | float):
        number = float(value)
        if math.isnan(number):
            return (1, 0, "")
        return (0, 0, number)
    return (0, 1, str(value).casefold())


class SqlExplorerEngine:
    def __init__(
        self,
        data_path: Path,
        table_name: str,
        database: str,
        default_limit: int,
        max_rows_display: int,
        max_value_chars: int,
    ) -> None:
        self.data_path = data_path
        self.table_name = table_name.replace('"', "")
        self.database = database
        self.default_limit = max(1, default_limit)
        self.max_rows_display = max(1, max_rows_display)
        self.max_value_chars = max(8, max_value_chars)

        self.conn = duckdb.connect(database=database)
        self.executed_sql: list[str] = []
        self.last_sql = self.default_query
        self.last_result_sql: str | None = None

        reader = _detect_reader(self.data_path)
        source_sql = f"{reader.function_name}({_sql_literal(str(self.data_path))}{reader.args})"
        self.conn.execute(f'DROP VIEW IF EXISTS "{self.table_name}"')
        self.conn.execute(f'CREATE VIEW "{self.table_name}" AS SELECT * FROM {source_sql}')

        self._schema_rows: list[tuple[Any, ...]] = []
        self.columns: list[str] = []
        self.column_types: dict[str, str] = {}
        self.column_lookup: dict[str, str] = {}
        self._column_completion_cache: list[CompletionItem] | None = None
        self._aggregate_completion_cache: list[CompletionItem] | None = None
        self._aggregate_arg_completion_cache: dict[str, list[CompletionItem]] = {}
        self._predicate_completion_cache: list[CompletionItem] | None = None
        self._direction_completion_cache: list[CompletionItem] | None = None
        self._sql_clause_completion_cache: dict[str, list[CompletionItem]] = {}
        self.refresh_schema()
        self._command_specs = self._build_command_specs()
        self._command_lookup = self._index_command_specs(self._command_specs)
        self._completion_engine = CompletionEngine(self)

    @property
    def default_query(self) -> str:
        return f'SELECT * FROM "{self.table_name}" LIMIT {self.default_limit}'

    def close(self) -> None:
        self.conn.close()

    def refresh_schema(self) -> None:
        schema_rows = self.conn.execute(f'DESCRIBE "{self.table_name}"').fetchall()
        self._schema_rows = [tuple(row) for row in schema_rows]
        self.columns = [str(row[0]) for row in self._schema_rows]
        self.column_types = {str(row[0]): str(row[1]) for row in self._schema_rows}
        self.column_lookup = {col.lower(): col for col in self.columns}
        self._invalidate_completion_caches()

    def _invalidate_completion_caches(self) -> None:
        self._column_completion_cache = None
        self._aggregate_completion_cache = None
        self._aggregate_arg_completion_cache = {}
        self._predicate_completion_cache = None
        self._direction_completion_cache = None
        self._sql_clause_completion_cache = {}
        if hasattr(self, "_completion_engine"):
            self._completion_engine.clear_cache()

    def helper_commands(self) -> list[str]:
        commands: list[str] = []
        for spec in self._command_specs:
            commands.append(spec.name)
            commands.extend(spec.aliases)
        return commands

    def _command_usage_lines(self) -> list[str]:
        return [spec.usage for spec in self._command_specs]

    def row_count(self) -> int:
        out = self.conn.execute(f'SELECT COUNT(*) FROM "{self.table_name}"').fetchone()
        if out is None:
            return 0
        return int(out[0])

    def schema_preview(self, max_columns: int = 24) -> str:
        rows = self.row_count()
        head = [
            "Data Explorer",
            "",
            f"{self.data_path}",
            f"table: {self.table_name}",
            f"rows: {rows:,}",
            f"columns: {len(self.columns)}",
            "",
            "Schema",
        ]
        for name in self.columns[:max_columns]:
            head.append(f"- {name}: {self.column_types[name]}")
        if len(self.columns) > max_columns:
            head.append(f"... +{len(self.columns) - max_columns} more columns")
        head.extend(
            [
                "",
                "Shortcuts",
                "Ctrl+Enter or F5 run query",
                "Ctrl+N or F6 sample query",
                "Ctrl+L or F7 clear editor",
                "Tab accept completion",
                "Ctrl+Space open completions",
                "Up/Down query history",
                "Ctrl+1 editor, Ctrl+2 results",
                "Ctrl+B toggle Data Explorer",
                "F1 (or Ctrl+Shift+P) help",
                "F10 (or Ctrl+Q) quit",
                "",
                "Helper Commands",
            ]
        )
        head.extend(self._command_usage_lines())
        return "\n".join(head)

    def help_text(self) -> str:
        lines = ["Run standard SQL directly. Helper commands:"]
        lines.extend(self._command_usage_lines())
        lines.extend(
            [
                "",
                (
                    "Editor: Tab/Enter accepts completion; Esc closes completion menu; "
                    "Ctrl+Space opens completions; Up/Down navigates completion menu or query history "
                    "at first/last line; Ctrl+Enter/F5 runs; Ctrl+N/F6 loads sample; Ctrl+L/F7 clears."
                ),
                (
                    "Navigation: Ctrl+1 focuses query editor, Ctrl+2 focuses results, "
                    "Ctrl+B toggles Data Explorer. Help: F1 or Ctrl+Shift+P."
                ),
                "",
                f"settings: limit={self.default_limit}, rows={self.max_rows_display}, values={self.max_value_chars}",
            ]
        )
        return "\n".join(lines)

    def _resolve_column(self, raw_column: str) -> str | None:
        candidate = raw_column.strip()
        if candidate.startswith('"') and candidate.endswith('"') and len(candidate) > 1:
            candidate = candidate[1:-1].replace('""', '"')
        return self.column_lookup.get(candidate.lower())

    def _display_rows(self, rows: list[tuple[Any, ...]]) -> tuple[list[tuple[Any, ...]], bool]:
        if len(rows) <= self.max_rows_display:
            return rows, False
        return rows[: self.max_rows_display], True

    def _table_response(self, columns: list[str], rows: list[tuple[Any, ...]], message: str) -> EngineResponse:
        shown, truncated = self._display_rows(rows)
        result = QueryResult(
            sql="",
            columns=columns,
            rows=shown,
            elapsed_ms=0.0,
            total_rows=len(rows),
            truncated=truncated,
        )
        return EngineResponse(status="ok", message=message, result=result)

    def run_sql(self, sql_text: str, remember: bool = True) -> EngineResponse:
        sql = sql_text.strip().rstrip(";")
        if not sql:
            return EngineResponse(status="info", message="Query is empty.")

        t0 = time.perf_counter()
        try:
            relation = self.conn.execute(sql)
            rows = relation.fetchall()
            columns = _result_columns(relation.description)
        except Exception as exc:  # noqa: BLE001
            return EngineResponse(status="error", message=str(exc))

        elapsed_ms = (time.perf_counter() - t0) * 1000
        self.last_sql = sql
        if remember:
            self.executed_sql.append(sql)

        if not columns:
            return EngineResponse(status="ok", message=f"Statement executed in {elapsed_ms:.1f} ms")

        shown, truncated = self._display_rows(rows)
        self.last_result_sql = sql
        result = QueryResult(
            sql=sql,
            columns=columns,
            rows=shown,
            elapsed_ms=elapsed_ms,
            total_rows=len(rows),
            truncated=truncated,
        )
        message = f"{len(shown):,}/{len(rows):,} rows shown in {elapsed_ms:.1f} ms"
        if truncated:
            message += f" (row display limit={self.max_rows_display})"
        return EngineResponse(status="ok", message=message, result=result)

    def run_input(self, raw_input: str) -> EngineResponse:
        text = raw_input.strip()
        if not text:
            return EngineResponse(status="info", message="Type SQL or /help.")
        if text.startswith("/"):
            return self._run_command(text)
        return self.run_sql(text)

    def _index_command_specs(self, specs: list[CommandSpec]) -> dict[str, CommandSpec]:
        lookup: dict[str, CommandSpec] = {}
        for spec in specs:
            lookup[spec.name.casefold()] = spec
            for alias in spec.aliases:
                lookup[alias.casefold()] = spec
        return lookup

    def _lookup_command(self, raw_name: str) -> CommandSpec | None:
        return self._command_lookup.get(raw_name.casefold())

    def _build_command_specs(self) -> list[CommandSpec]:
        return [
            CommandSpec("/help", "/help", "Show helper command reference.", self._cmd_help),
            CommandSpec("/schema", "/schema", "Show dataset schema.", self._cmd_schema),
            CommandSpec(
                "/sample",
                "/sample [n]",
                "Select sample rows.",
                self._cmd_sample,
                self._complete_sample,
            ),
            CommandSpec(
                "/filter",
                "/filter <where condition>",
                "Filter rows with a WHERE condition.",
                self._cmd_filter,
                self._complete_filter,
            ),
            CommandSpec(
                "/sort",
                "/sort <order expressions>",
                "Sort rows by expression(s).",
                self._cmd_sort,
                self._complete_sort,
            ),
            CommandSpec(
                "/group",
                "/group <group cols> | <aggregates> [| having]",
                "Aggregate by group columns.",
                self._cmd_group,
                self._complete_group,
            ),
            CommandSpec(
                "/agg",
                "/agg <aggregates> [| where]",
                "Run aggregate expression(s).",
                self._cmd_agg,
                self._complete_agg,
            ),
            CommandSpec(
                "/top",
                "/top <column> [n]",
                "Top values by frequency for a column.",
                self._cmd_top,
                self._complete_top,
            ),
            CommandSpec(
                "/profile",
                "/profile <column>",
                "Profile a single column.",
                self._cmd_profile,
                self._complete_profile,
            ),
            CommandSpec("/describe", "/describe", "Describe columns and nulls.", self._cmd_describe),
            CommandSpec(
                "/history",
                "/history [n]",
                "Show recent query history.",
                self._cmd_history,
                self._complete_history,
            ),
            CommandSpec(
                "/rerun",
                "/rerun <history_index>",
                "Rerun a query from history.",
                self._cmd_rerun,
                self._complete_rerun,
            ),
            CommandSpec("/rows", "/rows <n>", "Set row display limit.", self._cmd_rows, self._complete_rows),
            CommandSpec(
                "/values",
                "/values <n>",
                "Set max display length per value.",
                self._cmd_values,
                self._complete_values,
            ),
            CommandSpec("/limit", "/limit <n>", "Set helper query row limit.", self._cmd_limit, self._complete_limit),
            CommandSpec(
                "/save",
                "/save <path.csv|path.parquet|path.json>",
                "Save latest result to disk.",
                self._cmd_save,
                self._complete_save,
            ),
            CommandSpec("/last", "/last", "Load previous SQL into editor.", self._cmd_last),
            CommandSpec("/clear", "/clear", "Clear query editor.", self._cmd_clear),
            CommandSpec("/exit", "/exit or /quit", "Exit SQL explorer.", self._cmd_exit, aliases=("/quit",)),
        ]

    def _usage_error(self, usage: str) -> EngineResponse:
        return EngineResponse(status="error", message=f"Usage: {usage}")

    def _require_no_args(self, args: str, usage: str) -> EngineResponse | None:
        if args.strip():
            return self._usage_error(usage)
        return None

    def _run_command(self, command: str) -> EngineResponse:
        stripped = command.strip()
        if not stripped:
            return EngineResponse(status="info", message="Type SQL or /help.")
        parts = stripped.split(maxsplit=1)
        raw_name = parts[0]
        args = parts[1] if len(parts) == 2 else ""
        spec = self._lookup_command(raw_name)
        if spec is None:
            return EngineResponse(status="error", message=f"Unknown command: {stripped}. Use /help")
        return spec.handler(args)

    def _run_sql_helper(self, sql: str | None, usage: str) -> EngineResponse:
        if sql is None:
            return self._usage_error(usage)
        out = self.run_sql(sql)
        out.generated_sql = sql
        return out

    def _cmd_help(self, args: str) -> EngineResponse:
        err = self._require_no_args(args, "/help")
        if err is not None:
            return err
        return EngineResponse(status="info", message=self.help_text())

    def _cmd_schema(self, args: str) -> EngineResponse:
        err = self._require_no_args(args, "/schema")
        if err is not None:
            return err
        rows = [(str(r[0]), str(r[1]), str(r[2])) for r in self._schema_rows]
        return self._table_response(["column", "type", "nullable"], rows, "Schema")

    def _cmd_sample(self, args: str) -> EngineResponse:
        return self._run_sql_helper(self._sql_for_sample(args), "/sample [n]")

    def _cmd_filter(self, args: str) -> EngineResponse:
        return self._run_sql_helper(self._sql_for_filter(args), "/filter <where condition>")

    def _cmd_sort(self, args: str) -> EngineResponse:
        return self._run_sql_helper(self._sql_for_sort(args), "/sort <order expressions>")

    def _cmd_group(self, args: str) -> EngineResponse:
        return self._run_sql_helper(self._sql_for_group(args), "/group <group cols> | <aggregates> [| having]")

    def _cmd_agg(self, args: str) -> EngineResponse:
        return self._run_sql_helper(self._sql_for_agg(args), "/agg <aggregates> [| where]")

    def _cmd_top(self, args: str) -> EngineResponse:
        return self._run_sql_helper(self._sql_for_top(args), "/top <column> [n]")

    def _cmd_profile(self, args: str) -> EngineResponse:
        payload = args.strip()
        if not payload:
            return self._usage_error("/profile <column>")
        return self._profile_column(payload)

    def _cmd_describe(self, args: str) -> EngineResponse:
        err = self._require_no_args(args, "/describe")
        if err is not None:
            return err
        return self._describe_dataset()

    def _cmd_history(self, args: str) -> EngineResponse:
        payload = args.strip()
        count = 20
        if payload:
            parts = payload.split()
            if len(parts) != 1:
                return self._usage_error("/history [n]")
            try:
                count = max(1, int(parts[0]))
            except ValueError:
                return self._usage_error("/history [n]")
        history = self.executed_sql[-count:]
        start_idx = max(1, len(self.executed_sql) - len(history) + 1)
        rows = [(idx, sql) for idx, sql in enumerate(history, start=start_idx)]
        return self._table_response(["#", "sql"], rows, f"History ({len(history)} queries)")

    def _cmd_rerun(self, args: str) -> EngineResponse:
        payload = args.strip()
        parts = payload.split()
        if len(parts) != 1:
            return self._usage_error("/rerun <n>")
        try:
            idx = int(parts[0])
        except ValueError:
            return EngineResponse(status="error", message="/rerun expects an integer index")
        if idx < 1 or idx > len(self.executed_sql):
            return EngineResponse(status="error", message="History index out of range")
        sql = self.executed_sql[idx - 1]
        out = self.run_sql(sql)
        out.generated_sql = sql
        return out

    def _cmd_rows(self, args: str) -> EngineResponse:
        payload = args.strip()
        if len(payload.split()) != 1:
            return self._usage_error("/rows <n>")
        parsed = _parse_optional_positive_int(payload)
        if parsed is None:
            return self._usage_error("/rows <n>")
        self.max_rows_display = parsed
        return EngineResponse(status="ok", message=f"Row display limit set to {self.max_rows_display}")

    def _cmd_values(self, args: str) -> EngineResponse:
        payload = args.strip()
        if len(payload.split()) != 1:
            return self._usage_error("/values <n>")
        parsed = _parse_optional_positive_int(payload)
        if parsed is None:
            return self._usage_error("/values <n>")
        self.max_value_chars = parsed
        return EngineResponse(status="ok", message=f"Value display limit set to {self.max_value_chars}")

    def _cmd_limit(self, args: str) -> EngineResponse:
        payload = args.strip()
        if len(payload.split()) != 1:
            return self._usage_error("/limit <n>")
        parsed = _parse_optional_positive_int(payload)
        if parsed is None:
            return self._usage_error("/limit <n>")
        self.default_limit = parsed
        return EngineResponse(status="ok", message=f"Default helper query limit set to {self.default_limit}")

    def _cmd_save(self, args: str) -> EngineResponse:
        payload = args.strip()
        if not payload:
            return self._usage_error("/save <path>")
        return self._save_last_result(payload)

    def _cmd_last(self, args: str) -> EngineResponse:
        err = self._require_no_args(args, "/last")
        if err is not None:
            return err
        return EngineResponse(status="info", message="Loaded last SQL in editor.", load_query=self.last_sql)

    def _cmd_clear(self, args: str) -> EngineResponse:
        err = self._require_no_args(args, "/clear")
        if err is not None:
            return err
        return EngineResponse(status="info", message="Editor cleared.", clear_editor=True)

    def _cmd_exit(self, args: str) -> EngineResponse:
        err = self._require_no_args(args, "/exit")
        if err is not None:
            return err
        return EngineResponse(status="info", message="Exiting SQL explorer.", should_exit=True)

    def _sql_for_sample(self, args: str) -> str | None:
        payload = args.strip()
        sample_n = self.default_limit
        if payload:
            parts = payload.split()
            if len(parts) != 1:
                return None
            try:
                sample_n = max(1, int(parts[0]))
            except ValueError:
                return None
        return f'SELECT * FROM "{self.table_name}" LIMIT {sample_n}'

    def _sql_for_filter(self, args: str) -> str | None:
        cond = args.strip()
        if not cond:
            return None
        return f'SELECT * FROM "{self.table_name}" WHERE {cond} LIMIT {self.default_limit}'

    def _sql_for_sort(self, args: str) -> str | None:
        expr = args.strip()
        if not expr:
            return None
        return f'SELECT * FROM "{self.table_name}" ORDER BY {expr} LIMIT {self.default_limit}'

    def _sql_for_group(self, args: str) -> str | None:
        payload = args.strip()
        parts = _split_pipe_sections(payload)
        if not parts:
            return None
        group_cols = parts[0]
        if len(parts) == 1:
            return (
                f'SELECT {group_cols}, COUNT(*) AS count FROM "{self.table_name}" '
                f"GROUP BY {group_cols} ORDER BY count DESC, {group_cols}"
            )
        aggs = parts[1]
        having = parts[2] if len(parts) > 2 else ""
        sql = f'SELECT {group_cols}, {aggs} FROM "{self.table_name}" GROUP BY {group_cols}'
        if having:
            sql += f" HAVING {having}"
        sql += f" ORDER BY {group_cols}"
        return sql

    def _sql_for_agg(self, args: str) -> str | None:
        payload = args.strip()
        parts = _split_pipe_sections(payload)
        if not parts:
            return None
        aggs = parts[0]
        where = parts[1] if len(parts) > 1 else ""
        sql = f'SELECT {aggs} FROM "{self.table_name}"'
        if where:
            sql += f" WHERE {where}"
        return sql

    def _sql_for_top(self, args: str) -> str | None:
        parts = args.strip().split()
        if len(parts) < 1 or len(parts) > 2:
            return None
        resolved = self._resolve_column(parts[0])
        if resolved is None:
            return None
        top_n = 10
        if len(parts) == 2:
            try:
                top_n = max(1, int(parts[1]))
            except ValueError:
                return None
        qcol = _quote_ident(resolved)
        return (
            f'SELECT {qcol} AS value, COUNT(*) AS count FROM "{self.table_name}" '
            f"GROUP BY {qcol} ORDER BY count DESC, value LIMIT {top_n}"
        )

    def _describe_dataset(self) -> EngineResponse:
        total_rows = self.row_count()
        total_columns = len(self.columns)
        column_rows: list[tuple[Any, ...]] = []
        for column in self.columns:
            qcol = _quote_ident(column)
            out = self.conn.execute(
                f'''SELECT
                    COUNT(*) AS rows,
                    COUNT({qcol}) AS non_null,
                    COUNT(DISTINCT {qcol}) AS distinct_count
                FROM "{self.table_name}"'''
            ).fetchone()
            if out is None:
                continue
            rows = int(out[0])
            non_null = int(out[1])
            distinct_count = int(out[2])
            nulls = rows - non_null
            null_pct = (100.0 * nulls / rows) if rows else 0.0
            column_rows.append((column, self.column_types[column], distinct_count, nulls, f"{null_pct:.2f}%"))

        message = f"Dataset: {total_rows:,} rows x {total_columns} columns"
        return self._table_response(["column", "type", "distinct", "nulls", "null_%"], column_rows, message)

    def _profile_column(self, raw_column: str) -> EngineResponse:
        column = self._resolve_column(raw_column)
        if column is None:
            return EngineResponse(status="error", message=f"Unknown column: {raw_column}")

        qcol = _quote_ident(column)
        out = self.conn.execute(
            f'''SELECT
                COUNT(*) AS rows,
                COUNT(DISTINCT {qcol}) AS distinct_count,
                SUM(CASE WHEN {qcol} IS NULL THEN 1 ELSE 0 END) AS null_count,
                MIN({qcol}) AS min_value,
                MAX({qcol}) AS max_value
            FROM "{self.table_name}"'''
        ).fetchone()
        if out is None:
            return EngineResponse(status="error", message=f"Failed to profile column: {column}")

        metric_rows: list[tuple[Any, ...]] = [
            ("column", column),
            ("type", self.column_types[column]),
            ("rows", int(out[0])),
            ("distinct", int(out[1])),
            ("nulls", int(out[2])),
            ("min", _format_scalar(out[3])),
            ("max", _format_scalar(out[4])),
        ]

        if _is_numeric_type(self.column_types[column]):
            quantiles = self.conn.execute(
                f'''SELECT
                    QUANTILE_CONT({qcol}, 0.25),
                    QUANTILE_CONT({qcol}, 0.50),
                    QUANTILE_CONT({qcol}, 0.75)
                FROM "{self.table_name}"
                WHERE {qcol} IS NOT NULL'''
            ).fetchone()
            if quantiles is not None:
                metric_rows.append(("p25", _format_scalar(quantiles[0])))
                metric_rows.append(("p50", _format_scalar(quantiles[1])))
                metric_rows.append(("p75", _format_scalar(quantiles[2])))

        return self._table_response(["metric", "value"], metric_rows, f"Profile for {column}")

    def _save_last_result(self, path_arg: str) -> EngineResponse:
        if self.last_result_sql is None:
            return EngineResponse(status="error", message="No query result to save yet")

        out_path = Path(path_arg).expanduser().resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        suffix = out_path.suffix.lower()

        try:
            if suffix == ".csv":
                self.conn.execute(
                    f"COPY ({self.last_result_sql}) TO {_sql_literal(str(out_path))} (HEADER, DELIMITER ',')"
                )
            elif suffix in {".parquet", ".pq"}:
                self.conn.execute(f"COPY ({self.last_result_sql}) TO {_sql_literal(str(out_path))} (FORMAT PARQUET)")
            elif suffix == ".json":
                relation = self.conn.execute(self.last_result_sql)
                rows = relation.fetchall()
                columns = _result_columns(relation.description)
                records = [dict(zip(columns, row, strict=False)) for row in rows]
                out_path.write_text(json.dumps(records, indent=2, default=str), encoding="utf-8")
            else:
                return EngineResponse(status="error", message="Unsupported extension. Use .csv, .parquet/.pq, or .json")
        except Exception as exc:  # noqa: BLE001
            return EngineResponse(status="error", message=f"Save failed: {exc}")

        return EngineResponse(status="ok", message=f"Saved result to {out_path}")

    def _column_expr(self, column: str) -> str:
        return column if _is_simple_ident(column) else _quote_ident(column)

    def _base_completion_item(
        self,
        value: str,
        kind: CompletionKind,
        detail: str = "",
        score: int = 0,
    ) -> CompletionItem:
        return CompletionItem(label=value, insert_text=value, kind=kind, detail=detail, score=score)

    def _column_completion_items(self) -> list[CompletionItem]:
        if self._column_completion_cache is not None:
            return self._column_completion_cache
        items: list[CompletionItem] = []
        seen: set[str] = set()
        for column in self.columns:
            expr = self._column_expr(column)
            key = expr.casefold()
            if key in seen:
                continue
            seen.add(key)
            items.append(self._base_completion_item(expr, "column", self.column_types[column], score=120))
            if _is_simple_ident(column):
                quoted = _quote_ident(column)
                quoted_key = quoted.casefold()
                if quoted_key not in seen:
                    seen.add(quoted_key)
                    items.append(
                        self._base_completion_item(
                            quoted,
                            "column",
                            f"{self.column_types[column]} (quoted)",
                            score=112,
                        )
                    )
        self._column_completion_cache = items
        return items

    def _numeric_completion_items(self, values: list[int], detail: str = "") -> list[CompletionItem]:
        seen: set[int] = set()
        items: list[CompletionItem] = []
        for value in values:
            if value <= 0 or value in seen:
                continue
            seen.add(value)
            items.append(self._base_completion_item(str(value), "value", detail=detail, score=80))
        return items

    @staticmethod
    def _aggregate_arg_score(func_name: str, type_name: str) -> int:
        function = func_name.strip().upper()
        score = 118

        if function in {"SUM", "AVG"}:
            if _is_numeric_type(type_name):
                return score + 42
            return score - 46
        if function in {"MIN", "MAX"}:
            if _is_numeric_type(type_name):
                return score + 34
            if _is_temporal_type(type_name):
                return score + 30
            if _is_text_type(type_name):
                return score + 8
            return score + 4
        if function == "COUNT":
            if _is_numeric_type(type_name):
                return score + 18
            if _is_temporal_type(type_name):
                return score + 14
            return score + 10
        return score

    def _column_completion_items_for_aggregate(self, func_name: str) -> list[CompletionItem]:
        function = func_name.strip().upper()
        cached = self._aggregate_arg_completion_cache.get(function)
        if cached is not None:
            return cached

        items: list[CompletionItem] = []
        seen: set[str] = set()

        if function == "COUNT":
            items.append(self._base_completion_item("*", "value", "count rows", score=170))
            seen.add("*")

        for column in self.columns:
            expr = self._column_expr(column)
            type_name = self.column_types[column]
            base_score = self._aggregate_arg_score(function, type_name)

            expr_key = expr.casefold()
            if expr_key not in seen:
                seen.add(expr_key)
                items.append(self._base_completion_item(expr, "column", type_name, score=base_score))

            if _is_simple_ident(column):
                quoted = _quote_ident(column)
                quoted_key = quoted.casefold()
                if quoted_key not in seen:
                    seen.add(quoted_key)
                    items.append(
                        self._base_completion_item(
                            quoted,
                            "column",
                            f"{type_name} (quoted)",
                            score=base_score - 8,
                        )
                    )

            if function == "COUNT":
                distinct_expr = f"DISTINCT {expr}"
                distinct_key = distinct_expr.casefold()
                if distinct_key not in seen:
                    seen.add(distinct_key)
                    items.append(
                        self._base_completion_item(
                            distinct_expr,
                            "snippet",
                            f"{type_name} (distinct)",
                            score=base_score + 10,
                        )
                    )

        items.sort(
            key=lambda item: (
                -item.score,
                len(item.insert_text),
                item.insert_text.casefold(),
            )
        )
        self._aggregate_arg_completion_cache[function] = items
        return items

    def _ranked_columns_for_aggregate(self, func_name: str) -> list[str]:
        function = func_name.strip().upper()
        ranked = list(self.columns)
        ranked.sort(
            key=lambda column: (
                -self._aggregate_arg_score(function, self.column_types[column]),
                len(self._column_expr(column)),
                self._column_expr(column).casefold(),
            )
        )
        return ranked

    @staticmethod
    def _aggregate_alias_suffix(column: str) -> str:
        suffix = re.sub(r"[^A-Za-z0-9_]+", "_", column).strip("_").lower()
        if not suffix:
            return "value"
        if suffix[0].isdigit():
            return f"col_{suffix}"
        return suffix

    def _aggregate_snippet_item(self, func_name: str, detail: str, score: int) -> CompletionItem | None:
        ranked_columns = self._ranked_columns_for_aggregate(func_name)
        if not ranked_columns:
            return None
        best_column = ranked_columns[0]
        column_type = self.column_types[best_column]
        expr = self._column_expr(best_column)
        alias = f"{func_name.lower()}_{self._aggregate_alias_suffix(best_column)}"
        suffix = " (non-numeric fallback)" if func_name in {"SUM", "AVG"} and not _is_numeric_type(column_type) else ""
        return self._base_completion_item(
            f"{func_name}({expr}) AS {alias}",
            "snippet",
            f"{detail}{suffix}",
            score=score,
        )

    def _aggregate_completion_items(self) -> list[CompletionItem]:
        if self._aggregate_completion_cache is not None:
            return self._aggregate_completion_cache
        items: list[CompletionItem] = [
            self._base_completion_item("COUNT(*) AS count", "snippet", "count rows", score=140)
        ]
        aggregate_specs = [
            ("SUM", "sum numeric values", 135),
            ("AVG", "average numeric values", 130),
            ("MIN", "minimum value", 125),
            ("MAX", "maximum value", 125),
        ]
        for func_name, detail, score in aggregate_specs:
            snippet = self._aggregate_snippet_item(func_name, detail, score)
            if snippet is not None:
                items.append(snippet)
        self._aggregate_completion_cache = items
        return items

    def _predicate_completion_items(self) -> list[CompletionItem]:
        if self._predicate_completion_cache is not None:
            return self._predicate_completion_cache
        items = list(self._column_completion_items())
        first_col = self._column_expr(self.columns[0]) if self.columns else "column_name"
        items.extend(
            [
                self._base_completion_item(f"{first_col} IS NOT NULL", "snippet", "null check", score=115),
                self._base_completion_item(f"{first_col} = ", "snippet", "equality predicate", score=110),
                self._base_completion_item(f"{first_col} > ", "snippet", "greater-than predicate", score=105),
                self._base_completion_item(f"{first_col} LIKE '%'", "snippet", "string pattern predicate", score=100),
            ]
        )
        self._predicate_completion_cache = items
        return items

    def _complete_sample(self, args: str, trailing_space: bool) -> list[CompletionItem]:
        del args, trailing_space
        return self._numeric_completion_items([self.default_limit, 10, 25, 50, 100], detail="sample rows")

    def _complete_filter(self, args: str, trailing_space: bool) -> list[CompletionItem]:
        del args, trailing_space
        return self._predicate_completion_items()

    def _complete_sort(self, args: str, trailing_space: bool) -> list[CompletionItem]:
        del args, trailing_space
        items = list(self._column_completion_items())
        for column in self.columns[: min(8, len(self.columns))]:
            expr = self._column_expr(column)
            items.append(self._base_completion_item(f"{expr} DESC", "snippet", "descending sort", score=110))
            items.append(self._base_completion_item(f"{expr} ASC", "snippet", "ascending sort", score=105))
        return items

    def _complete_group(self, args: str, trailing_space: bool) -> list[CompletionItem]:
        del trailing_space
        pipe_count = args.count("|")
        if pipe_count == 0:
            return self._column_completion_items()
        if pipe_count == 1:
            return self._aggregate_completion_items()
        return [
            self._base_completion_item("COUNT(*) > 1", "snippet", "having clause", score=120),
            self._base_completion_item("SUM(...) > 0", "snippet", "having clause", score=100),
        ]

    def _complete_agg(self, args: str, trailing_space: bool) -> list[CompletionItem]:
        del trailing_space
        if args.count("|") == 0:
            return self._aggregate_completion_items()
        return self._predicate_completion_items()

    def _complete_top(self, args: str, trailing_space: bool) -> list[CompletionItem]:
        parts = args.split()
        if not parts:
            return self._column_completion_items()
        if len(parts) == 1 and not trailing_space:
            return self._column_completion_items()
        if len(parts) in {1, 2}:
            return self._numeric_completion_items([10, self.default_limit, 25, 50], detail="top rows")
        return []

    def _complete_profile(self, args: str, trailing_space: bool) -> list[CompletionItem]:
        del args, trailing_space
        return self._column_completion_items()

    def _complete_history(self, args: str, trailing_space: bool) -> list[CompletionItem]:
        del args, trailing_space
        return self._numeric_completion_items([20, 50, 100], detail="history size")

    def _complete_rerun(self, args: str, trailing_space: bool) -> list[CompletionItem]:
        del args, trailing_space
        if not self.executed_sql:
            return [self._base_completion_item("1", "value", "history index", score=80)]
        start = max(1, len(self.executed_sql) - 9)
        items: list[CompletionItem] = []
        for idx in range(len(self.executed_sql), start - 1, -1):
            sql = self.executed_sql[idx - 1].replace("\n", " ")
            detail = sql if len(sql) <= 50 else f"{sql[:47]}..."
            items.append(self._base_completion_item(str(idx), "value", detail=detail, score=140))
        return items

    def _complete_rows(self, args: str, trailing_space: bool) -> list[CompletionItem]:
        del args, trailing_space
        return self._numeric_completion_items([100, 200, 400, 1000], detail="row display limit")

    def _complete_values(self, args: str, trailing_space: bool) -> list[CompletionItem]:
        del args, trailing_space
        return self._numeric_completion_items([80, 120, 160, 240], detail="value character limit")

    def _complete_limit(self, args: str, trailing_space: bool) -> list[CompletionItem]:
        del args, trailing_space
        return self._numeric_completion_items([10, 25, 100, 500], detail="helper query limit")

    def _complete_save(self, args: str, trailing_space: bool) -> list[CompletionItem]:
        del args, trailing_space
        return [
            self._base_completion_item("results.csv", "value", "export CSV", score=100),
            self._base_completion_item("results.parquet", "value", "export Parquet", score=95),
            self._base_completion_item("results.json", "value", "export JSON", score=95),
        ]

    def helper_command_completion_items(self) -> list[CompletionItem]:
        items: list[CompletionItem] = []
        seen: set[str] = set()
        for spec in self._command_specs:
            for raw_name in (spec.name, *spec.aliases):
                key = raw_name.casefold()
                if key in seen:
                    continue
                seen.add(key)
                items.append(self._base_completion_item(raw_name, "helper_command", spec.description, score=180))
        return items

    def helper_argument_completion_items(
        self,
        command_name: str,
        args: str,
        trailing_space: bool,
    ) -> list[CompletionItem]:
        spec = self._lookup_command(command_name)
        if spec is None or spec.completer is None:
            return []
        return spec.completer(args, trailing_space)

    def _sql_keyword_items(self, keywords: list[str], score: int = 90) -> list[CompletionItem]:
        return [self._base_completion_item(keyword, "sql_keyword", "SQL keyword", score=score) for keyword in keywords]

    def _with_direction_completion_items(self) -> list[CompletionItem]:
        if self._direction_completion_cache is not None:
            return self._direction_completion_cache
        items = list(self._column_completion_items())
        for column in self.columns[: min(8, len(self.columns))]:
            expr = self._column_expr(column)
            items.append(self._base_completion_item(f"{expr} ASC", "snippet", "ascending sort", score=112))
            items.append(self._base_completion_item(f"{expr} DESC", "snippet", "descending sort", score=114))
        self._direction_completion_cache = items
        return items

    @staticmethod
    def _merge_completion_items(*groups: list[CompletionItem]) -> list[CompletionItem]:
        merged: list[CompletionItem] = []
        seen: set[str] = set()
        for items in groups:
            for item in items:
                key = item.insert_text.casefold()
                if key in seen:
                    continue
                seen.add(key)
                merged.append(item)
        return merged

    def _default_sql_completion_items(self) -> list[CompletionItem]:
        items: list[CompletionItem] = []
        for keyword in SQL_KEYWORDS:
            items.append(self._base_completion_item(keyword, "sql_keyword", "SQL keyword", score=90))
        items.append(self._base_completion_item(self.table_name, "table", "active table/view", score=95))
        items.extend(self._column_completion_items())
        items.extend(self._aggregate_completion_items())
        deduped: list[CompletionItem] = []
        seen: set[str] = set()
        for item in items:
            key = item.insert_text.casefold()
            if key in seen:
                continue
            seen.add(key)
            deduped.append(item)
        return deduped

    def sql_completion_items_for_clause(self, clause: SqlClause) -> list[CompletionItem]:
        clause_key = clause.casefold()
        cached = self._sql_clause_completion_cache.get(clause_key)
        if cached is not None:
            return cached
        table_items = [self._base_completion_item(self.table_name, "table", "active table/view", score=150)]
        columns = self._column_completion_items()
        predicates = self._predicate_completion_items()
        aggregate_items = self._aggregate_completion_items()
        numeric_values = self._numeric_completion_items([10, 25, 50, 100], detail="row count")

        if clause_key == "select":
            items = self._merge_completion_items(
                columns,
                aggregate_items,
                self._sql_keyword_items(["DISTINCT", "AS", "FROM", "CASE", "WHEN", "THEN", "ELSE", "END"], score=120),
            )
            self._sql_clause_completion_cache[clause_key] = items
            return items
        if clause_key in {"from", "join"}:
            items = self._merge_completion_items(
                table_items,
                self._sql_keyword_items(
                    ["JOIN", "LEFT JOIN", "RIGHT JOIN", "INNER JOIN", "WHERE", "GROUP BY", "ORDER BY", "LIMIT"],
                    score=118,
                ),
            )
            self._sql_clause_completion_cache[clause_key] = items
            return items
        if clause_key in {"where", "having", "join_on"}:
            items = self._merge_completion_items(
                predicates,
                self._sql_keyword_items(["AND", "OR", "NOT", "IN", "IS", "NULL", "LIKE", "BETWEEN"], score=116),
                aggregate_items if clause_key == "having" else [],
            )
            self._sql_clause_completion_cache[clause_key] = items
            return items
        if clause_key == "group_by":
            items = self._merge_completion_items(
                columns,
                self._sql_keyword_items(["HAVING", "ORDER BY", "LIMIT"], score=115),
            )
            self._sql_clause_completion_cache[clause_key] = items
            return items
        if clause_key == "order_by":
            items = self._merge_completion_items(
                self._with_direction_completion_items(),
                self._sql_keyword_items(["ASC", "DESC", "LIMIT"], score=114),
            )
            self._sql_clause_completion_cache[clause_key] = items
            return items
        if clause_key == "limit":
            items = self._merge_completion_items(
                numeric_values,
                self._sql_keyword_items(["OFFSET"], score=110),
            )
            self._sql_clause_completion_cache[clause_key] = items
            return items
        items = self._default_sql_completion_items()
        self._sql_clause_completion_cache[clause_key] = items
        return items

    def sql_completion_items(self) -> list[CompletionItem]:
        return self.sql_completion_items_for_clause("unknown")

    def sql_completion_items_for_function_args(self, function_name: str) -> list[CompletionItem]:
        return self._column_completion_items_for_aggregate(function_name)

    def completion_tokens(self) -> list[str]:
        raw_items = [*self.helper_command_completion_items(), *self.sql_completion_items()]
        seen: set[str] = set()
        tokens: list[str] = []
        for item in raw_items:
            key = item.insert_text.casefold()
            if key in seen:
                continue
            seen.add(key)
            tokens.append(item.insert_text)
        return tokens

    def completion_items(self, text: str, cursor_location: tuple[int, int]) -> list[CompletionItem]:
        return self._completion_engine.get_items(text, cursor_location)

    def record_completion_acceptance(self, token: str) -> None:
        self._completion_engine.record_acceptance(token)


class CompletionEngine:
    def __init__(self, engine: SqlExplorerEngine) -> None:
        self._engine = engine
        self._sqlglot_tokenizer = SqlglotTokenizer()
        self._completion_cache: dict[tuple[str, int, int, int], list[CompletionItem]] = {}
        self._completion_cache_limit = 192
        self._acceptance_counts: dict[str, int] = {}
        self._acceptance_revision = 0

    def clear_cache(self) -> None:
        self._completion_cache.clear()

    def record_acceptance(self, token: str) -> None:
        key = token.casefold()
        self._acceptance_counts[key] = self._acceptance_counts.get(key, 0) + 1
        self._acceptance_revision += 1
        self.clear_cache()

    def _line_before_cursor(self, text: str, cursor_location: tuple[int, int]) -> tuple[str, int, int]:
        lines = text.split("\n")
        if not lines:
            lines = [""]
        row = min(max(cursor_location[0], 0), len(lines) - 1)
        line = lines[row]
        col = min(max(cursor_location[1], 0), len(line))
        return line[:col], row, col

    def _helper_context(self, text: str, row: int, col: int, before: str) -> CompletionContext:
        stripped = before.lstrip()
        parts = stripped.split(maxsplit=1)
        command_token = parts[0] if parts else "/"
        trailing_space = stripped.endswith(" ")
        args = parts[1] if len(parts) > 1 else ""

        if len(parts) == 1 and not trailing_space:
            prefix = command_token
            replacement_start = len(before) - len(prefix)
            return CompletionContext(
                text=text,
                cursor_row=row,
                cursor_col=col,
                line_before_cursor=before,
                mode="helper",
                prefix=prefix,
                replacement_start=replacement_start,
                replacement_end=len(before),
                helper_command=command_token,
                helper_args="",
                helper_has_trailing_space=False,
                completing_command_name=True,
            )

        prefix = ""
        replacement_start = len(before)
        if args and not trailing_space:
            match = re.search(r'("(?:""|[^"])*)$|([^\s|]+)$', args)
            if match is not None:
                prefix = match.group(1) or match.group(2) or ""
                replacement_start = len(before) - len(prefix)

        return CompletionContext(
            text=text,
            cursor_row=row,
            cursor_col=col,
            line_before_cursor=before,
            mode="helper",
            prefix=prefix,
            replacement_start=replacement_start,
            replacement_end=len(before),
            helper_command=command_token,
            helper_args=args,
            helper_has_trailing_space=trailing_space,
            completing_command_name=False,
        )

    @staticmethod
    def _is_inside_single_quoted_literal(before: str) -> bool:
        quote_count = 0
        idx = 0
        while idx < len(before):
            char = before[idx]
            if char != "'":
                idx += 1
                continue
            if idx + 1 < len(before) and before[idx + 1] == "'":
                idx += 2
                continue
            quote_count += 1
            idx += 1
        return quote_count % 2 == 1

    @staticmethod
    def _has_unclosed_double_quote(before: str) -> bool:
        quote_count = 0
        idx = 0
        while idx < len(before):
            char = before[idx]
            if char != '"':
                idx += 1
                continue
            if idx + 1 < len(before) and before[idx + 1] == '"':
                idx += 2
                continue
            quote_count += 1
            idx += 1
        return quote_count % 2 == 1

    @staticmethod
    def _detect_sql_clause_from_words(words: list[str]) -> SqlClause:
        clause: SqlClause = "unknown"
        for word in words:
            token = " ".join(word.split())
            if token == "SELECT":
                clause = "select"
            elif token == "FROM":
                clause = "from"
            elif token in {"WHERE", "HAVING", "LIMIT"}:
                clause = cast(SqlClause, token.lower())
            elif token == "GROUP BY":
                clause = "group_by"
            elif token == "ORDER BY":
                clause = "order_by"
            elif token == "ON":
                clause = "join_on"
            elif token in {"JOIN", "LEFT JOIN", "RIGHT JOIN", "INNER JOIN", "FULL JOIN", "CROSS JOIN"}:
                clause = "join"
        return clause

    def _detect_sql_clause(self, before: str) -> SqlClause:
        normalized = before
        if self._has_unclosed_double_quote(before):
            normalized += '"'
        try:
            tokens = self._sqlglot_tokenizer.tokenize(normalized)
        except Exception:
            return "unknown"
        words = [token.text.upper() for token in tokens if token.text and token.text[0].isalpha()]
        if not words:
            return "unknown"
        return self._detect_sql_clause_from_words(words)

    @staticmethod
    def _nearest_unmatched_open_paren(before: str) -> int | None:
        open_parens: list[int] = []
        idx = 0
        in_single_quote = False
        in_double_quote = False
        in_line_comment = False
        block_comment_depth = 0

        while idx < len(before):
            char = before[idx]
            next_char = before[idx + 1] if idx + 1 < len(before) else ""

            if in_line_comment:
                if char == "\n":
                    in_line_comment = False
                idx += 1
                continue

            if block_comment_depth > 0:
                if char == "/" and next_char == "*":
                    block_comment_depth += 1
                    idx += 2
                    continue
                if char == "*" and next_char == "/":
                    block_comment_depth -= 1
                    idx += 2
                    continue
                idx += 1
                continue

            if in_single_quote:
                if char == "'" and next_char == "'":
                    idx += 2
                    continue
                if char == "'":
                    in_single_quote = False
                idx += 1
                continue

            if in_double_quote:
                if char == '"' and next_char == '"':
                    idx += 2
                    continue
                if char == '"':
                    in_double_quote = False
                idx += 1
                continue

            if char == "-" and next_char == "-":
                in_line_comment = True
                idx += 2
                continue
            if char == "/" and next_char == "*":
                block_comment_depth = 1
                idx += 2
                continue
            if char == "'":
                in_single_quote = True
                idx += 1
                continue
            if char == '"':
                in_double_quote = True
                idx += 1
                continue
            if char == "(":
                open_parens.append(idx)
            elif char == ")" and open_parens:
                open_parens.pop()
            idx += 1

        if not open_parens:
            return None
        return open_parens[-1]

    @staticmethod
    def _aggregate_function_before_paren(before: str, open_paren_idx: int) -> str | None:
        token_source = before[:open_paren_idx].rstrip()
        if not token_source:
            return None
        match = _IDENT_PREFIX_RE.search(token_source)
        if match is None:
            return None
        function_name = match.group(1).upper()
        if function_name not in _AGGREGATE_FUNCTIONS:
            return None
        return function_name

    def _function_argument_context(self, before: str) -> tuple[bool, str | None]:
        open_paren_idx = self._nearest_unmatched_open_paren(before)
        if open_paren_idx is None:
            return False, None
        function_name = self._aggregate_function_before_paren(before, open_paren_idx)
        if function_name is None:
            return False, None
        return True, function_name

    def _sql_context(self, text: str, row: int, col: int, before: str) -> CompletionContext | None:
        if self._is_inside_single_quoted_literal(before):
            return None
        inside_function_args, sql_function = self._function_argument_context(before)
        prefix_match = _QUOTED_PREFIX_RE.search(before) or _IDENT_PREFIX_RE.search(before)
        if prefix_match is None:
            prefix = ""
            replacement_start = len(before)
        else:
            prefix = prefix_match.group(1)
            replacement_start = len(before) - len(prefix)
        return CompletionContext(
            text=text,
            cursor_row=row,
            cursor_col=col,
            line_before_cursor=before,
            mode="sql",
            prefix=prefix,
            replacement_start=replacement_start,
            replacement_end=len(before),
            sql_clause=self._detect_sql_clause(before),
            sql_function=sql_function,
            inside_function_args=inside_function_args,
        )

    def _build_context(self, text: str, cursor_location: tuple[int, int]) -> CompletionContext | None:
        before, row, col = self._line_before_cursor(text, cursor_location)
        if before.lstrip().startswith("/"):
            return self._helper_context(text, row, col, before)
        return self._sql_context(text, row, col, before)

    def _match_candidates(
        self,
        candidates: list[CompletionItem],
        prefix: str,
        replacement_start: int,
        replacement_end: int,
    ) -> list[CompletionItem]:
        prefix_lower = prefix.casefold()
        ranked_matches: list[tuple[int, CompletionItem]] = []
        seen: set[str] = set()
        for candidate in candidates:
            token = candidate.insert_text
            if prefix and not token.casefold().startswith(prefix_lower):
                continue
            if token.casefold() == prefix_lower:
                continue
            key = token.casefold()
            if key in seen:
                continue
            seen.add(key)
            insert_text = token
            if token.isupper() and prefix.islower():
                insert_text = token.lower()
            elif token.islower() and prefix.isupper():
                insert_text = token.upper()
            dynamic_score = candidate.score
            dynamic_score += min(self._acceptance_counts.get(key, 0) * 6, 36)
            if prefix:
                if token.startswith(prefix):
                    dynamic_score += 26
                else:
                    dynamic_score += 18
                dynamic_score -= min(max(len(token) - len(prefix), 0), 18)
                if prefix.startswith('"') and token.startswith('"'):
                    dynamic_score += 12
            built = CompletionItem(
                label=insert_text,
                insert_text=insert_text,
                kind=candidate.kind,
                detail=candidate.detail,
                replacement_start=replacement_start,
                replacement_end=replacement_end,
                score=dynamic_score,
            )
            ranked_matches.append((dynamic_score, built))
        ranked_matches.sort(
            key=lambda pair: (
                -pair[0],
                len(pair[1].insert_text),
                pair[1].insert_text.casefold(),
            )
        )
        return [pair[1] for pair in ranked_matches[:64]]

    def get_items(self, text: str, cursor_location: tuple[int, int]) -> list[CompletionItem]:
        cache_key = (text, cursor_location[0], cursor_location[1], self._acceptance_revision)
        cached = self._completion_cache.get(cache_key)
        if cached is not None:
            return cached

        context = self._build_context(text, cursor_location)
        if context is None:
            return []

        matched: list[CompletionItem]
        if context.mode == "helper":
            if context.completing_command_name:
                candidates = self._engine.helper_command_completion_items()
            else:
                if context.helper_command is None:
                    return []
                candidates = self._engine.helper_argument_completion_items(
                    context.helper_command,
                    context.helper_args,
                    context.helper_has_trailing_space,
                )
                if not candidates:
                    candidates = self._engine.helper_command_completion_items()
            matched = self._match_candidates(
                candidates,
                context.prefix,
                context.replacement_start,
                context.replacement_end,
            )
        else:
            sql_candidates = self._engine.sql_completion_items_for_clause(context.sql_clause)
            if context.inside_function_args and context.sql_function is not None:
                function_candidates = self._engine.sql_completion_items_for_function_args(context.sql_function)
                if function_candidates:
                    sql_candidates = function_candidates
            matched = self._match_candidates(
                sql_candidates,
                context.prefix,
                context.replacement_start,
                context.replacement_end,
            )

        self._completion_cache[cache_key] = matched
        if len(self._completion_cache) > self._completion_cache_limit:
            oldest_key = next(iter(self._completion_cache))
            self._completion_cache.pop(oldest_key)
        return matched


class SqlQueryEditor(TextArea):
    BINDINGS = [
        Binding("ctrl+enter", "app.run_query", "Run", priority=True),
        Binding("f5", "app.run_query", "Run", show=False, priority=True),
        Binding("ctrl+n", "app.load_sample", "Sample", priority=True),
        Binding("f6", "app.load_sample", show=False, priority=True),
        Binding("ctrl+l", "app.clear_editor", "Clear", priority=True),
        Binding("f7", "app.clear_editor", show=False, priority=True),
        Binding("ctrl+1", "app.focus_editor", "Editor", priority=True),
        Binding("ctrl+2", "app.focus_results", "Results", priority=True),
        Binding("ctrl+b", "app.toggle_sidebar", "Data Explorer", key_display="^b", priority=True),
        Binding("f1", "app.show_help", "Help", priority=True),
        Binding("ctrl+shift+p", "app.show_help", show=False, priority=True),
        Binding("f10", "app.quit", "Quit", priority=True),
        Binding("ctrl+q", "app.quit", show=False, priority=True),
    ]

    def __init__(
        self,
        text: str,
        token_provider: Callable[[], list[str]],
        history_prev: Callable[[], str | None],
        history_next: Callable[[], str | None],
        completion_provider: Callable[[str, tuple[int, int]], list[CompletionItem]] | None = None,
        helper_command_provider: Callable[[], list[str]] | None = None,
        completion_changed: Callable[[list[CompletionItem], int, bool], None] | None = None,
        completion_accepted: Callable[[CompletionItem], None] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(text, language="sql", theme="monokai", tab_behavior="indent", soft_wrap=False, **kwargs)
        self._token_provider = token_provider
        self._history_prev = history_prev
        self._history_next = history_next
        self._completion_provider = completion_provider
        self._helper_command_provider = helper_command_provider or (lambda: list(DEFAULT_HELPER_COMMANDS))
        self._completion_changed = completion_changed
        self._completion_accepted = completion_accepted
        self._completion_items: list[CompletionItem] = []
        self._completion_index = 0
        self._completion_open = False
        self._suspend_completion_refresh = False
        self._last_completion_signature: tuple[str, tuple[int, int], bool] | None = None
        self._sql_syntax = Syntax(
            "",
            "sql",
            theme="monokai",
            word_wrap=False,
            line_numbers=False,
            indent_guides=False,
            background_color="default",
        )
        self.indent_width = 4

    def dismiss_completion_menu(self) -> None:
        self._completion_items = []
        self._completion_index = 0
        self._completion_open = False
        self.suggestion = ""
        self._notify_completion_change()

    def _notify_completion_change(self) -> None:
        if self._completion_changed is None:
            return
        items = self._completion_items if self._completion_open else []
        index = self._completion_index if items else 0
        self._completion_changed(items, index, self._completion_open)

    def _refresh_completion_state(self, *, force_open: bool = False) -> None:
        if self._completion_provider is None:
            self.dismiss_completion_menu()
            return
        signature = (self.text, self.cursor_location, self.has_focus)
        if not force_open and signature == self._last_completion_signature:
            return
        self._last_completion_signature = signature
        completions = self._completion_provider(self.text, self.cursor_location)
        if not completions:
            self.dismiss_completion_menu()
            return
        row, col = self.cursor_location
        line_before = self.document[row][:col]
        should_open = (bool(line_before.strip()) or force_open) and self.has_focus
        self._completion_items = completions
        self._completion_index = max(0, min(self._completion_index, len(self._completion_items) - 1))
        self._completion_open = should_open
        self._notify_completion_change()

    def _apply_inline_suggestion_from_selected_completion(self) -> None:
        if not self._completion_items:
            self.suggestion = ""
            return
        row, col = self.cursor_location
        item = self._completion_items[self._completion_index]
        if item.replacement_end != col:
            self.suggestion = ""
            return
        replacement_start = max(0, min(item.replacement_start, col))
        current_text = self.document[row][replacement_start:col]
        if not current_text:
            self.suggestion = ""
            return
        if not item.insert_text.casefold().startswith(current_text.casefold()):
            self.suggestion = ""
            return
        self.suggestion = item.insert_text[len(current_text) :]

    def _move_completion_selection(self, delta: int) -> None:
        if not self._completion_open or not self._completion_items:
            return
        self._completion_index = (self._completion_index + delta) % len(self._completion_items)
        self._notify_completion_change()
        self._apply_inline_suggestion_from_selected_completion()

    def accept_completion_at_index(self, index: int) -> bool:
        if not self._completion_items:
            return False
        target_index = max(0, min(index, len(self._completion_items) - 1))
        self._completion_index = target_index
        item = self._completion_items[self._completion_index]
        row, col = self.cursor_location
        if item.replacement_end != col:
            return False
        start = (row, item.replacement_start)
        end = (row, item.replacement_end)
        self._suspend_completion_refresh = True
        try:
            result = self.replace(item.insert_text, start, end, maintain_selection_offset=False)
            self.move_cursor(result.end_location)
        finally:
            self._suspend_completion_refresh = False
        self.dismiss_completion_menu()
        if self._completion_accepted is not None:
            self._completion_accepted(item)
        return True

    def set_completion_index(self, index: int) -> None:
        if not self._completion_items:
            return
        self._completion_index = max(0, min(index, len(self._completion_items) - 1))
        self._notify_completion_change()
        self._apply_inline_suggestion_from_selected_completion()

    def _accept_selected_completion(self) -> bool:
        if not self._completion_open or not self._completion_items:
            return False
        return self.accept_completion_at_index(self._completion_index)

    async def _on_key(self, event: Any) -> None:
        if event.key == "ctrl+space":
            event.stop()
            event.prevent_default()
            self._refresh_completion_state(force_open=True)
            self._apply_inline_suggestion_from_selected_completion()
            return
        if event.key == "escape" and self._completion_open:
            event.stop()
            event.prevent_default()
            self.dismiss_completion_menu()
            return
        if event.key == "up" and self._completion_open:
            event.stop()
            event.prevent_default()
            self._move_completion_selection(-1)
            return
        if event.key == "down" and self._completion_open:
            event.stop()
            event.prevent_default()
            self._move_completion_selection(1)
            return
        if event.key in {"tab", "enter"} and self._accept_selected_completion():
            event.stop()
            event.prevent_default()
            return
        if event.key == "tab":
            event.stop()
            event.prevent_default()
            if self.suggestion:
                self.insert(self.suggestion)
            else:
                self.insert(" " * self._find_columns_to_next_tab_stop())
            return
        await super()._on_key(event)

    def action_cursor_up(self, select: bool = False) -> None:
        if select:
            super().action_cursor_up(select)
            return
        row, _ = self.cursor_location
        if row == 0:
            prior = self._history_prev()
            if prior is not None:
                self.load_text(prior)
                self.move_cursor((self.document.line_count - 1, len(self.document[-1])))
                return
        super().action_cursor_up(select)

    def action_cursor_down(self, select: bool = False) -> None:
        if select:
            super().action_cursor_down(select)
            return
        row, _ = self.cursor_location
        if row == self.document.line_count - 1:
            nxt = self._history_next()
            if nxt is not None:
                self.load_text(nxt)
                self.move_cursor((self.document.line_count - 1, len(self.document[-1])))
                return
        super().action_cursor_down(select)

    def update_suggestion(self) -> None:
        if self._suspend_completion_refresh:
            self.suggestion = ""
            return
        if self._completion_provider is not None:
            self._refresh_completion_state()
            self._apply_inline_suggestion_from_selected_completion()
            return

        row, col = self.cursor_location
        left_text = self.document[row][:col]
        prefix_match = (
            _HELPER_PREFIX_RE.search(left_text)
            or _QUOTED_PREFIX_RE.search(left_text)
            or _IDENT_PREFIX_RE.search(left_text)
        )
        if prefix_match is None:
            self.suggestion = ""
            return
        prefix = prefix_match.group(1)
        prefix_lower = prefix.lower()
        for token in self._token_provider():
            if not token.lower().startswith(prefix_lower):
                continue
            candidate = token
            if token.isupper() and prefix.islower():
                candidate = token.lower()
            elif token.islower() and prefix.isupper():
                candidate = token.upper()
            if candidate.lower() == prefix_lower:
                continue
            self.suggestion = candidate[len(prefix) :]
            return
        self.suggestion = ""

    def on_focus(self, _event: Any) -> None:
        if self._completion_provider is None:
            return
        self._refresh_completion_state()
        self._apply_inline_suggestion_from_selected_completion()

    def on_blur(self, _event: Any) -> None:
        self._last_completion_signature = None
        self.dismiss_completion_menu()

    def get_line(self, line_index: int) -> Text:
        line_string = self.document.get_line(line_index)
        if not line_string:
            return Text("", end="", no_wrap=True)
        if line_string.lstrip().startswith("/"):
            return self._highlight_helper_command_line(line_string)

        highlighted = self._sql_syntax.highlight(line_string)
        # Rich appends a trailing newline to highlighted output; remove only that
        # so user-typed trailing spaces remain intact for correct cursor rendering.
        if highlighted.plain.endswith("\n"):
            highlighted = highlighted[:-1]
        highlighted.end = ""
        highlighted.no_wrap = True
        return highlighted

    def _highlight_helper_command_line(self, line: str) -> Text:
        rendered = Text(end="", no_wrap=True)
        indent_count = len(line) - len(line.lstrip())
        if indent_count:
            rendered.append(line[:indent_count])

        command_line = line[indent_count:]
        if not command_line.startswith("/"):
            rendered.append(command_line, style="bright_white")
            return rendered

        parts = command_line.split(maxsplit=1)
        command = parts[0]
        args = parts[1] if len(parts) > 1 else ""
        helper_commands = {item.casefold() for item in self._helper_command_provider()}
        command_style = "bold cyan" if command.casefold() in helper_commands else "bold red"
        rendered.append(command, style=command_style)
        if args:
            rendered.append(" ")
            rendered.append(args, style="bright_white")
        return rendered


class SqlExplorerTui(App[None]):
    TITLE = "my-project SQL Explorer"
    SUB_TITLE = "DuckDB + Textual"

    CSS = """
    Screen {
        layout: vertical;
    }

    #body {
        height: 1fr;
    }

    #sidebar {
        width: 38;
        border: round #4f9da6;
        padding: 1;
        background: #102025;
        color: #f0f4f8;
    }

    #workspace {
        width: 1fr;
        padding: 0 1;
    }

    .section-title {
        color: #9abed8;
        text-style: bold;
        margin-top: 1;
    }

    #query_editor {
        height: 10;
        border: round #4d7ea8;
    }

    #completion_menu {
        display: none;
        max-height: 8;
        border: round #4d7ea8;
        margin-bottom: 1;
        background: #0f1b22;
    }

    #results_table {
        height: 1fr;
        border: round #5b8f67;
    }

    #activity_log {
        height: 11;
        border: round #b18b3d;
    }
    """

    BINDINGS = [
        Binding("ctrl+enter", "run_query", "Run", priority=True),
        Binding("f5", "run_query", "Run", show=False, priority=True),
        Binding("ctrl+n", "load_sample", "Sample", priority=True),
        Binding("f6", "load_sample", "Sample", show=False, priority=True),
        Binding("ctrl+l", "clear_editor", "Clear", priority=True),
        Binding("f7", "clear_editor", "Clear", show=False, priority=True),
        Binding("ctrl+1", "focus_editor", "Editor", priority=True),
        Binding("ctrl+2", "focus_results", "Results", priority=True),
        Binding("ctrl+b", "toggle_sidebar", "Data Explorer", key_display="^b", priority=True),
        Binding("f1", "show_help", "Help", priority=True),
        Binding("ctrl+shift+p", "show_help", "Help", show=False, priority=True),
        Binding("f10", "quit", "Quit", priority=True),
        Binding("ctrl+q", "quit", "Quit", show=False, priority=True),
    ]

    def __init__(self, engine: SqlExplorerEngine) -> None:
        super().__init__()
        self.engine = engine
        self._history_cursor: int | None = None
        self._active_result: QueryResult | None = None
        self._base_rows: list[tuple[Any, ...]] = []
        self._sort_column_index: int | None = None
        self._sort_reverse = False

    def _reset_history_cursor(self) -> None:
        self._history_cursor = None

    def _history_prev(self) -> str | None:
        history = self.engine.executed_sql
        if not history:
            return None
        if self._history_cursor is None:
            self._history_cursor = len(history) - 1
        else:
            self._history_cursor = (self._history_cursor - 1) % len(history)
        return history[self._history_cursor]

    def _history_next(self) -> str | None:
        history = self.engine.executed_sql
        if not history:
            return None
        if self._history_cursor is None:
            self._history_cursor = 0
        else:
            self._history_cursor = (self._history_cursor + 1) % len(history)
        return history[self._history_cursor]

    def _results_table(self) -> DataTable[str]:
        return cast(DataTable[str], self.query_one("#results_table", DataTable))

    def _query_editor(self) -> SqlQueryEditor:
        return self.query_one("#query_editor", SqlQueryEditor)

    def _completion_menu(self) -> OptionList:
        return self.query_one("#completion_menu", OptionList)

    @staticmethod
    def _completion_option_prompt(item: CompletionItem) -> str:
        kind = item.kind.replace("_", " ")
        if item.detail:
            return f"{item.insert_text}  [{kind}] {item.detail}"
        return f"{item.insert_text}  [{kind}]"

    def _on_editor_completion_changed(
        self,
        items: list[CompletionItem],
        selected_index: int,
        is_open: bool,
    ) -> None:
        if not self.screen.is_mounted:
            return
        menu = self._completion_menu()
        if not is_open or not items:
            menu.display = False
            menu.clear_options()
            return
        visible_items = items[:8]
        prompts = [self._completion_option_prompt(item) for item in visible_items]
        menu.set_options(prompts)
        menu.highlighted = min(max(0, selected_index), len(visible_items) - 1)
        menu.display = True

    def _on_editor_completion_accepted(self, item: CompletionItem) -> None:
        self.engine.record_completion_acceptance(item.insert_text)
        self._on_editor_completion_changed([], 0, False)

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Horizontal(id="body"):
            with Vertical(id="sidebar"):
                yield Static(self.engine.schema_preview(), id="sidebar_text")
            with Vertical(id="workspace"):
                yield Static("Query", classes="section-title")
                yield SqlQueryEditor(
                    self.engine.default_query,
                    self.engine.completion_tokens,
                    self._history_prev,
                    self._history_next,
                    completion_provider=self.engine.completion_items,
                    helper_command_provider=self.engine.helper_commands,
                    completion_changed=self._on_editor_completion_changed,
                    completion_accepted=self._on_editor_completion_accepted,
                    id="query_editor",
                )
                yield OptionList(id="completion_menu")
                yield Static("Results", id="results_header", classes="section-title")
                yield DataTable(id="results_table")
                yield Static("Activity", classes="section-title")
                yield RichLog(id="activity_log", markup=True, highlight=True, wrap=True)
        yield Footer()

    def on_mount(self) -> None:
        table = self._results_table()
        table.zebra_stripes = True
        self._completion_menu().display = False
        self._log("Ready. Press Ctrl+Enter/F5 to run SQL. F1 opens help, F10 quits.", "info")
        boot = self.engine.run_sql(self.engine.default_query, remember=False)
        self._apply_response(boot)
        self._query_editor().focus()

    def action_run_query(self) -> None:
        editor = self._query_editor()
        editor.dismiss_completion_menu()
        query = editor.text
        response = self.engine.run_input(query)
        self._reset_history_cursor()
        self._apply_response(response)

    def action_load_sample(self) -> None:
        editor = self._query_editor()
        editor.dismiss_completion_menu()
        editor.text = self.engine.default_query
        self._reset_history_cursor()
        editor.focus()
        self._log("Loaded sample query.", "info")

    def action_clear_editor(self) -> None:
        editor = self._query_editor()
        editor.dismiss_completion_menu()
        editor.text = ""
        self._reset_history_cursor()
        editor.focus()

    def action_focus_editor(self) -> None:
        self._query_editor().focus()

    def action_focus_results(self) -> None:
        self._results_table().focus()

    def action_toggle_sidebar(self) -> None:
        sidebar = self.query_one("#sidebar", Vertical)
        sidebar.display = not sidebar.display
        state = "shown" if sidebar.display else "hidden"
        self._log(f"Data Explorer {state}.", "info")

    def action_show_help(self) -> None:
        self._log(self.engine.help_text(), "info")

    def _apply_response(self, response: EngineResponse) -> None:
        if response.generated_sql:
            self._log(f"Generated SQL:\n{response.generated_sql}", "info")

        if response.result is not None:
            self._render_table(response.result)

        if response.message:
            self._log(response.message, response.status)

        if response.load_query is not None:
            editor = self._query_editor()
            editor.text = response.load_query
            self._reset_history_cursor()
            editor.focus()

        if response.clear_editor:
            self.action_clear_editor()

        if response.should_exit:
            self.exit()

        sidebar = self.query_one("#sidebar_text", Static)
        sidebar.update(self.engine.schema_preview())

    def _render_table(self, result: QueryResult) -> None:
        self._active_result = result
        self._base_rows = list(result.rows)
        self._sort_column_index = None
        self._sort_reverse = False

        self._redraw_results_table()

    def _redraw_results_table(self) -> None:
        result = self._active_result
        table = self._results_table()
        table.clear(columns=True)
        if result is None or not result.columns:
            self.query_one("#results_header", Static).update("Results")
            return

        table.add_columns(*result.columns)
        for row in result.rows:
            table.add_row(*[_format_scalar(value, self.engine.max_value_chars) for value in row])

        header = f"Results ({len(result.rows):,}/{result.total_rows:,} rows, {result.elapsed_ms:.1f} ms)"
        if result.truncated:
            header += " [truncated]"
        if self._sort_column_index is not None and self._sort_column_index < len(result.columns):
            direction = "desc" if self._sort_reverse else "asc"
            header += f" [sorted: {result.columns[self._sort_column_index]} {direction}]"
        self.query_one("#results_header", Static).update(header)

    def on_data_table_header_selected(self, event: DataTable.HeaderSelected) -> None:
        result = self._active_result
        if result is None or not result.rows:
            return

        if self._sort_column_index == event.column_index:
            self._sort_reverse = not self._sort_reverse
        else:
            self._sort_column_index = event.column_index
            self._sort_reverse = False

        column_index = event.column_index
        result.rows = sorted(
            self._base_rows,
            key=lambda row: _sort_cell_key(row[column_index]) if column_index < len(row) else (2, 0, ""),
            reverse=self._sort_reverse,
        )
        self._redraw_results_table()
        direction = "DESC" if self._sort_reverse else "ASC"
        self._log(f"Sorted results by {event.label.plain} {direction}", "info")

    def _log(self, message: str, status: ResultStatus) -> None:
        logger = self.query_one("#activity_log", RichLog)
        style_prefix = {
            "ok": "[green]",
            "info": "[cyan]",
            "error": "[red]",
        }[status]
        logger.write(f"{style_prefix}{message}[/]")


def _render_console_response(console: Console, response: EngineResponse, max_value_chars: int) -> int:
    if response.generated_sql:
        console.print(Panel(response.generated_sql, title="Generated SQL", border_style="magenta"))

    if response.result is not None:
        result = response.result
        table = Table(
            title=f"Result ({len(result.rows)}/{result.total_rows} rows, {result.elapsed_ms:.1f} ms)",
            box=box.SIMPLE,
        )
        for column in result.columns:
            table.add_column(column)
        for row in result.rows:
            table.add_row(*[_format_scalar(value, max_value_chars) for value in row])
        console.print(table)

    if response.message:
        border_style = "green"
        if response.status == "error":
            border_style = "red"
        elif response.status == "info":
            border_style = "cyan"
        console.print(Panel(response.message, border_style=border_style))

    return 1 if response.status == "error" else 0


@app.command()
def main(
    data: Path = typer.Argument(
        ...,
        exists=True,
        readable=True,
        resolve_path=True,
        help="CSV, TSV, or Parquet file path.",
    ),
    table_name: str = typer.Option("data", "--table", "-t", help="Logical table/view name inside DuckDB."),
    limit: int = typer.Option(100, "--limit", "-l", min=1, help="Default helper query row limit."),
    max_rows: int = typer.Option(1000, "--max-rows", min=1, help="Maximum rows displayed in the result grid."),
    max_value_chars: int = typer.Option(
        160,
        "--max-value-chars",
        min=8,
        help="Maximum characters displayed per cell.",
    ),
    database: str = typer.Option(
        ":memory:",
        "--db",
        help="DuckDB database path. Use :memory: for in-memory session.",
    ),
    execute: str | None = typer.Option(None, "--execute", "-e", help="Run SQL non-interactively and exit."),
    query_file: Path | None = typer.Option(
        None,
        "--file",
        "-f",
        exists=True,
        readable=True,
        resolve_path=True,
        help="Run SQL from file non-interactively and exit.",
    ),
    no_ui: bool = typer.Option(False, "--no-ui", help="Run once in standard terminal output mode."),
) -> None:
    if execute and query_file:
        raise typer.BadParameter("Use either --execute or --file, not both.")

    file_path = data.expanduser().resolve()
    engine = SqlExplorerEngine(
        data_path=file_path,
        table_name=table_name,
        database=database,
        default_limit=limit,
        max_rows_display=max_rows,
        max_value_chars=max_value_chars,
    )

    try:
        non_interactive_sql = execute
        if query_file is not None:
            non_interactive_sql = query_file.read_text(encoding="utf-8").strip()

        if non_interactive_sql is not None:
            response = engine.run_input(non_interactive_sql)
            exit_code = _render_console_response(Console(), response, engine.max_value_chars)
            raise typer.Exit(code=exit_code)

        if no_ui:
            response = engine.run_sql(engine.default_query, remember=False)
            exit_code = _render_console_response(Console(), response, engine.max_value_chars)
            raise typer.Exit(code=exit_code)

        SqlExplorerTui(engine).run()
    finally:
        engine.close()


if __name__ == "__main__":
    app()
