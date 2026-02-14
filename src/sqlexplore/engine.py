import json
import math
import re
import time
import tomllib
from dataclasses import dataclass
from functools import cache
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as importlib_version
from pathlib import Path
from typing import Any, Callable, Literal, cast

import duckdb
import typer

from sqlexplore.completion.completion_types import CompletionItem, CompletionResult, SqlClause
from sqlexplore.completion.completion_utils import (
    is_numeric_type,
    is_simple_ident,
    parse_optional_positive_int,
    parse_single_positive_int_arg,
    quote_ident,
)
from sqlexplore.completion.completions import CompletionEngine, EngineCompletionCatalog
from sqlexplore.sql_templates import (
    DEFAULT_LOAD_QUERY_TEMPLATE,
    TXT_LOAD_QUERY_TEMPLATE,
    render_load_query,
)

ResultStatus = Literal["ok", "info", "sql", "error"]
STATUS_STYLE_BY_RESULT: dict[ResultStatus, str] = {
    "ok": "green",
    "info": "cyan",
    "sql": "cyan",
    "error": "red",
}
QUALIFIED_FUNCTION_LABEL_RE = re.compile(
    r'^(?P<prefix>(?:"(?:""|[^"]+)"|[A-Za-z_][A-Za-z0-9_$]*)\.)+(?P<func>"(?:""|[^"]+)"|[A-Za-z_][A-Za-z0-9_$]*)\('
)
QUOTED_FUNCTION_LABEL_RE = re.compile(r'^(?P<func>"(?:""|[^"]+)")\(')
UNKNOWN_VERSION = "0.0.0+unknown"


@cache
def app_version() -> str:
    try:
        return importlib_version("sqlexplore")
    except PackageNotFoundError:
        pyproject_version = _version_from_pyproject()
        return pyproject_version or UNKNOWN_VERSION


def _version_from_pyproject() -> str | None:
    for parent in Path(__file__).resolve().parents:
        pyproject_path = parent / "pyproject.toml"
        if not pyproject_path.is_file():
            continue
        try:
            with pyproject_path.open("rb") as pyproject_file:
                pyproject = cast(dict[str, object], tomllib.load(pyproject_file))
        except (OSError, tomllib.TOMLDecodeError):
            continue
        project = pyproject.get("project")
        if not isinstance(project, dict):
            continue
        project_map = cast(dict[str, object], project)
        version_value = project_map.get("version")
        if isinstance(version_value, str) and version_value.strip():
            return version_value.strip()
    return None


@dataclass(slots=True)
class QueryResult:
    sql: str
    columns: list[str]
    column_types: list[str]
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
    executed_sql: str | None = None
    should_exit: bool = False
    load_query: str | None = None
    clear_editor: bool = False

    def activity_sql_log(self) -> tuple[str, str] | None:
        if self.generated_sql is not None:
            return "Generated", self.generated_sql
        if self.executed_sql is not None:
            return "Executed", self.executed_sql
        return None


@dataclass(slots=True)
class CommandSpec:
    name: str
    usage: str
    description: str
    handler: Callable[[str], EngineResponse]
    completer: Callable[[str, bool], list[CompletionItem]] | None = None
    aliases: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class SqlHelperCommandSpec:
    name: str
    usage: str
    description: str
    sql_builder: Callable[[str], str | None]
    completer: Callable[[str, bool], list[CompletionItem]]


@dataclass(frozen=True, slots=True)
class FileReader:
    function_name: str
    args: str
    query_template: str = DEFAULT_LOAD_QUERY_TEMPLATE


@dataclass(frozen=True, slots=True)
class StructField:
    name: str
    type_name: str
    children: tuple["StructField", ...] = ()


StructPath = tuple[str, str]


def _detect_reader(file_path: Path) -> FileReader:
    suffix = file_path.suffix.lower()
    if suffix == ".csv":
        return FileReader(function_name="read_csv_auto", args="")
    if suffix == ".tsv":
        return FileReader(function_name="read_csv_auto", args=", delim='\\t'")
    if suffix in {".parquet", ".pq"}:
        return FileReader(function_name="read_parquet", args="")
    if suffix == ".txt":
        return FileReader(
            function_name="read_csv",
            args=", auto_detect=false, header=false, delim='\\0', columns={'line':'VARCHAR'}, force_not_null=['line']",
            query_template=TXT_LOAD_QUERY_TEMPLATE,
        )
    raise typer.BadParameter("Only .csv, .tsv, .txt, and .parquet/.pq files are supported.")


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _normalize_function_ident(token: str) -> str:
    if not (token.startswith('"') and token.endswith('"') and len(token) > 1):
        return token
    unquoted = token[1:-1].replace('""', '"')
    return unquoted if is_simple_ident(unquoted) else token


def _normalize_result_column_label(name: str) -> str:
    qualified_match = QUALIFIED_FUNCTION_LABEL_RE.match(name)
    if qualified_match is not None:
        func_token = _normalize_function_ident(qualified_match.group("func"))
        return f"{func_token}({name[qualified_match.end() :]}"

    quoted_match = QUOTED_FUNCTION_LABEL_RE.match(name)
    if quoted_match is not None:
        func_token = _normalize_function_ident(quoted_match.group("func"))
        return f"{func_token}({name[quoted_match.end() :]}"

    return name


def result_columns(description: list[tuple[Any, ...]] | None) -> list[str]:
    if not description:
        return []
    return [_normalize_result_column_label(str(item[0])) for item in description]


def _result_column_types(description: list[tuple[Any, ...]] | None) -> list[str]:
    if not description:
        return []
    return [str(item[1]) for item in description]


def _split_pipe_sections(raw: str) -> list[str]:
    return [part.strip() for part in raw.split("|") if part.strip()]


def _split_optional_where(raw: str) -> tuple[str, str] | None:
    payload = raw.strip()
    if "|" not in payload:
        return payload, ""
    before, after = payload.split("|", maxsplit=1)
    where_clause = after.strip()
    if not where_clause:
        return None
    return before.strip(), where_clause


def format_scalar(value: Any, max_chars: int | None = None) -> str:
    if value is None:
        return "NULL"
    text = str(value)
    if max_chars is None or len(text) <= max_chars:
        return text
    if max_chars <= 3:
        return text[:max_chars]
    return f"{text[: max_chars - 3]}..."


def is_varchar_type(type_name: str) -> bool:
    upper = type_name.upper()
    return any(marker in upper for marker in ("CHAR", "TEXT", "STRING", "VARCHAR"))


def is_struct_type_name(type_name: str) -> bool:
    stripped = type_name.strip()
    if len(stripped) < 7:
        return False
    if stripped[:6].casefold() != "struct":
        return False
    return stripped[6:].lstrip().startswith("(")


def _type_id(dtype: Any) -> str:
    return str(getattr(dtype, "id", "")).casefold()


def _type_children(dtype: Any) -> tuple[tuple[str, Any], ...]:
    try:
        raw_children = getattr(dtype, "children")
    except Exception:  # noqa: BLE001
        return ()
    children: list[tuple[str, Any]] = []
    for child_name, child_type in raw_children:
        children.append((str(child_name), child_type))
    return tuple(children)


def is_struct_type(dtype: Any) -> bool:
    return _type_id(dtype) == "struct"


def parse_struct_fields(dtype: Any) -> tuple[StructField, ...]:
    if not is_struct_type(dtype):
        return ()
    return tuple(
        StructField(
            name=child_name,
            type_name=str(child_type),
            children=parse_struct_fields(child_type),
        )
        for child_name, child_type in _type_children(dtype)
    )


def flatten_struct_paths(fields: tuple[StructField, ...], prefix: str = "") -> tuple[StructPath, ...]:
    paths: list[StructPath] = []
    for field in fields:
        path = f"{prefix}.{field.name}" if prefix else field.name
        paths.append((path, field.type_name))
        if field.children:
            paths.extend(flatten_struct_paths(field.children, path))
    return tuple(paths)


def sort_cell_key(value: Any) -> tuple[int, int, float | str]:
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
        load_query = render_load_query(source_sql, reader.query_template)
        self.conn.execute(f'DROP VIEW IF EXISTS "{self.table_name}"')
        self.conn.execute(f'CREATE VIEW "{self.table_name}" AS {load_query}')

        self._schema_rows: list[tuple[Any, ...]] = []
        self.columns: list[str] = []
        self.column_types: dict[str, str] = {}
        self.struct_fields_by_column: dict[str, tuple[StructField, ...]] = {}
        self.struct_paths_by_column: dict[str, tuple[StructPath, ...]] = {}
        self.column_lookup: dict[str, str] = {}
        self.refresh_schema()

        self._completion_catalog = EngineCompletionCatalog(self)
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
        type_objects = self._column_type_objects_from_table()
        self.struct_fields_by_column, self.struct_paths_by_column = self._build_struct_metadata(type_objects)
        self.column_lookup = {col.lower(): col for col in self.columns}
        self._invalidate_completion_caches()

    def _column_type_objects_from_table(self) -> dict[str, Any]:
        description = self.conn.execute(f'SELECT * FROM "{self.table_name}" LIMIT 0').description
        if not description:
            return {}
        return {str(item[0]): item[1] for item in description if len(item) >= 2}

    @classmethod
    def _build_struct_metadata(
        cls,
        column_type_objects: dict[str, Any],
    ) -> tuple[dict[str, tuple[StructField, ...]], dict[str, tuple[StructPath, ...]]]:
        struct_fields_by_column: dict[str, tuple[StructField, ...]] = {}
        struct_paths_by_column: dict[str, tuple[StructPath, ...]] = {}
        for column_name, dtype in column_type_objects.items():
            fields = cls._struct_fields_for_type(dtype)
            if not fields:
                continue
            struct_fields_by_column[column_name] = fields
            struct_paths_by_column[column_name] = flatten_struct_paths(fields)
        return struct_fields_by_column, struct_paths_by_column

    @staticmethod
    def _struct_fields_for_type(dtype: Any) -> tuple[StructField, ...]:
        return parse_struct_fields(dtype)

    def _invalidate_completion_caches(self) -> None:
        if hasattr(self, "_completion_catalog"):
            self._completion_catalog.clear_cache()
        if hasattr(self, "_completion_engine"):
            self._completion_engine.clear_cache()

    def helper_commands(self) -> list[str]:
        commands: list[str] = []
        for spec in self._command_specs:
            commands.append(spec.name)
            commands.extend(spec.aliases)
        return commands

    def command_specs(self) -> list[CommandSpec]:
        return self._command_specs

    def has_helper_command(self, raw_name: str) -> bool:
        return self.lookup_command(raw_name) is not None

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
                "F8 copy full result TSV",
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
                    "Editor: completions appear while typing; Ctrl+Space opens completion mode; "
                    "Tab accepts completion; Esc closes completion menu; Up/Down navigates completion menu "
                    "when visible, otherwise moves cursor/history at first/last line; "
                    "Ctrl+Enter/F5 runs; Ctrl+N/F6 loads sample; Ctrl+L/F7 clears."
                ),
                (
                    "Navigation: Ctrl+1 focuses query editor, Ctrl+2 focuses results, "
                    "Ctrl+B toggles Data Explorer; F8 copies full result TSV. "
                    "Help: F1 or Ctrl+Shift+P."
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
            column_types=[],
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
            columns = result_columns(relation.description)
            column_types = _result_column_types(relation.description)
        except Exception as exc:  # noqa: BLE001
            return EngineResponse(status="error", message=str(exc), executed_sql=sql)

        elapsed_ms = (time.perf_counter() - t0) * 1000
        self.last_sql = sql
        if remember:
            self.executed_sql.append(sql)

        if not columns:
            return EngineResponse(
                status="ok",
                message=f"Statement executed in {elapsed_ms:.1f} ms",
                executed_sql=sql,
            )

        shown, truncated = self._display_rows(rows)
        self.last_result_sql = sql
        result = QueryResult(
            sql=sql,
            columns=columns,
            column_types=column_types,
            rows=shown,
            elapsed_ms=elapsed_ms,
            total_rows=len(rows),
            truncated=truncated,
        )
        message = f"{len(shown):,}/{len(rows):,} rows shown in {elapsed_ms:.1f} ms"
        if truncated:
            message += f" (row display limit={self.max_rows_display})"
        return EngineResponse(status="ok", message=message, result=result, executed_sql=sql)

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

    def lookup_command(self, raw_name: str) -> CommandSpec | None:
        return self._command_lookup.get(raw_name.casefold())

    def _build_sql_helper_handler(
        self,
        usage: str,
        sql_builder: Callable[[str], str | None],
    ) -> Callable[[str], EngineResponse]:
        def handler(args: str) -> EngineResponse:
            return self._run_sql_helper(sql_builder(args), usage)

        return handler

    def _sql_helper_command_specs(self) -> list[CommandSpec]:
        completion = self._completion_catalog
        helper_defs = [
            SqlHelperCommandSpec(
                "/sample",
                "/sample [n]",
                "Select sample rows.",
                self._sql_for_sample,
                completion.complete_sample,
            ),
            SqlHelperCommandSpec(
                "/filter",
                "/filter <where condition>",
                "Filter rows with a WHERE condition.",
                self._sql_for_filter,
                completion.complete_filter,
            ),
            SqlHelperCommandSpec(
                "/sort",
                "/sort <order expressions>",
                "Sort rows by expression(s).",
                self._sql_for_sort,
                completion.complete_sort,
            ),
            SqlHelperCommandSpec(
                "/group",
                "/group <group cols> | <aggregates> [| having]",
                "Aggregate by group columns.",
                self._sql_for_group,
                completion.complete_group,
            ),
            SqlHelperCommandSpec(
                "/agg",
                "/agg <aggregates> [| where]",
                "Run aggregate expression(s).",
                self._sql_for_agg,
                completion.complete_agg,
            ),
            SqlHelperCommandSpec(
                "/top",
                "/top <column> <n>",
                "Top values by frequency for a column.",
                self._sql_for_top,
                completion.complete_top,
            ),
            SqlHelperCommandSpec(
                "/dupes",
                "/dupes <key_cols_csv> [n] [| where]",
                "Find duplicate key combinations.",
                self._sql_for_dupes,
                completion.complete_dupes,
            ),
        ]
        return [
            CommandSpec(
                item.name,
                item.usage,
                item.description,
                self._build_sql_helper_handler(item.usage, item.sql_builder),
                item.completer,
            )
            for item in helper_defs
        ]

    def _build_command_specs(self) -> list[CommandSpec]:
        completion = self._completion_catalog
        return [
            CommandSpec("/help", "/help", "Show helper command reference.", self._cmd_help),
            CommandSpec("/schema", "/schema", "Show dataset schema.", self._cmd_schema),
            *self._sql_helper_command_specs(),
            CommandSpec(
                "/hist",
                "/hist <numeric_col> [bins] [| where]",
                "Histogram bins for a numeric column.",
                self._cmd_hist,
                completion.complete_hist,
            ),
            CommandSpec(
                "/crosstab",
                "/crosstab <col_a> <col_b> [n] [| where]",
                "Top value pairs by frequency.",
                self._cmd_crosstab,
                completion.complete_crosstab,
            ),
            CommandSpec(
                "/corr",
                "/corr <numeric_x> <numeric_y> [| where]",
                "Correlation and non-null pair count.",
                self._cmd_corr,
                completion.complete_corr,
            ),
            CommandSpec(
                "/profile",
                "/profile <column>",
                "Profile a single column.",
                self._cmd_profile,
                completion.complete_profile,
            ),
            CommandSpec("/describe", "/describe", "Describe columns and nulls.", self._cmd_describe),
            CommandSpec(
                "/summary",
                "/summary [n_cols] [| where]",
                "Show per-column summary statistics.",
                self._cmd_summary,
                completion.complete_summary,
            ),
            CommandSpec(
                "/history",
                "/history [n]",
                "Show recent query history.",
                self._cmd_history,
                completion.complete_history,
            ),
            CommandSpec(
                "/rerun",
                "/rerun <history_index>",
                "Rerun a query from history.",
                self._cmd_rerun,
                completion.complete_rerun,
            ),
            CommandSpec("/rows", "/rows <n>", "Set row display limit.", self._cmd_rows, completion.complete_rows),
            CommandSpec(
                "/values",
                "/values <n>",
                "Set max display length per value.",
                self._cmd_values,
                completion.complete_values,
            ),
            CommandSpec(
                "/limit",
                "/limit <n>",
                "Set helper query row limit.",
                self._cmd_limit,
                completion.complete_limit,
            ),
            CommandSpec(
                "/save",
                "/save <path.csv|path.parquet|path.json>",
                "Save latest result to disk.",
                self._cmd_save,
                completion.complete_save,
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

    @staticmethod
    def _unknown_column_error(raw_column: str) -> EngineResponse:
        return EngineResponse(status="error", message=f"Unknown column: {raw_column}")

    @staticmethod
    def _numeric_column_error(command_name: str, column: str) -> EngineResponse:
        return EngineResponse(status="error", message=f"{command_name} requires numeric column: {column}")

    def _resolve_required_column(self, raw_column: str) -> tuple[str | None, EngineResponse | None]:
        resolved = self._resolve_column(raw_column)
        if resolved is None:
            return None, self._unknown_column_error(raw_column)
        return resolved, None

    def _resolve_required_columns(
        self,
        raw_columns: tuple[str, ...],
    ) -> tuple[tuple[str, ...] | None, EngineResponse | None]:
        resolved_columns: list[str] = []
        for raw_column in raw_columns:
            resolved_column, err = self._resolve_required_column(raw_column)
            if err is not None:
                return None, err
            assert resolved_column is not None
            resolved_columns.append(resolved_column)
        return tuple(resolved_columns), None

    def _resolve_required_numeric_columns(
        self,
        command_name: str,
        raw_columns: tuple[str, ...],
    ) -> tuple[tuple[str, ...] | None, EngineResponse | None]:
        resolved_columns, err = self._resolve_required_columns(raw_columns)
        if err is not None:
            return None, err
        assert resolved_columns is not None
        for resolved_column in resolved_columns:
            type_name = self.column_types[resolved_column]
            if is_numeric_type(type_name):
                continue
            return None, self._numeric_column_error(command_name, resolved_column)
        return resolved_columns, None

    @staticmethod
    def _split_args_with_optional_where(args: str) -> tuple[list[str], str] | None:
        split = _split_optional_where(args)
        if split is None:
            return None
        base, where_clause = split
        return base.split(), where_clause

    def _resolve_unique_columns(self, raw_columns: list[str]) -> list[str] | None:
        resolved_columns: list[str] = []
        seen: set[str] = set()
        for raw_column in raw_columns:
            resolved = self._resolve_column(raw_column)
            if resolved is None:
                return None
            key = resolved.casefold()
            if key in seen:
                continue
            seen.add(key)
            resolved_columns.append(resolved)
        return resolved_columns

    def _run_command(self, command: str) -> EngineResponse:
        stripped = command.strip()
        if not stripped:
            return EngineResponse(status="info", message="Type SQL or /help.")
        parts = stripped.split(maxsplit=1)
        raw_name = parts[0]
        args = parts[1] if len(parts) == 2 else ""
        spec = self.lookup_command(raw_name)
        if spec is None:
            return EngineResponse(status="error", message=f"Unknown command: {stripped}. Use /help")
        return spec.handler(args)

    def _run_sql_helper(self, sql: str | None, usage: str) -> EngineResponse:
        if sql is None:
            return self._usage_error(usage)
        out = self.run_sql(sql)
        out.generated_sql = sql
        return out

    def _run_generated_sql(self, sql: str) -> EngineResponse:
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

    def _cmd_summary(self, args: str) -> EngineResponse:
        parsed = self._parse_summary_args(args)
        if parsed is None:
            return self._usage_error("/summary [n_cols] [| where]")
        column_limit, where_clause = parsed
        return self._summarize_dataset(column_limit, where_clause)

    def _cmd_hist(self, args: str) -> EngineResponse:
        parsed = self._parse_hist_args(args)
        if parsed is None:
            return self._usage_error("/hist <numeric_col> [bins] [| where]")
        raw_column, bins, where_clause = parsed
        resolved_columns, err = self._resolve_required_numeric_columns("/hist", (raw_column,))
        if err is not None:
            return err
        assert resolved_columns is not None
        resolved_column = resolved_columns[0]
        return self._run_generated_sql(self._sql_for_hist(resolved_column, bins, where_clause))

    def _cmd_crosstab(self, args: str) -> EngineResponse:
        parsed = self._parse_crosstab_args(args)
        if parsed is None:
            return self._usage_error("/crosstab <col_a> <col_b> [n] [| where]")
        raw_a, raw_b, limit, where_clause = parsed
        resolved_columns, err = self._resolve_required_columns((raw_a, raw_b))
        if err is not None:
            return err
        assert resolved_columns is not None
        col_a, col_b = resolved_columns
        return self._run_generated_sql(self._sql_for_crosstab(col_a, col_b, limit, where_clause))

    def _cmd_corr(self, args: str) -> EngineResponse:
        parsed = self._parse_corr_args(args)
        if parsed is None:
            return self._usage_error("/corr <numeric_x> <numeric_y> [| where]")
        raw_x, raw_y, where_clause = parsed
        resolved_columns, err = self._resolve_required_numeric_columns("/corr", (raw_x, raw_y))
        if err is not None:
            return err
        assert resolved_columns is not None
        col_x, col_y = resolved_columns
        return self._run_generated_sql(self._sql_for_corr(col_x, col_y, where_clause))

    def _cmd_history(self, args: str) -> EngineResponse:
        payload = args.strip()
        count = 20
        if payload:
            parsed = parse_single_positive_int_arg(payload)
            if parsed is None:
                return self._usage_error("/history [n]")
            count = parsed
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

    def _set_positive_int_setting(
        self,
        args: str,
        usage: str,
        label: str,
        setter: Callable[[int], None],
    ) -> EngineResponse:
        parsed = parse_single_positive_int_arg(args)
        if parsed is None:
            return self._usage_error(usage)
        setter(parsed)
        return EngineResponse(status="ok", message=f"{label} set to {parsed}")

    def _cmd_rows(self, args: str) -> EngineResponse:
        return self._set_positive_int_setting(
            args,
            "/rows <n>",
            "Row display limit",
            lambda value: setattr(self, "max_rows_display", value),
        )

    def _cmd_values(self, args: str) -> EngineResponse:
        return self._set_positive_int_setting(
            args,
            "/values <n>",
            "Value display limit",
            lambda value: setattr(self, "max_value_chars", value),
        )

    def _cmd_limit(self, args: str) -> EngineResponse:
        return self._set_positive_int_setting(
            args,
            "/limit <n>",
            "Default helper query limit",
            lambda value: setattr(self, "default_limit", value),
        )

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
        sample_n = self.default_limit if not payload else parse_single_positive_int_arg(payload)
        if sample_n is None:
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
        if len(parts) != 2:
            return None
        resolved = self._resolve_column(parts[0])
        if resolved is None:
            return None
        try:
            top_n = max(1, int(parts[1]))
        except ValueError:
            return None
        qcol = quote_ident(resolved)
        return (
            f'SELECT {qcol} AS value, COUNT(*) AS count FROM "{self.table_name}" '
            f"GROUP BY {qcol} ORDER BY count DESC, value LIMIT {top_n}"
        )

    def _sql_for_dupes(self, args: str) -> str | None:
        split = _split_optional_where(args)
        if split is None:
            return None
        base, where_clause = split
        if not base:
            return None

        tokens = base.split()
        key_columns_raw = base
        limit = self.default_limit
        if len(tokens) > 1:
            parsed_limit = parse_optional_positive_int(tokens[-1])
            if parsed_limit is not None:
                limit = parsed_limit
                key_columns_raw = " ".join(tokens[:-1]).strip()
                if not key_columns_raw:
                    return None

        if "," not in key_columns_raw and len(key_columns_raw.split()) > 1:
            trimmed = key_columns_raw.strip()
            if not (trimmed.startswith('"') and trimmed.endswith('"')):
                return None

        raw_columns = [part.strip() for part in key_columns_raw.split(",") if part.strip()]
        if not raw_columns:
            return None

        resolved_columns = self._resolve_unique_columns(raw_columns)
        if not resolved_columns:
            return None

        group_expr = ", ".join(quote_ident(column) for column in resolved_columns)
        sql = f'SELECT {group_expr}, COUNT(*) AS count FROM "{self.table_name}"'
        if where_clause:
            sql += f" WHERE {where_clause}"
        sql += f" GROUP BY {group_expr} HAVING COUNT(*) > 1 ORDER BY count DESC, {group_expr} LIMIT {limit}"
        return sql

    def _parse_hist_args(self, args: str) -> tuple[str, int, str] | None:
        parsed = self._split_args_with_optional_where(args)
        if parsed is None:
            return None
        parts, where_clause = parsed
        if len(parts) == 1:
            return parts[0], 10, where_clause
        if len(parts) == 2:
            bins = parse_optional_positive_int(parts[1])
            if bins is None:
                return None
            return parts[0], bins, where_clause
        return None

    def _parse_crosstab_args(self, args: str) -> tuple[str, str, int, str] | None:
        parsed = self._split_args_with_optional_where(args)
        if parsed is None:
            return None
        parts, where_clause = parsed
        if len(parts) == 2:
            return parts[0], parts[1], self.default_limit, where_clause
        if len(parts) == 3:
            limit = parse_optional_positive_int(parts[2])
            if limit is None:
                return None
            return parts[0], parts[1], limit, where_clause
        return None

    def _parse_corr_args(self, args: str) -> tuple[str, str, str] | None:
        parsed = self._split_args_with_optional_where(args)
        if parsed is None:
            return None
        parts, where_clause = parsed
        if len(parts) != 2:
            return None
        return parts[0], parts[1], where_clause

    def _sql_for_hist(self, column: str, bins: int, where_clause: str) -> str:
        qcol = quote_ident(column)
        predicate = f"({where_clause}) AND " if where_clause else ""
        return f'''WITH filtered AS (
    SELECT CAST({qcol} AS DOUBLE) AS value
    FROM "{self.table_name}"
    WHERE {predicate}{qcol} IS NOT NULL
),
stats AS (
    SELECT
        MIN(value) AS min_value,
        MAX(value) AS max_value,
        COUNT(*) AS total_count
    FROM filtered
),
binned AS (
    SELECT
        CASE
            WHEN stats.max_value = stats.min_value THEN 1
            ELSE LEAST(
                {bins},
                CAST(
                    FLOOR(((value - stats.min_value) / NULLIF(stats.max_value - stats.min_value, 0)) * {bins})
                    AS BIGINT
                ) + 1
            )
        END AS bin_index
    FROM filtered
    CROSS JOIN stats
),
counts AS (
    SELECT
        bin_index,
        COUNT(*) AS count
    FROM binned
    GROUP BY bin_index
)
SELECT
    stats.min_value + ((bin_index - 1) * (stats.max_value - stats.min_value) / {bins}) AS bin_start,
    CASE
        WHEN bin_index = {bins} THEN stats.max_value
        ELSE stats.min_value + (bin_index * (stats.max_value - stats.min_value) / {bins})
    END AS bin_end,
    count,
    ROUND(100.0 * count / NULLIF(stats.total_count, 0), 2) AS pct
FROM counts
CROSS JOIN stats
ORDER BY bin_start'''

    def _sql_for_crosstab(self, col_a: str, col_b: str, limit: int, where_clause: str) -> str:
        qcol_a = quote_ident(col_a)
        qcol_b = quote_ident(col_b)
        sql = f'SELECT {qcol_a}, {qcol_b}, COUNT(*) AS count FROM "{self.table_name}"'
        if where_clause:
            sql += f" WHERE {where_clause}"
        sql += f" GROUP BY {qcol_a}, {qcol_b} ORDER BY count DESC, {qcol_a}, {qcol_b} LIMIT {limit}"
        return sql

    def _sql_for_corr(self, col_x: str, col_y: str, where_clause: str) -> str:
        qcol_x = quote_ident(col_x)
        qcol_y = quote_ident(col_y)
        sql = (
            f"SELECT CORR({qcol_x}, {qcol_y}) AS corr, "
            f"COUNT(*) FILTER (WHERE {qcol_x} IS NOT NULL AND {qcol_y} IS NOT NULL) AS n_non_null "
            f'FROM "{self.table_name}"'
        )
        if where_clause:
            sql += f" WHERE {where_clause}"
        return sql

    def _parse_summary_args(self, args: str) -> tuple[int, str] | None:
        split = _split_optional_where(args)
        if split is None:
            return None
        count_part, where_clause = split
        if not count_part:
            return len(self.columns), where_clause
        parsed_count = parse_single_positive_int_arg(count_part)
        if parsed_count is None:
            return None
        return min(parsed_count, len(self.columns)), where_clause

    def _describe_dataset(self) -> EngineResponse:
        total_rows = self.row_count()
        total_columns = len(self.columns)
        column_rows: list[tuple[Any, ...]] = []
        for column in self.columns:
            qcol = quote_ident(column)
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

        message = f"Describe: {total_rows:,} rows x {total_columns} columns"
        return self._table_response(["column", "type", "distinct", "nulls", "null_%"], column_rows, message)

    def _summarize_dataset(self, column_limit: int, where_clause: str) -> EngineResponse:
        limited_columns = self.columns[: max(1, column_limit)]
        where_sql = f" WHERE {where_clause}" if where_clause else ""
        try:
            row_out = self.conn.execute(f'SELECT COUNT(*) FROM "{self.table_name}"{where_sql}').fetchone()
        except Exception as exc:  # noqa: BLE001
            return EngineResponse(status="error", message=f"Summary failed: {exc}")
        total_rows = int(row_out[0]) if row_out is not None else 0

        summary_rows: list[tuple[Any, ...]] = []
        for column in limited_columns:
            qcol = quote_ident(column)
            try:
                out = self.conn.execute(
                    f'''SELECT
                        COUNT({qcol}) AS non_null,
                        COUNT(DISTINCT {qcol}) AS distinct_count,
                        ANY_VALUE(CAST({qcol} AS VARCHAR)) FILTER (WHERE {qcol} IS NOT NULL) AS sample_value,
                        MIN(CAST({qcol} AS VARCHAR)) AS min_value,
                        MAX(CAST({qcol} AS VARCHAR)) AS max_value,
                        AVG(LENGTH(CAST({qcol} AS VARCHAR))) FILTER (WHERE {qcol} IS NOT NULL) AS avg_len
                    FROM "{self.table_name}"{where_sql}'''
                ).fetchone()
            except Exception as exc:  # noqa: BLE001
                return EngineResponse(status="error", message=f"Summary failed for column {column}: {exc}")
            if out is None:
                continue

            non_null = int(out[0])
            distinct_count = int(out[1])
            nulls = max(total_rows - non_null, 0)
            null_pct = (100.0 * nulls / total_rows) if total_rows else 0.0
            avg_len = "NULL" if out[5] is None else f"{float(out[5]):.2f}"
            summary_rows.append(
                (
                    column,
                    self.column_types[column],
                    format_scalar(out[2], self.max_value_chars),
                    format_scalar(out[3], self.max_value_chars),
                    format_scalar(out[4], self.max_value_chars),
                    avg_len,
                    distinct_count,
                    nulls,
                    f"{null_pct:.2f}%",
                )
            )

        message = f"Summary ({total_rows:,} rows x {len(summary_rows)} columns)"
        if where_clause:
            message += " with filter"
        return self._table_response(
            ["column", "type", "sample", "min", "max", "avg_len", "distinct", "nulls", "null_%"],
            summary_rows,
            message,
        )

    def _profile_column(self, raw_column: str) -> EngineResponse:
        column = self._resolve_column(raw_column)
        if column is None:
            return EngineResponse(status="error", message=f"Unknown column: {raw_column}")

        qcol = quote_ident(column)
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
            ("min", format_scalar(out[3])),
            ("max", format_scalar(out[4])),
        ]

        if is_numeric_type(self.column_types[column]):
            quantiles = self.conn.execute(
                f'''SELECT
                    QUANTILE_CONT({qcol}, 0.25),
                    QUANTILE_CONT({qcol}, 0.50),
                    QUANTILE_CONT({qcol}, 0.75)
                FROM "{self.table_name}"
                WHERE {qcol} IS NOT NULL'''
            ).fetchone()
            if quantiles is not None:
                metric_rows.append(("p25", format_scalar(quantiles[0])))
                metric_rows.append(("p50", format_scalar(quantiles[1])))
                metric_rows.append(("p75", format_scalar(quantiles[2])))

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
                columns = result_columns(relation.description)
                records = [dict(zip(columns, row, strict=False)) for row in rows]
                out_path.write_text(json.dumps(records, indent=2, default=str), encoding="utf-8")
            else:
                return EngineResponse(status="error", message="Unsupported extension. Use .csv, .parquet/.pq, or .json")
        except Exception as exc:  # noqa: BLE001
            return EngineResponse(status="error", message=f"Save failed: {exc}")

        return EngineResponse(status="ok", message=f"Saved result to {out_path}")

    def helper_command_completion_items(self) -> list[CompletionItem]:
        return self._completion_catalog.helper_command_completion_items()

    def helper_argument_completion_items(
        self,
        command_name: str,
        args: str,
        trailing_space: bool,
    ) -> list[CompletionItem]:
        return self._completion_catalog.helper_argument_completion_items(command_name, args, trailing_space)

    def sql_completion_items_for_clause(self, clause: SqlClause) -> list[CompletionItem]:
        return self._completion_catalog.sql_completion_items_for_clause(clause)

    def sql_completion_items(self) -> list[CompletionItem]:
        return self._completion_catalog.sql_completion_items()

    def sql_completion_items_for_function_args(self, function_name: str) -> list[CompletionItem]:
        return self._completion_catalog.sql_completion_items_for_function_args(function_name)

    def completion_tokens(self) -> list[str]:
        return self._completion_catalog.completion_tokens()

    def completion_items(self, text: str, cursor_location: tuple[int, int]) -> list[CompletionItem]:
        return self._completion_engine.get_result(text, cursor_location).items

    def completion_result(self, text: str, cursor_location: tuple[int, int]) -> CompletionResult:
        return self._completion_engine.get_result(text, cursor_location)

    def record_completion_acceptance(self, token: str) -> None:
        self._completion_engine.record_acceptance(token)
