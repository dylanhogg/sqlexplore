import math
import time
import tomllib
from dataclasses import dataclass
from functools import cache
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as importlib_version
from pathlib import Path
from typing import Any, Literal, cast

import duckdb
import typer

from sqlexplore.commands.registry import (
    CommandSpec,
    build_command_specs,
    command_usage_lines,
    helper_commands,
    index_command_specs,
    run_command,
)
from sqlexplore.completion.completions import CompletionEngine, EngineCompletionCatalog
from sqlexplore.completion.models import CompletionItem, CompletionResult, SqlClause
from sqlexplore.core.data_source_plan import DataLoadMode, DataSourceBinding, plan_engine_data_sources
from sqlexplore.core.engine_models import (
    EngineResponse,
    HistoryQueryType,
    QueryHistoryEntry,
    QueryResult,
    ResultStatus,
)
from sqlexplore.core.logging_utils import get_logger, log_event, new_trace_id, truncate_for_log
from sqlexplore.core.result_utils import format_scalar, result_column_types, result_columns, sql_literal
from sqlexplore.core.sql_templates import (
    DEFAULT_LOAD_QUERY_TEMPLATE,
    TXT_LOAD_QUERY_TEMPLATE,
    render_load_query,
)

STATUS_STYLE_BY_RESULT: dict[ResultStatus, str] = {
    "ok": "green",
    "info": "cyan",
    "sql": "cyan",
    "error": "red",
}
MAX_SQL_LOG_CHARS = 8_000
logger = get_logger(__name__)
UNKNOWN_VERSION = "0.0.0+unknown"
__all__ = [
    "EngineResponse",
    "QueryResult",
    "ResultStatus",
    "STATUS_STYLE_BY_RESULT",
    "SqlExplorerEngine",
    "app_version",
    "flatten_struct_paths",
    "format_scalar",
    "is_struct_type",
    "is_struct_type_name",
    "is_varchar_type",
    "parse_struct_fields",
    "result_columns",
    "sort_cell_key",
]


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
            function_name="read_text",
            args="",
            query_template=TXT_LOAD_QUERY_TEMPLATE,
        )
    raise typer.BadParameter("Only .csv, .tsv, .txt, and .parquet/.pq files are supported.")


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
        data_sources: tuple[DataSourceBinding, ...] | None = None,
        load_mode: DataLoadMode = "union",
        active_table: str | None = None,
    ) -> None:
        logger.info(
            "engine init start data_path=%s table_name=%s database=%s default_limit=%s max_rows_display=%s "
            "max_value_chars=%s load_mode=%s source_count=%s active_table=%s",
            data_path,
            table_name,
            database,
            default_limit,
            max_rows_display,
            max_value_chars,
            load_mode,
            len(data_sources) if data_sources is not None else 1,
            active_table,
        )
        plan = plan_engine_data_sources(
            data_path=data_path,
            table_name=table_name,
            data_sources=data_sources,
            load_mode=load_mode,
            active_table=active_table,
        )
        self.load_mode = load_mode
        self._primary_table_name = plan.primary_table_name
        self.data_sources = plan.data_sources
        self.table_names = plan.table_names
        self._table_lookup = {name.casefold(): name for name in self.table_names}
        self.table_name = plan.active_table_name
        self.data_path = self._active_data_path()

        self.database = database
        self.default_limit = max(1, default_limit)
        self.max_rows_display = max(1, max_rows_display)
        self.max_value_chars = max(8, max_value_chars)

        self.conn = duckdb.connect(database=database)
        self.executed_sql: list[str] = []
        self.query_history: list[QueryHistoryEntry] = []
        self.last_sql = self.default_query
        self.last_result_sql: str | None = None

        self._load_data_sources()

        self._schema_rows: list[tuple[Any, ...]] = []
        self.columns: list[str] = []
        self.column_types: dict[str, str] = {}
        self.struct_fields_by_column: dict[str, tuple[StructField, ...]] = {}
        self.struct_paths_by_column: dict[str, tuple[StructPath, ...]] = {}
        self.column_lookup: dict[str, str] = {}
        self.refresh_schema()

        self._completion_catalog = EngineCompletionCatalog(self)
        self._command_specs = build_command_specs(self, self._completion_catalog)
        self._command_lookup = index_command_specs(self._command_specs)
        self._completion_engine = CompletionEngine(self)
        logger.info("engine init complete columns=%s", len(self.columns))

    def _active_data_path(self) -> Path:
        if self.load_mode == "union":
            return self.data_sources[0].path
        active_key = self.table_name.casefold()
        for source in self.data_sources:
            if source.table_name.casefold() == active_key:
                return source.path
        return self.data_sources[0].path

    def _load_query_for_source(self, source: DataSourceBinding) -> str:
        reader = _detect_reader(source.path)
        source_sql = f"{reader.function_name}({sql_literal(str(source.path))}{reader.args})"
        load_query = render_load_query(source_sql, reader.query_template)
        logger.debug(
            "engine data load table=%s path=%s reader=%s source_sql=%s load_query=%s",
            source.table_name,
            source.path,
            reader.function_name,
            truncate_for_log(source_sql, max_chars=MAX_SQL_LOG_CHARS),
            truncate_for_log(load_query, max_chars=MAX_SQL_LOG_CHARS),
        )
        return load_query

    def _replace_view(self, table_name: str, load_query: str) -> None:
        self.conn.execute(f'DROP VIEW IF EXISTS "{table_name}"')
        self.conn.execute(f'CREATE VIEW "{table_name}" AS {load_query}')

    def _load_union_sources(self) -> None:
        if len(self.data_sources) == 1:
            source_query = self._load_query_for_source(self.data_sources[0])
            self._replace_view(self.table_name, source_query)
            return

        union_parts = [
            f"SELECT * FROM ({self._load_query_for_source(source)}) AS src_{idx}"
            for idx, source in enumerate(self.data_sources, start=1)
        ]
        union_query = " UNION ALL ".join(union_parts)
        self._replace_view(self.table_name, union_query)

    def _load_table_sources(self) -> None:
        for source in self.data_sources:
            source_query = self._load_query_for_source(source)
            self._replace_view(source.table_name, source_query)

    def _load_data_sources(self) -> None:
        if self.load_mode == "union":
            self._load_union_sources()
            return
        self._load_table_sources()

    @property
    def default_query(self) -> str:
        return f'SELECT * FROM "{self.table_name}" LIMIT {self.default_limit}'

    def close(self) -> None:
        logger.info("engine close database=%s", self.database)
        self.conn.close()

    def refresh_schema(self, table_name: str | None = None) -> None:
        if table_name is not None:
            resolved_table_name = self.resolve_table_name(table_name)
            if resolved_table_name is None:
                raise typer.BadParameter(f"Unknown table: {table_name}")
            self.table_name = resolved_table_name
            self.data_path = self._active_data_path()

        schema_rows = self.conn.execute(f'DESCRIBE "{self.table_name}"').fetchall()
        self._schema_rows = [tuple(row) for row in schema_rows]
        self.columns = [str(row[0]) for row in self._schema_rows]
        self.column_types = {str(row[0]): str(row[1]) for row in self._schema_rows}
        type_objects = self._column_type_objects_from_table()
        self.struct_fields_by_column, self.struct_paths_by_column = self._build_struct_metadata(type_objects)
        self.column_lookup = {col.lower(): col for col in self.columns}
        self._invalidate_completion_caches()
        logger.debug(
            "schema refreshed table=%s columns=%s struct_columns=%s",
            self.table_name,
            len(self.columns),
            len(self.struct_fields_by_column),
        )

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
        return helper_commands(self._command_specs)

    def command_specs(self) -> list[CommandSpec]:
        return self._command_specs

    def has_helper_command(self, raw_name: str) -> bool:
        return self.lookup_command(raw_name) is not None

    def _command_usage_lines(self) -> list[str]:
        return command_usage_lines(self._command_specs)

    def row_count(self) -> int:
        out = self.conn.execute(f'SELECT COUNT(*) FROM "{self.table_name}"').fetchone()
        if out is None:
            return 0
        return int(out[0])

    def schema_preview(self, max_columns: int = 24) -> str:
        rows = self.row_count()
        table_lines = ["Tables"]
        for name in self.table_names:
            marker = "*" if name.casefold() == self.table_name.casefold() else "-"
            table_lines.append(f"{marker} {name}")
        head = [
            "Data Explorer",
            "",
            f"mode: {self.load_mode}",
            f"active table: {self.table_name}",
            f"active source: {self.data_path}",
            f"rows: {rows:,}",
            f"columns: {len(self.columns)}",
            "",
            *table_lines,
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
                f"Active table: {self.table_name}",
                f"Loaded tables: {', '.join(self.table_names)}",
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

    def resolve_column(self, raw_column: str) -> str | None:
        return self._resolve_column(raw_column)

    def resolve_table_name(self, raw_table_name: str) -> str | None:
        candidate = raw_table_name.strip()
        if candidate.startswith('"') and candidate.endswith('"') and len(candidate) > 1:
            candidate = candidate[1:-1].replace('""', '"')
        return self._table_lookup.get(candidate.casefold())

    def switch_table(self, raw_table_name: str) -> str | None:
        resolved_table_name = self.resolve_table_name(raw_table_name)
        if resolved_table_name is None:
            return None
        self.refresh_schema(resolved_table_name)
        self.last_sql = self.default_query
        return resolved_table_name

    @property
    def schema_rows(self) -> list[tuple[Any, ...]]:
        return self._schema_rows

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

    def table_response(self, columns: list[str], rows: list[tuple[Any, ...]], message: str) -> EngineResponse:
        return self._table_response(columns, rows, message)

    def _append_history(
        self,
        query_text: str,
        query_type: HistoryQueryType,
        query_status: Literal["success", "error"],
    ) -> None:
        self.query_history.append(
            QueryHistoryEntry(query_text=query_text, query_type=query_type, query_status=query_status)
        )

    def run_sql(
        self,
        sql_text: str,
        remember: bool = True,
        add_to_query_history: bool = True,
        query_type: HistoryQueryType = "user_entered_sql",
    ) -> EngineResponse:
        sql = sql_text.strip().rstrip(";")
        if not sql:
            logger.info("run_sql skipped empty query")
            return EngineResponse(status="info", message="Query is empty.")
        trace_id = new_trace_id()
        logger.debug(
            "run_sql start query_type=%s remember=%s add_to_query_history=%s sql_chars=%s sql=%s",
            query_type,
            remember,
            add_to_query_history,
            len(sql),
            truncate_for_log(sql, max_chars=MAX_SQL_LOG_CHARS),
        )

        t0 = time.perf_counter()
        try:
            relation = self.conn.execute(sql)
            rows = relation.fetchall()
            columns = result_columns(relation.description)
            column_types = result_column_types(relation.description)
        except Exception as exc:  # noqa: BLE001
            if remember and add_to_query_history:
                self._append_history(sql, query_type, "error")
            log_event(
                "query.execute",
                {
                    "trace_id": trace_id,
                    "query_type": query_type,
                    "status": "error",
                    "sql": sql,
                    "error": str(exc),
                },
                logger=logger,
            )
            logger.exception(
                "run_sql error query_type=%s sql=%s",
                query_type,
                truncate_for_log(sql, max_chars=MAX_SQL_LOG_CHARS),
            )
            return EngineResponse(status="error", message=str(exc), executed_sql=sql)

        elapsed_ms = (time.perf_counter() - t0) * 1000
        self.last_sql = sql
        if remember:
            self.executed_sql.append(sql)
            if add_to_query_history:
                self._append_history(sql, query_type, "success")

        if not columns:
            log_event(
                "query.execute",
                {
                    "trace_id": trace_id,
                    "query_type": query_type,
                    "status": "success",
                    "sql": sql,
                    "elapsed_ms": elapsed_ms,
                    "rows": 0,
                    "columns": 0,
                    "truncated": False,
                },
                logger=logger,
            )
            logger.info("run_sql complete non-tabular elapsed_ms=%.1f", elapsed_ms)
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
        out_of_rows = len(rows)
        out_of_rows = max(out_of_rows, self.row_count())
        message = f"{len(shown):,}/{out_of_rows:,} rows shown in {elapsed_ms:.1f} ms"
        if truncated:
            message += f" (row display limit={self.max_rows_display})"
        log_event(
            "query.execute",
            {
                "trace_id": trace_id,
                "query_type": query_type,
                "status": "success",
                "sql": sql,
                "elapsed_ms": elapsed_ms,
                "rows": len(rows),
                "columns": len(columns),
                "truncated": truncated,
            },
            logger=logger,
        )
        logger.info(
            "run_sql complete rows=%s shown=%s columns=%s elapsed_ms=%.1f truncated=%s",
            len(rows),
            len(shown),
            len(columns),
            elapsed_ms,
            truncated,
        )
        return EngineResponse(status="ok", message=message, result=result, executed_sql=sql)

    def run_input(self, raw_input: str, add_to_query_history: bool = True) -> EngineResponse:
        text = raw_input.strip()
        if not text:
            logger.info("run_input empty")
            return EngineResponse(status="info", message="Type SQL or /help.")
        if text.startswith("/"):
            logger.info("run_input helper_command command=%s", text.split(maxsplit=1)[0])
            out = run_command(self, text)
            if add_to_query_history:
                query_status: Literal["success", "error"] = "error" if out.status == "error" else "success"
                self._append_history(text, "user_entered_command", query_status)
            return out
        return self.run_sql(text, add_to_query_history=add_to_query_history, query_type="user_entered_sql")

    def lookup_command(self, raw_name: str) -> CommandSpec | None:
        return self._command_lookup.get(raw_name.casefold())

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
