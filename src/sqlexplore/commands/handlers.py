from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable

from sqlexplore.completion.helpers import (
    is_numeric_type,
    parse_optional_positive_int,
    parse_single_positive_int_arg,
    quote_ident,
)
from sqlexplore.core.engine_models import EngineResponse, ResultStatus
from sqlexplore.core.result_utils import format_scalar, result_columns, sql_literal
from sqlexplore.llm.llm_sql import (
    build_prompt,
    build_schema_context,
    fetch_sample_rows,
    generate_sql,
    resolve_llm_model,
    validate_generated_sql,
    validate_llm_api_key,
)

from .protocols import CommandEngine

USAGE_HELP = "/help"
USAGE_LLM = "/llm query <natural language query>"
USAGE_SCHEMA = "/schema"
USAGE_SAMPLE = "/sample [n]"
USAGE_FILTER = "/filter <where condition>"
USAGE_SORT = "/sort <order expressions>"
USAGE_GROUP = "/group <group cols> | <aggregates> [| having]"
USAGE_AGG = "/agg <aggregates> [| where]"
USAGE_TOP = "/top <column> <n>"
USAGE_DUPES = "/dupes <key_cols_csv> [n] [| where]"
USAGE_HIST = "/hist <numeric_col> [bins] [| where]"
USAGE_CROSSTAB = "/crosstab <col_a> <col_b> [n] [| where]"
USAGE_CORR = "/corr <numeric_x> <numeric_y> [| where]"
USAGE_PROFILE = "/profile <column>"
USAGE_DESCRIBE = "/describe"
USAGE_SUMMARY = "/summary [n_cols] [| where]"
USAGE_HISTORY = "/history [n]"
USAGE_RERUN = "/rerun <history_index>"
USAGE_ROWS = "/rows <n>"
USAGE_VALUES = "/values <n>"
USAGE_LIMIT = "/limit <n>"
USAGE_SAVE = "/save <path.csv|path.parquet|path.pq|path.json>"
USAGE_LAST = "/last"
USAGE_CLEAR = "/clear"
USAGE_EXIT = "/exit or /quit"


def response(status: ResultStatus, message: str, **kwargs: Any) -> EngineResponse:
    return EngineResponse(status=status, message=message, **kwargs)


def usage_error(usage: str) -> EngineResponse:
    return response(status="error", message=f"Usage: {usage}")


def require_no_args(args: str, usage: str) -> EngineResponse | None:
    if args.strip():
        return usage_error(usage)
    return None


def unknown_column_error(raw_column: str) -> EngineResponse:
    return response(status="error", message=f"Unknown column: {raw_column}")


def numeric_column_error(command_name: str, column: str) -> EngineResponse:
    return response(status="error", message=f"{command_name} requires numeric column: {column}")


def resolve_required_column(engine: CommandEngine, raw_column: str) -> tuple[str | None, EngineResponse | None]:
    resolved = engine.resolve_column(raw_column)
    if resolved is None:
        return None, unknown_column_error(raw_column)
    return resolved, None


def resolve_required_columns(
    engine: CommandEngine,
    raw_columns: tuple[str, ...],
) -> tuple[tuple[str, ...] | None, EngineResponse | None]:
    resolved_columns: list[str] = []
    for raw_column in raw_columns:
        resolved_column, err = resolve_required_column(engine, raw_column)
        if err is not None:
            return None, err
        assert resolved_column is not None
        resolved_columns.append(resolved_column)
    return tuple(resolved_columns), None


def resolve_required_numeric_columns(
    engine: CommandEngine,
    command_name: str,
    raw_columns: tuple[str, ...],
) -> tuple[tuple[str, ...] | None, EngineResponse | None]:
    resolved_columns, err = resolve_required_columns(engine, raw_columns)
    if err is not None:
        return None, err
    assert resolved_columns is not None
    for resolved_column in resolved_columns:
        type_name = engine.column_types[resolved_column]
        if is_numeric_type(type_name):
            continue
        return None, numeric_column_error(command_name, resolved_column)
    return resolved_columns, None


def split_optional_where(raw: str) -> tuple[str, str] | None:
    payload = raw.strip()
    if "|" not in payload:
        return payload, ""
    before, after = payload.split("|", maxsplit=1)
    where_clause = after.strip()
    if not where_clause:
        return None
    return before.strip(), where_clause


def split_args_with_optional_where(args: str) -> tuple[list[str], str] | None:
    split = split_optional_where(args)
    if split is None:
        return None
    base, where_clause = split
    return base.split(), where_clause


def resolve_unique_columns(
    engine: CommandEngine,
    raw_columns: list[str],
) -> tuple[list[str] | None, EngineResponse | None]:
    resolved_columns: list[str] = []
    seen: set[str] = set()
    for raw_column in raw_columns:
        resolved, err = resolve_required_column(engine, raw_column)
        if err is not None:
            return None, err
        assert resolved is not None
        key = resolved.casefold()
        if key in seen:
            continue
        seen.add(key)
        resolved_columns.append(resolved)
    return resolved_columns, None


def run_sql_helper(engine: CommandEngine, sql: str | None, usage: str) -> EngineResponse:
    if sql is None:
        return usage_error(usage)
    out = engine.run_sql(sql)
    out.generated_sql = sql
    return out


def run_generated_sql(engine: CommandEngine, sql: str) -> EngineResponse:
    out = engine.run_sql(sql)
    out.generated_sql = sql
    return out


def build_sql_helper_handler(
    engine: CommandEngine,
    usage: str,
    sql_builder: Callable[[CommandEngine, str], str | None],
) -> Callable[[str], EngineResponse]:
    def handler(args: str) -> EngineResponse:
        return run_sql_helper(engine, sql_builder(engine, args), usage)

    return handler


def cmd_help(engine: CommandEngine, args: str) -> EngineResponse:
    err = require_no_args(args, USAGE_HELP)
    if err is not None:
        return err
    rows = [(spec.name, spec.usage, spec.description) for spec in engine.command_specs()]
    return engine.table_response(["command", "usage", "description"], rows, "Helper commands")


def _parse_llm_query_args(args: str) -> str | None:
    payload = args.strip()
    if not payload:
        return None
    parts = payload.split(maxsplit=1)
    if len(parts) != 2:
        return None
    subcommand, query = parts
    if subcommand.casefold() != "query":
        return None
    query_payload = query.strip()
    return query_payload or None


def cmd_llm(engine: CommandEngine, args: str) -> EngineResponse:
    nl_query = _parse_llm_query_args(args)
    if nl_query is None:
        return usage_error(USAGE_LLM)

    api_key_error = validate_llm_api_key()
    if api_key_error is not None:
        return response(status="error", message=api_key_error)

    model = resolve_llm_model()
    schema_context = build_schema_context(engine)
    sample_rows = fetch_sample_rows(engine)
    prompt = build_prompt(
        user_query=nl_query,
        table_name=engine.table_name,
        schema_context=schema_context,
        sample_rows=sample_rows,
    )
    try:
        sql = generate_sql(prompt=prompt, model=model)
    except Exception as exc:  # noqa: BLE001
        return response(status="error", message=f"LLM provider error: {exc}")

    sql_error = validate_generated_sql(sql, engine.table_name)
    if sql_error is not None:
        return response(status="error", message=sql_error)
    return run_generated_sql(engine, sql)


def cmd_schema(engine: CommandEngine, args: str) -> EngineResponse:
    err = require_no_args(args, USAGE_SCHEMA)
    if err is not None:
        return err
    rows = [(str(r[0]), str(r[1]), str(r[2])) for r in engine.schema_rows]
    return engine.table_response(["column", "type", "nullable"], rows, "Schema")


def cmd_profile(engine: CommandEngine, args: str) -> EngineResponse:
    payload = args.strip()
    if not payload:
        return usage_error(USAGE_PROFILE)
    return profile_column(engine, payload)


def cmd_describe(engine: CommandEngine, args: str) -> EngineResponse:
    err = require_no_args(args, USAGE_DESCRIBE)
    if err is not None:
        return err
    return describe_dataset(engine)


def cmd_summary(engine: CommandEngine, args: str) -> EngineResponse:
    parsed = parse_summary_args(engine, args)
    if parsed is None:
        return usage_error(USAGE_SUMMARY)
    column_limit, where_clause = parsed
    return summarize_dataset(engine, column_limit, where_clause)


def cmd_hist(engine: CommandEngine, args: str) -> EngineResponse:
    parsed = parse_hist_args(args)
    if parsed is None:
        return usage_error(USAGE_HIST)
    raw_column, bins, where_clause = parsed
    resolved_columns, err = resolve_required_numeric_columns(engine, "/hist", (raw_column,))
    if err is not None:
        return err
    assert resolved_columns is not None
    resolved_column = resolved_columns[0]
    return run_generated_sql(engine, sql_for_hist(engine, resolved_column, bins, where_clause))


def cmd_crosstab(engine: CommandEngine, args: str) -> EngineResponse:
    parsed = parse_crosstab_args(engine, args)
    if parsed is None:
        return usage_error(USAGE_CROSSTAB)
    raw_a, raw_b, limit, where_clause = parsed
    resolved_columns, err = resolve_required_columns(engine, (raw_a, raw_b))
    if err is not None:
        return err
    assert resolved_columns is not None
    col_a, col_b = resolved_columns
    return run_generated_sql(engine, sql_for_crosstab(engine, col_a, col_b, limit, where_clause))


def cmd_corr(engine: CommandEngine, args: str) -> EngineResponse:
    parsed = parse_corr_args(args)
    if parsed is None:
        return usage_error(USAGE_CORR)
    raw_x, raw_y, where_clause = parsed
    resolved_columns, err = resolve_required_numeric_columns(engine, "/corr", (raw_x, raw_y))
    if err is not None:
        return err
    assert resolved_columns is not None
    col_x, col_y = resolved_columns
    return run_generated_sql(engine, sql_for_corr(engine, col_x, col_y, where_clause))


def cmd_top(engine: CommandEngine, args: str) -> EngineResponse:
    parts = args.strip().split()
    if len(parts) != 2:
        return usage_error(USAGE_TOP)
    resolved_column, err = resolve_required_column(engine, parts[0])
    if err is not None:
        return err
    assert resolved_column is not None
    top_n = parse_optional_positive_int(parts[1])
    if top_n is None:
        return usage_error(USAGE_TOP)
    return run_generated_sql(engine, sql_for_top(engine, resolved_column, top_n))


def cmd_dupes(engine: CommandEngine, args: str) -> EngineResponse:
    parsed = parse_dupes_args(engine, args)
    if parsed is None:
        return usage_error(USAGE_DUPES)
    raw_columns, limit, where_clause = parsed
    resolved_columns, err = resolve_unique_columns(engine, raw_columns)
    if err is not None:
        return err
    assert resolved_columns is not None
    return run_generated_sql(engine, sql_for_dupes(engine, resolved_columns, limit, where_clause))


def cmd_history(engine: CommandEngine, args: str) -> EngineResponse:
    payload = args.strip()
    count = 20
    if payload:
        parsed = parse_single_positive_int_arg(payload)
        if parsed is None:
            return usage_error(USAGE_HISTORY)
        count = parsed
    history = engine.executed_sql[-count:]
    start_idx = max(1, len(engine.executed_sql) - len(history) + 1)
    rows = [(idx, sql) for idx, sql in enumerate(history, start=start_idx)]
    return engine.table_response(["#", "sql"], rows, f"History ({len(history)} queries)")


def cmd_rerun(engine: CommandEngine, args: str) -> EngineResponse:
    payload = args.strip()
    parts = payload.split()
    if len(parts) != 1:
        return usage_error(USAGE_RERUN)
    try:
        idx = int(parts[0])
    except ValueError:
        return response(status="error", message="/rerun expects an integer index")
    if idx < 1 or idx > len(engine.executed_sql):
        return response(status="error", message="History index out of range")
    sql = engine.executed_sql[idx - 1]
    out = engine.run_sql(sql)
    out.generated_sql = sql
    return out


def set_positive_int_setting(
    engine: CommandEngine,
    args: str,
    usage: str,
    label: str,
    setter: Callable[[int], None],
) -> EngineResponse:
    parsed = parse_single_positive_int_arg(args)
    if parsed is None:
        return usage_error(usage)
    setter(parsed)
    return response(status="ok", message=f"{label} set to {parsed}")


def cmd_rows(engine: CommandEngine, args: str) -> EngineResponse:
    return set_positive_int_setting(
        engine,
        args,
        USAGE_ROWS,
        "Row display limit",
        lambda value: setattr(engine, "max_rows_display", value),
    )


def cmd_values(engine: CommandEngine, args: str) -> EngineResponse:
    return set_positive_int_setting(
        engine,
        args,
        USAGE_VALUES,
        "Value display limit",
        lambda value: setattr(engine, "max_value_chars", value),
    )


def cmd_limit(engine: CommandEngine, args: str) -> EngineResponse:
    parsed = parse_single_positive_int_arg(args)
    if parsed is None:
        return usage_error(USAGE_LIMIT)
    engine.default_limit = parsed
    engine.max_rows_display = parsed
    return response(
        status="ok",
        message=f"Default helper query limit set to {parsed}; row display limit set to {parsed}",
        load_query=f'SELECT * FROM "{engine.table_name}" LIMIT {engine.default_limit}',
    )


def cmd_save(engine: CommandEngine, args: str) -> EngineResponse:
    payload = args.strip()
    if not payload:
        return usage_error(USAGE_SAVE)
    return save_last_result(engine, payload)


def cmd_last(engine: CommandEngine, args: str) -> EngineResponse:
    err = require_no_args(args, USAGE_LAST)
    if err is not None:
        return err
    return response(status="info", message="Loaded last SQL in editor.", load_query=engine.last_sql)


def cmd_clear(engine: CommandEngine, args: str) -> EngineResponse:
    err = require_no_args(args, USAGE_CLEAR)
    if err is not None:
        return err
    return response(status="info", message="Editor cleared.", clear_editor=True)


def cmd_exit(engine: CommandEngine, args: str) -> EngineResponse:
    err = require_no_args(args, USAGE_EXIT)
    if err is not None:
        return err
    return response(status="info", message="Exiting SQL explorer.", should_exit=True)


def sql_for_sample(engine: CommandEngine, args: str) -> str | None:
    payload = args.strip()
    sample_n = engine.default_limit if not payload else parse_single_positive_int_arg(payload)
    if sample_n is None:
        return None
    return f'SELECT * FROM "{engine.table_name}" LIMIT {sample_n}'


def sql_for_filter(engine: CommandEngine, args: str) -> str | None:
    cond = args.strip()
    if not cond:
        return None
    return f'SELECT * FROM "{engine.table_name}" WHERE {cond} LIMIT {engine.default_limit}'


def sql_for_sort(engine: CommandEngine, args: str) -> str | None:
    expr = args.strip()
    if not expr:
        return None
    return f'SELECT * FROM "{engine.table_name}" ORDER BY {expr} LIMIT {engine.default_limit}'


def sql_for_group(engine: CommandEngine, args: str) -> str | None:
    payload = args.strip()
    if not payload:
        return None
    parts = [part.strip() for part in payload.split("|")]
    if any(not part for part in parts):
        return None
    if len(parts) > 3:
        return None
    group_cols = parts[0]
    if len(parts) == 1:
        return (
            f'SELECT {group_cols}, COUNT(*) AS count FROM "{engine.table_name}" '
            f"GROUP BY {group_cols} ORDER BY count DESC, {group_cols}"
        )
    aggs = parts[1]
    having = parts[2] if len(parts) > 2 else ""
    sql = f'SELECT {group_cols}, {aggs} FROM "{engine.table_name}" GROUP BY {group_cols}'
    if having:
        sql += f" HAVING {having}"
    sql += f" ORDER BY {group_cols}"
    return sql


def sql_for_agg(engine: CommandEngine, args: str) -> str | None:
    payload = args.strip()
    if not payload:
        return None
    parts = [part.strip() for part in payload.split("|")]
    if any(not part for part in parts):
        return None
    if len(parts) > 2:
        return None
    aggs = parts[0]
    where = parts[1] if len(parts) > 1 else ""
    sql = f'SELECT {aggs} FROM "{engine.table_name}"'
    if where:
        sql += f" WHERE {where}"
    return sql


def sql_for_top(engine: CommandEngine, column: str, top_n: int) -> str:
    qcol = quote_ident(column)
    return (
        f'SELECT {qcol} AS value, COUNT(*) AS count FROM "{engine.table_name}" '
        f"GROUP BY {qcol} ORDER BY count DESC, value LIMIT {top_n}"
    )


def parse_dupes_args(engine: CommandEngine, args: str) -> tuple[list[str], int, str] | None:
    split = split_optional_where(args)
    if split is None:
        return None
    base, where_clause = split
    if not base:
        return None

    tokens = base.split()
    key_columns_raw = base
    limit = engine.default_limit
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
    return raw_columns, limit, where_clause


def sql_for_dupes(engine: CommandEngine, resolved_columns: list[str], limit: int, where_clause: str) -> str:
    group_expr = ", ".join(quote_ident(column) for column in resolved_columns)
    sql = f'SELECT {group_expr}, COUNT(*) AS count FROM "{engine.table_name}"'
    if where_clause:
        sql += f" WHERE {where_clause}"
    sql += f" GROUP BY {group_expr} HAVING COUNT(*) > 1 ORDER BY count DESC, {group_expr} LIMIT {limit}"
    return sql


def parse_hist_args(args: str) -> tuple[str, int, str] | None:
    parsed = split_args_with_optional_where(args)
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


def parse_crosstab_args(engine: CommandEngine, args: str) -> tuple[str, str, int, str] | None:
    parsed = split_args_with_optional_where(args)
    if parsed is None:
        return None
    parts, where_clause = parsed
    if len(parts) == 2:
        return parts[0], parts[1], engine.default_limit, where_clause
    if len(parts) == 3:
        limit = parse_optional_positive_int(parts[2])
        if limit is None:
            return None
        return parts[0], parts[1], limit, where_clause
    return None


def parse_corr_args(args: str) -> tuple[str, str, str] | None:
    parsed = split_args_with_optional_where(args)
    if parsed is None:
        return None
    parts, where_clause = parsed
    if len(parts) != 2:
        return None
    return parts[0], parts[1], where_clause


def sql_for_hist(engine: CommandEngine, column: str, bins: int, where_clause: str) -> str:
    qcol = quote_ident(column)
    predicate = f"({where_clause}) AND " if where_clause else ""
    return f'''WITH filtered AS (
    SELECT CAST({qcol} AS DOUBLE) AS value
    FROM "{engine.table_name}"
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


def sql_for_crosstab(engine: CommandEngine, col_a: str, col_b: str, limit: int, where_clause: str) -> str:
    qcol_a = quote_ident(col_a)
    qcol_b = quote_ident(col_b)
    sql = f'SELECT {qcol_a}, {qcol_b}, COUNT(*) AS count FROM "{engine.table_name}"'
    if where_clause:
        sql += f" WHERE {where_clause}"
    sql += f" GROUP BY {qcol_a}, {qcol_b} ORDER BY count DESC, {qcol_a}, {qcol_b} LIMIT {limit}"
    return sql


def sql_for_corr(engine: CommandEngine, col_x: str, col_y: str, where_clause: str) -> str:
    qcol_x = quote_ident(col_x)
    qcol_y = quote_ident(col_y)
    sql = (
        f"SELECT CORR({qcol_x}, {qcol_y}) AS corr, "
        f"COUNT(*) FILTER (WHERE {qcol_x} IS NOT NULL AND {qcol_y} IS NOT NULL) AS n_non_null "
        f'FROM "{engine.table_name}"'
    )
    if where_clause:
        sql += f" WHERE {where_clause}"
    return sql


def parse_summary_args(engine: CommandEngine, args: str) -> tuple[int, str] | None:
    split = split_optional_where(args)
    if split is None:
        return None
    count_part, where_clause = split
    if not count_part:
        return len(engine.columns), where_clause
    parsed_count = parse_single_positive_int_arg(count_part)
    if parsed_count is None:
        return None
    return min(parsed_count, len(engine.columns)), where_clause


def describe_dataset(engine: CommandEngine) -> EngineResponse:
    total_rows = engine.row_count()
    total_columns = len(engine.columns)
    column_rows: list[tuple[Any, ...]] = []
    for column in engine.columns:
        qcol = quote_ident(column)
        out = engine.conn.execute(
            f'''SELECT
                COUNT(*) AS rows,
                COUNT({qcol}) AS non_null,
                COUNT(DISTINCT {qcol}) AS distinct_count
            FROM "{engine.table_name}"'''
        ).fetchone()
        if out is None:
            continue
        rows = int(out[0])
        non_null = int(out[1])
        distinct_count = int(out[2])
        nulls = rows - non_null
        null_pct = (100.0 * nulls / rows) if rows else 0.0
        column_rows.append((column, engine.column_types[column], distinct_count, nulls, f"{null_pct:.2f}%"))

    message = f"Describe: {total_rows:,} rows x {total_columns} columns"
    return engine.table_response(["column", "type", "distinct", "nulls", "null_%"], column_rows, message)


def summarize_dataset(engine: CommandEngine, column_limit: int, where_clause: str) -> EngineResponse:
    limited_columns = engine.columns[: max(1, column_limit)]
    where_sql = f" WHERE {where_clause}" if where_clause else ""
    try:
        row_out = engine.conn.execute(f'SELECT COUNT(*) FROM "{engine.table_name}"{where_sql}').fetchone()
    except Exception as exc:  # noqa: BLE001
        return response(status="error", message=f"Summary failed: {exc}")
    total_rows = int(row_out[0]) if row_out is not None else 0

    summary_rows: list[tuple[Any, ...]] = []
    for column in limited_columns:
        qcol = quote_ident(column)
        try:
            out = engine.conn.execute(
                f'''SELECT
                    COUNT({qcol}) AS non_null,
                    COUNT(DISTINCT {qcol}) AS distinct_count,
                    ANY_VALUE(CAST({qcol} AS VARCHAR)) FILTER (WHERE {qcol} IS NOT NULL) AS sample_value,
                    MIN(CAST({qcol} AS VARCHAR)) AS min_value,
                    MAX(CAST({qcol} AS VARCHAR)) AS max_value,
                    AVG(LENGTH(CAST({qcol} AS VARCHAR))) FILTER (WHERE {qcol} IS NOT NULL) AS avg_len
                FROM "{engine.table_name}"{where_sql}'''
            ).fetchone()
        except Exception as exc:  # noqa: BLE001
            return response(status="error", message=f"Summary failed for column {column}: {exc}")
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
                engine.column_types[column],
                format_scalar(out[2], engine.max_value_chars),
                format_scalar(out[3], engine.max_value_chars),
                format_scalar(out[4], engine.max_value_chars),
                avg_len,
                distinct_count,
                nulls,
                f"{null_pct:.2f}%",
            )
        )

    message = f"Summary ({total_rows:,} rows x {len(summary_rows)} columns)"
    if where_clause:
        message += " with filter"
    return engine.table_response(
        ["column", "type", "sample", "min", "max", "avg_len", "distinct", "nulls", "null_%"],
        summary_rows,
        message,
    )


def profile_column(engine: CommandEngine, raw_column: str) -> EngineResponse:
    column = engine.resolve_column(raw_column)
    if column is None:
        return response(status="error", message=f"Unknown column: {raw_column}")

    qcol = quote_ident(column)
    out = engine.conn.execute(
        f'''SELECT
            COUNT(*) AS rows,
            COUNT(DISTINCT {qcol}) AS distinct_count,
            SUM(CASE WHEN {qcol} IS NULL THEN 1 ELSE 0 END) AS null_count,
            MIN({qcol}) AS min_value,
            MAX({qcol}) AS max_value
        FROM "{engine.table_name}"'''
    ).fetchone()
    if out is None:
        return response(status="error", message=f"Failed to profile column: {column}")

    metric_rows: list[tuple[Any, ...]] = [
        ("column", column),
        ("type", engine.column_types[column]),
        ("rows", int(out[0])),
        ("distinct", int(out[1])),
        ("nulls", int(out[2])),
        ("min", format_scalar(out[3])),
        ("max", format_scalar(out[4])),
    ]

    if is_numeric_type(engine.column_types[column]):
        quantiles = engine.conn.execute(
            f'''SELECT
                QUANTILE_CONT({qcol}, 0.25),
                QUANTILE_CONT({qcol}, 0.50),
                QUANTILE_CONT({qcol}, 0.75)
            FROM "{engine.table_name}"
            WHERE {qcol} IS NOT NULL'''
        ).fetchone()
        if quantiles is not None:
            metric_rows.append(("p25", format_scalar(quantiles[0])))
            metric_rows.append(("p50", format_scalar(quantiles[1])))
            metric_rows.append(("p75", format_scalar(quantiles[2])))

    return engine.table_response(["metric", "value"], metric_rows, f"Profile for {column}")


def save_last_result(engine: CommandEngine, path_arg: str) -> EngineResponse:
    if engine.last_result_sql is None:
        return response(status="error", message="No query result to save yet")

    out_path = Path(path_arg).expanduser().resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    suffix = out_path.suffix.lower()

    try:
        if suffix == ".csv":
            engine.conn.execute(
                f"COPY ({engine.last_result_sql}) TO {sql_literal(str(out_path))} (HEADER, DELIMITER ',')"
            )
        elif suffix in {".parquet", ".pq"}:
            engine.conn.execute(f"COPY ({engine.last_result_sql}) TO {sql_literal(str(out_path))} (FORMAT PARQUET)")
        elif suffix == ".json":
            relation = engine.conn.execute(engine.last_result_sql)
            rows = relation.fetchall()
            columns = result_columns(relation.description)
            records = [dict(zip(columns, row, strict=False)) for row in rows]
            out_path.write_text(json.dumps(records, indent=2, default=str), encoding="utf-8")
        else:
            return response(status="error", message="Unsupported extension. Use .csv, .parquet/.pq, or .json")
    except Exception as exc:  # noqa: BLE001
        return response(status="error", message=f"Save failed: {exc}")

    return response(status="ok", message=f"Saved result to {out_path}")
