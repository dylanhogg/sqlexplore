from __future__ import annotations

import json
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Callable, Literal

from sqlexplore.completion.helpers import (
    is_numeric_type,
    parse_optional_positive_int,
    parse_single_positive_int_arg,
    quote_ident,
)
from sqlexplore.core.engine_models import EngineResponse, HistoryQueryType, ResultStatus
from sqlexplore.core.logging_utils import (
    find_log_event,
    get_logger,
    log_event,
    new_trace_id,
    read_log_events,
    read_log_events_for_trace,
    truncate_for_log,
)
from sqlexplore.core.marimo_export import MarimoSessionExport, export_session_to_marimo
from sqlexplore.core.result_utils import format_scalar, result_columns, sql_literal
from sqlexplore.llm.llm_sql import resolve_llm_model, validate_llm_api_key

from .llm_runner import llm_activity_lines, run_llm_query_with_retry, summarize_llm_call_metrics
from .protocols import CommandEngine

USAGE_HELP = "/help"
USAGE_LLM = "/llm-query <natural language query>"
USAGE_LLM_HISTORY = "/llm-history [n]"
USAGE_LLM_SHOW = "/llm-show <trace_id>"
USAGE_SCHEMA = "/schema"
USAGE_TABLES = "/tables"
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
USAGE_HISTORY_LOG = "/history-log [n]"
USAGE_RERUN_LOG = "/rerun-log <event_id>"
USAGE_ROWS = "/rows <n>"
USAGE_VALUES = "/values <n>"
USAGE_LIMIT = "/limit <n>"
USAGE_SAVE = "/save <path.csv|path.parquet|path.pq|path.json>"
USAGE_SAVE_MARIMO = "/save-marimo"
USAGE_LAST = "/last"
USAGE_CLEAR = "/clear"
USAGE_EXIT = "/exit or /quit"
LLM_MISSING_KEY_MESSAGE = "LLM API key not found in environment."
LLM_PROVIDER_ERROR_MESSAGE = "LLM request failed. Check API key and network, then try again."
LLM_SQL_RETRY_INFO_MESSAGE = "Info: LLM auto-retry fixed SQL (1 retry)."
MAX_LLM_QUERY_LOG_CHARS = 8_000
logger = get_logger(__name__)


type LlmErrorKind = Literal["missing_key", "provider", "invalid_sql"]
KNOWN_QUERY_TYPES: set[HistoryQueryType] = {
    "user_entered_sql",
    "user_entered_command",
    "command_generated_sql",
    "llm_generated_sql",
}
LlmEventStatus = Literal["success", "provider_error", "invalid_sql", "missing_key"]
LLM_HISTORY_COLUMNS = [
    "trace_id",
    "session_id",
    "status",
    "retries",
    "total_tokens",
    "elapsed_secs",
    "total_cost_cents",
    "query",
    "generated_sql",
    "model",
    "request_tokens",
    "response_tokens",
    "reasoning_tokens",
]
HISTORY_COLUMNS = ["#", "session_id", "type", "status", "sql"]
HISTORY_LOG_COLUMNS = ["event_id", "session_id", "type", "status", "sql"]


@dataclass(frozen=True, slots=True)
class LlmResultEvent:
    trace_id: str
    query: str
    model: str
    status: LlmEventStatus
    retry_count: int
    request_tokens: int | None = None
    response_tokens: int | None = None
    total_tokens: int | None = None
    reasoning_tokens: int | None = None
    elapsed_secs: float | None = None
    total_cost_cents: float | None = None
    response: EngineResponse | None = None
    invalid_sql_detail: str | None = None
    generated_sql: str | None = None

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "trace_id": self.trace_id,
            "query": self.query,
            "model": self.model,
            "status": self.status,
            "retry_count": self.retry_count,
        }
        if self.invalid_sql_detail is not None:
            payload["invalid_sql_detail"] = self.invalid_sql_detail
        if self.generated_sql is not None:
            payload["generated_sql"] = self.generated_sql
        if self.request_tokens is not None:
            payload["request_tokens"] = self.request_tokens
        if self.response_tokens is not None:
            payload["response_tokens"] = self.response_tokens
        if self.total_tokens is not None:
            payload["total_tokens"] = self.total_tokens
        if self.reasoning_tokens is not None:
            payload["reasoning_tokens"] = self.reasoning_tokens
        if self.elapsed_secs is not None:
            payload["elapsed_secs"] = self.elapsed_secs
        if self.total_cost_cents is not None:
            payload["total_cost_cents"] = self.total_cost_cents
        if self.response is not None:
            payload["response_status"] = self.response.status
            payload["response_message"] = self.response.message
            if self.response.generated_sql is not None:
                payload["generated_sql"] = self.response.generated_sql
        return payload


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
    out = engine.run_sql(sql, query_type="command_generated_sql")
    out.generated_sql = sql
    return out


def run_generated_sql(
    engine: CommandEngine,
    sql: str,
    query_type: Literal["command_generated_sql", "llm_generated_sql"] = "command_generated_sql",
) -> EngineResponse:
    out = engine.run_sql(sql, query_type=query_type)
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


def llm_error_response(
    kind: LlmErrorKind,
    detail: str | None = None,
    generated_sql: str | None = None,
    activity_messages: list[tuple[ResultStatus, str]] | None = None,
) -> EngineResponse:
    if kind == "missing_key":
        return response(status="error", message=LLM_MISSING_KEY_MESSAGE, activity_messages=activity_messages)
    if kind == "provider":
        return response(status="error", message=LLM_PROVIDER_ERROR_MESSAGE, activity_messages=activity_messages)
    if kind == "invalid_sql":
        reason = (detail or "Model output was not valid SQL.").strip()
        message = f"Invalid SQL generated by model: {reason} Please rephrase your request and try again."
        return response(
            status="error",
            message=message,
            generated_sql=generated_sql,
            activity_messages=activity_messages,
        )
    assert False, kind


def _log_llm_query_event(trace_id: str, query: str, model: str) -> None:
    log_event(
        "llm.query",
        {
            "trace_id": trace_id,
            "query": query,
            "model": model,
        },
        logger=logger,
    )


def _log_llm_result_event(event: LlmResultEvent) -> None:
    log_event("llm.result", event.to_payload(), logger=logger)


def cmd_llm(engine: CommandEngine, args: str) -> EngineResponse:
    nl_query = args.strip()
    if not nl_query:
        return usage_error(USAGE_LLM)
    logger.info("llm command query=%s", truncate_for_log(nl_query, max_chars=MAX_LLM_QUERY_LOG_CHARS))
    trace_id = new_trace_id()

    api_key_error = validate_llm_api_key()
    model = resolve_llm_model()
    _log_llm_query_event(trace_id, nl_query, model)
    if api_key_error is not None:
        logger.error("llm command missing api key")
        _log_llm_result_event(
            LlmResultEvent(
                trace_id=trace_id,
                query=nl_query,
                model=model,
                status="missing_key",
                retry_count=0,
            )
        )
        return llm_error_response("missing_key")

    run_result = run_llm_query_with_retry(engine, nl_query, model, trace_id=trace_id)
    metrics_summary = summarize_llm_call_metrics(run_result.llm_call_metrics)
    base_result_event = LlmResultEvent(
        trace_id=trace_id,
        query=nl_query,
        model=model,
        status="success",
        retry_count=run_result.retry_count,
        request_tokens=metrics_summary.request_tokens,
        response_tokens=metrics_summary.response_tokens,
        total_tokens=metrics_summary.total_tokens,
        reasoning_tokens=metrics_summary.reasoning_tokens,
        elapsed_secs=metrics_summary.elapsed_secs,
        total_cost_cents=metrics_summary.total_cost_cents,
    )
    llm_activity_messages: list[tuple[ResultStatus, str]] = [
        ("info", line) for line in llm_activity_lines(run_result.llm_call_metrics)
    ]
    if run_result.status == "provider_error":
        _log_llm_result_event(replace(base_result_event, status="provider_error"))
        return llm_error_response("provider", activity_messages=llm_activity_messages)
    if run_result.status == "invalid_sql":
        _log_llm_result_event(
            replace(
                base_result_event,
                status="invalid_sql",
                invalid_sql_detail=run_result.invalid_sql_detail,
                generated_sql=run_result.generated_sql,
            )
        )
        return llm_error_response(
            "invalid_sql",
            detail=run_result.invalid_sql_detail,
            generated_sql=run_result.generated_sql,
            activity_messages=llm_activity_messages,
        )

    assert run_result.response is not None
    out = run_result.response
    if llm_activity_messages:
        out.activity_messages = [*(out.activity_messages or []), *llm_activity_messages]
    if run_result.retry_count > 0 and out.status != "error":
        if out.message:
            out.message = f"{out.message}\n{LLM_SQL_RETRY_INFO_MESSAGE}"
        else:
            out.message = LLM_SQL_RETRY_INFO_MESSAGE
    _log_llm_result_event(replace(base_result_event, status="success", response=out))
    return out


def cmd_schema(engine: CommandEngine, args: str) -> EngineResponse:
    err = require_no_args(args, USAGE_SCHEMA)
    if err is not None:
        return err
    rows = [(str(r[0]), str(r[1]), str(r[2])) for r in engine.schema_rows]
    return engine.table_response(["column", "type", "nullable"], rows, "Schema")


def cmd_tables(engine: CommandEngine, args: str) -> EngineResponse:
    err = require_no_args(args, USAGE_TABLES)
    if err is not None:
        return err
    rows = [(role, table_name, source, row_count) for role, table_name, source, row_count in engine.startup_tables()]
    return engine.table_response(["role", "table", "source", "rows"], rows, "Loaded tables")


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


def _parse_count_arg(args: str, usage: str, default_count: int = 20) -> tuple[int | None, EngineResponse | None]:
    payload = args.strip()
    if not payload:
        return default_count, None
    parsed = parse_single_positive_int_arg(payload)
    if parsed is None:
        return None, usage_error(usage)
    return parsed, None


def _parse_single_arg(args: str, usage: str) -> tuple[str | None, EngineResponse | None]:
    parts = args.strip().split()
    if len(parts) != 1:
        return None, usage_error(usage)
    return parts[0], None


def cmd_history(engine: CommandEngine, args: str) -> EngineResponse:
    count, err = _parse_count_arg(args, USAGE_HISTORY)
    if err is not None:
        return err
    assert count is not None
    history = engine.query_history[-count:]
    start_idx = max(1, len(engine.query_history) - len(history) + 1)
    rows = [
        (idx, engine.session_id, entry.query_type, entry.query_status, entry.query_text)
        for idx, entry in enumerate(history, start=start_idx)
    ]
    return engine.table_response(HISTORY_COLUMNS, rows, f"History ({len(history)} queries)")


def cmd_llm_history(engine: CommandEngine, args: str) -> EngineResponse:
    count, err = _parse_count_arg(args, USAGE_LLM_HISTORY)
    if err is not None:
        return err
    assert count is not None
    events = read_log_events(event_type="llm.result", limit=count)
    rows = [_llm_history_row(event) for event in events]
    return engine.table_response(LLM_HISTORY_COLUMNS, rows, f"LLM history ({len(rows)} traces)")


def _llm_history_row(event: dict[str, Any]) -> tuple[str, ...]:
    return (
        _event_text(event, "trace_id"),
        _event_session_id(event),
        _event_text(event, "status"),
        _event_text(event, "retry_count"),
        _event_text(event, "total_tokens"),
        _event_float_text(event, "elapsed_secs", precision=1),
        _event_float_text(event, "total_cost_cents", precision=4),
        _event_text(event, "query"),
        _event_text(event, "generated_sql"),
        _event_text(event, "model"),
        _event_text(event, "request_tokens"),
        _event_text(event, "response_tokens"),
        _event_text(event, "reasoning_tokens"),
    )


def _event_text(event: dict[str, Any], key: str) -> str:
    value = event.get(key)
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return str(value)


def _event_session_id(event: dict[str, Any]) -> str:
    return _event_text(event, "session_id")


def _event_float_text(event: dict[str, Any], key: str, precision: int) -> str:
    value = event.get(key)
    if value is None:
        return ""
    if isinstance(value, int | float):
        return f"{float(value):.{precision}f}"
    if isinstance(value, str):
        try:
            parsed = float(value)
        except ValueError:
            return value
        return f"{parsed:.{precision}f}"
    return str(value)


def _resolve_query_type_from_event(event: dict[str, Any]) -> HistoryQueryType:
    raw_query_type = event.get("query_type")
    if not isinstance(raw_query_type, str):
        return "user_entered_sql"
    if raw_query_type not in KNOWN_QUERY_TYPES:
        return "user_entered_sql"
    return raw_query_type


def _events_by_kind(events: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for event in events:
        kind = _event_text(event, "kind")
        if kind not in grouped:
            grouped[kind] = []
        grouped[kind].append(event)
    return grouped


def _last_event(grouped_events: dict[str, list[dict[str, Any]]], kind: str) -> dict[str, Any] | None:
    by_kind = grouped_events.get(kind, [])
    if not by_kind:
        return None
    return by_kind[-1]


def _bundle_generated_sql(grouped_events: dict[str, list[dict[str, Any]]]) -> str | None:
    for event_kind, field_name in (
        ("llm.result", "generated_sql"),
        ("llm.sql_execution", "sql"),
        ("llm.response", "sql"),
    ):
        event = _last_event(grouped_events, event_kind)
        if event is None:
            continue
        generated_sql = _event_text(event, field_name).strip()
        if generated_sql:
            return generated_sql
    return None


def _llm_trace_summary(grouped_events: dict[str, list[dict[str, Any]]]) -> tuple[str, str, str, str]:
    llm_query = _last_event(grouped_events, "llm.query")
    llm_result = _last_event(grouped_events, "llm.result")
    query = _event_text(llm_query or {}, "query")
    model = _event_text(llm_query or {}, "model")
    status = _event_text(llm_result or {}, "status")
    generated_sql = _bundle_generated_sql(grouped_events) or ""
    return query, model, status, generated_sql


def _trace_session_id(events: list[dict[str, Any]]) -> str:
    for event in reversed(events):
        session_id = _event_session_id(event).strip()
        if session_id:
            return session_id
    return ""


def cmd_llm_show(engine: CommandEngine, args: str) -> EngineResponse:
    trace_id, err = _parse_single_arg(args, USAGE_LLM_SHOW)
    if err is not None:
        return err
    assert trace_id is not None
    events = read_log_events_for_trace(trace_id)
    if not events:
        return response(status="error", message=f"LLM trace not found: {trace_id}")

    grouped_events = _events_by_kind(events)
    session_id = _trace_session_id(events)
    query, model, status, generated_sql = _llm_trace_summary(grouped_events)
    llm_result = _last_event(grouped_events, "llm.result")
    bundle = {
        "trace_id": trace_id,
        "query": query,
        "model": model,
        "result": llm_result or {},
        "requests": grouped_events.get("llm.request", []),
        "responses": grouped_events.get("llm.response", []),
        "retries": grouped_events.get("llm.retry", []),
        "sql_executions": grouped_events.get("llm.sql_execution", []),
    }
    bundle_json = json.dumps(bundle, ensure_ascii=True, default=str, indent=2, sort_keys=True)
    rows = [
        ("trace_id", trace_id),
        ("session_id", session_id),
        ("status", status),
        ("query", query),
        ("model", model),
        ("generated_sql", generated_sql),
        ("bundle_json", bundle_json),
    ]
    out = engine.table_response(["field", "value"], rows, f"LLM trace {trace_id}")
    if generated_sql:
        out.load_query = generated_sql
        out.message = f"{out.message}\nLoaded generated SQL in editor."
    return out


def cmd_history_log(engine: CommandEngine, args: str) -> EngineResponse:
    count, err = _parse_count_arg(args, USAGE_HISTORY_LOG)
    if err is not None:
        return err
    assert count is not None
    events = read_log_events(event_type="query.execute", limit=count)
    rows = [
        (
            _event_text(event, "event_id"),
            _event_session_id(event),
            _event_text(event, "query_type"),
            _event_text(event, "status"),
            _event_text(event, "sql"),
        )
        for event in events
    ]
    return engine.table_response(
        HISTORY_LOG_COLUMNS,
        rows,
        f"Persisted SQL history ({len(rows)} queries)",
    )


def cmd_rerun(engine: CommandEngine, args: str) -> EngineResponse:
    payload = args.strip()
    parts = payload.split()
    if len(parts) != 1:
        return usage_error(USAGE_RERUN)
    try:
        idx = int(parts[0])
    except ValueError:
        return response(status="error", message="/rerun expects an integer index")
    if idx < 1 or idx > len(engine.query_history):
        return response(status="error", message="History index out of range")
    query = engine.query_history[idx - 1].query_text
    command_name = query.split(maxsplit=1)[0].casefold()
    if command_name == "/rerun":
        return response(status="error", message="Cannot rerun a /rerun entry")
    out = engine.run_input(query)
    if not query.startswith("/"):
        out.generated_sql = query
    return out


def cmd_rerun_log(engine: CommandEngine, args: str) -> EngineResponse:
    event_id, err = _parse_single_arg(args, USAGE_RERUN_LOG)
    if err is not None:
        return err
    assert event_id is not None
    event = find_log_event(event_id, event_type="query.execute")
    if event is None:
        return response(status="error", message=f"Log event not found: {event_id}")
    sql = _event_text(event, "sql").strip()
    if not sql:
        return response(status="error", message=f"Log event has no SQL text: {event_id}")
    query_type = _resolve_query_type_from_event(event)
    out = engine.run_sql(sql, query_type=query_type)
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


def cmd_save_marimo(engine: CommandEngine, args: str) -> EngineResponse:
    err = require_no_args(args, USAGE_SAVE_MARIMO)
    if err is not None:
        return err
    try:
        output_path = export_session_to_marimo(_marimo_session_export(engine), Path.cwd())
    except (OSError, ValueError) as exc:
        return response(status="error", message=f"Marimo export failed: {exc}")
    return response(status="ok", message=f"Saved marimo notebook to {output_path}")


def _marimo_session_export(engine: CommandEngine) -> MarimoSessionExport:
    return MarimoSessionExport(
        session_id=engine.session_id,
        data_paths=engine.data_paths,
        table_name=engine.table_name,
        database=engine.database,
        query_history=tuple(engine.query_history),
    )


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
