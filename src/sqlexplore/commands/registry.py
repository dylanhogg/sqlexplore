from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from sqlexplore.completion.models import CompletionItem
from sqlexplore.core.engine_models import EngineResponse

from .handlers import (
    USAGE_AGG,
    USAGE_CLEAR,
    USAGE_CORR,
    USAGE_CROSSTAB,
    USAGE_DESCRIBE,
    USAGE_DUPES,
    USAGE_EXIT,
    USAGE_FILTER,
    USAGE_GROUP,
    USAGE_HELP,
    USAGE_HIST,
    USAGE_HISTORY,
    USAGE_HISTORY_LOG,
    USAGE_LAST,
    USAGE_LIMIT,
    USAGE_LLM,
    USAGE_LLM_HISTORY,
    USAGE_LLM_SHOW,
    USAGE_PROFILE,
    USAGE_RERUN,
    USAGE_RERUN_LOG,
    USAGE_ROWS,
    USAGE_SAMPLE,
    USAGE_SAVE,
    USAGE_SCHEMA,
    USAGE_SORT,
    USAGE_SUMMARY,
    USAGE_TOP,
    USAGE_VALUES,
    build_sql_helper_handler,
    cmd_clear,
    cmd_corr,
    cmd_crosstab,
    cmd_describe,
    cmd_dupes,
    cmd_exit,
    cmd_help,
    cmd_hist,
    cmd_history,
    cmd_history_log,
    cmd_last,
    cmd_limit,
    cmd_llm,
    cmd_llm_history,
    cmd_llm_show,
    cmd_profile,
    cmd_rerun,
    cmd_rerun_log,
    cmd_rows,
    cmd_save,
    cmd_schema,
    cmd_summary,
    cmd_top,
    cmd_values,
    response,
    sql_for_agg,
    sql_for_filter,
    sql_for_group,
    sql_for_sample,
    sql_for_sort,
)
from .protocols import CommandCompletionCatalog, CommandEngine


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
    sql_builder: Callable[[CommandEngine, str], str | None]
    completer: Callable[[str, bool], list[CompletionItem]]


def helper_commands(specs: list[CommandSpec]) -> list[str]:
    commands: list[str] = []
    for spec in specs:
        commands.append(spec.name)
        commands.extend(spec.aliases)
    return commands


def command_usage_lines(specs: list[CommandSpec]) -> list[str]:
    return [spec.usage for spec in specs]


def index_command_specs(specs: list[CommandSpec]) -> dict[str, CommandSpec]:
    lookup: dict[str, CommandSpec] = {}
    for spec in specs:
        lookup[spec.name.casefold()] = spec
        for alias in spec.aliases:
            lookup[alias.casefold()] = spec
    return lookup


def _bind(
    engine: CommandEngine,
    handler: Callable[[CommandEngine, str], EngineResponse],
) -> Callable[[str], EngineResponse]:
    return lambda args: handler(engine, args)


def build_command_specs(engine: CommandEngine, completion: CommandCompletionCatalog) -> list[CommandSpec]:
    helper_defs = [
        SqlHelperCommandSpec(
            "/sample",
            USAGE_SAMPLE,
            "Select sample rows.",
            sql_for_sample,
            completion.complete_sample,
        ),
        SqlHelperCommandSpec(
            "/filter",
            USAGE_FILTER,
            "Filter rows with a WHERE condition.",
            sql_for_filter,
            completion.complete_filter,
        ),
        SqlHelperCommandSpec(
            "/sort",
            USAGE_SORT,
            "Sort rows by expression(s).",
            sql_for_sort,
            completion.complete_sort,
        ),
        SqlHelperCommandSpec(
            "/group",
            USAGE_GROUP,
            "Aggregate by group columns.",
            sql_for_group,
            completion.complete_group,
        ),
        SqlHelperCommandSpec(
            "/agg",
            USAGE_AGG,
            "Run aggregate expression(s).",
            sql_for_agg,
            completion.complete_agg,
        ),
    ]
    return [
        CommandSpec("/help", USAGE_HELP, "Show helper command reference.", _bind(engine, cmd_help)),
        CommandSpec(
            "/llm",
            USAGE_LLM,
            "Generate DuckDB SQL from natural language.",
            _bind(engine, cmd_llm),
        ),
        CommandSpec(
            "/llm-history",
            USAGE_LLM_HISTORY,
            "Show recent LLM trace outcomes from sqlexplore.log.",
            _bind(engine, cmd_llm_history),
            completion.complete_history,
        ),
        CommandSpec(
            "/llm-show",
            USAGE_LLM_SHOW,
            "Show replay bundle for one LLM trace_id.",
            _bind(engine, cmd_llm_show),
        ),
        CommandSpec("/schema", USAGE_SCHEMA, "Show dataset schema.", _bind(engine, cmd_schema)),
        *[
            CommandSpec(
                item.name,
                item.usage,
                item.description,
                build_sql_helper_handler(engine, item.usage, item.sql_builder),
                item.completer,
            )
            for item in helper_defs
        ],
        CommandSpec(
            "/top",
            USAGE_TOP,
            "Top values by frequency for a column.",
            _bind(engine, cmd_top),
            completion.complete_top,
        ),
        CommandSpec(
            "/dupes",
            USAGE_DUPES,
            "Find duplicate key combinations.",
            _bind(engine, cmd_dupes),
            completion.complete_dupes,
        ),
        CommandSpec(
            "/hist",
            USAGE_HIST,
            "Histogram bins for a numeric column.",
            _bind(engine, cmd_hist),
            completion.complete_hist,
        ),
        CommandSpec(
            "/crosstab",
            USAGE_CROSSTAB,
            "Top value pairs by frequency.",
            _bind(engine, cmd_crosstab),
            completion.complete_crosstab,
        ),
        CommandSpec(
            "/corr",
            USAGE_CORR,
            "Correlation and non-null pair count.",
            _bind(engine, cmd_corr),
            completion.complete_corr,
        ),
        CommandSpec(
            "/profile",
            USAGE_PROFILE,
            "Profile a single column.",
            _bind(engine, cmd_profile),
            completion.complete_profile,
        ),
        CommandSpec(
            "/describe",
            USAGE_DESCRIBE,
            "Describe columns and nulls.",
            _bind(engine, cmd_describe),
        ),
        CommandSpec(
            "/summary",
            USAGE_SUMMARY,
            "Show per-column summary statistics.",
            _bind(engine, cmd_summary),
            completion.complete_summary,
        ),
        CommandSpec(
            "/history",
            USAGE_HISTORY,
            "Show recent query history.",
            _bind(engine, cmd_history),
            completion.complete_history,
        ),
        CommandSpec(
            "/history-log",
            USAGE_HISTORY_LOG,
            "Show persisted SQL history from sqlexplore.log.",
            _bind(engine, cmd_history_log),
            completion.complete_history,
        ),
        CommandSpec(
            "/rerun",
            USAGE_RERUN,
            "Rerun a query from history.",
            _bind(engine, cmd_rerun),
            completion.complete_rerun,
        ),
        CommandSpec(
            "/rerun-log",
            USAGE_RERUN_LOG,
            "Rerun SQL by event_id from sqlexplore.log.",
            _bind(engine, cmd_rerun_log),
        ),
        CommandSpec(
            "/rows",
            USAGE_ROWS,
            "Set row display limit.",
            _bind(engine, cmd_rows),
            completion.complete_rows,
        ),
        CommandSpec(
            "/values",
            USAGE_VALUES,
            "Set max display length per value.",
            _bind(engine, cmd_values),
            completion.complete_values,
        ),
        CommandSpec(
            "/limit",
            USAGE_LIMIT,
            "Set helper query + row display limits.",
            _bind(engine, cmd_limit),
            completion.complete_limit,
        ),
        CommandSpec(
            "/save",
            USAGE_SAVE,
            "Save latest result to disk.",
            _bind(engine, cmd_save),
            completion.complete_save,
        ),
        CommandSpec("/last", USAGE_LAST, "Load previous SQL into editor.", _bind(engine, cmd_last)),
        CommandSpec("/clear", USAGE_CLEAR, "Clear query editor.", _bind(engine, cmd_clear)),
        CommandSpec(
            "/exit",
            USAGE_EXIT,
            "Exit SQL explorer.",
            _bind(engine, cmd_exit),
            aliases=("/quit",),
        ),
    ]


def run_command(engine: CommandEngine, command: str) -> EngineResponse:
    stripped = command.strip()
    if not stripped:
        return response(status="info", message="Type SQL or /help.")
    parts = stripped.split(maxsplit=1)
    raw_name = parts[0]
    args = parts[1] if len(parts) == 2 else ""
    spec = engine.lookup_command(raw_name)
    if spec is None:
        return response(status="error", message=f"Unknown command: {stripped}. Use /help")
    return spec.handler(args)
