from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from sqlexplore.completion.models import CompletionItem
from sqlexplore.core.engine_models import EngineResponse

from .handlers import (
    build_sql_helper_handler,
    cmd_clear,
    cmd_corr,
    cmd_crosstab,
    cmd_describe,
    cmd_exit,
    cmd_help,
    cmd_hist,
    cmd_history,
    cmd_last,
    cmd_limit,
    cmd_profile,
    cmd_rerun,
    cmd_rows,
    cmd_save,
    cmd_schema,
    cmd_summary,
    cmd_values,
    response,
    sql_for_agg,
    sql_for_dupes,
    sql_for_filter,
    sql_for_group,
    sql_for_sample,
    sql_for_sort,
    sql_for_top,
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
            "/sample [n]",
            "Select sample rows.",
            sql_for_sample,
            completion.complete_sample,
        ),
        SqlHelperCommandSpec(
            "/filter",
            "/filter <where condition>",
            "Filter rows with a WHERE condition.",
            sql_for_filter,
            completion.complete_filter,
        ),
        SqlHelperCommandSpec(
            "/sort",
            "/sort <order expressions>",
            "Sort rows by expression(s).",
            sql_for_sort,
            completion.complete_sort,
        ),
        SqlHelperCommandSpec(
            "/group",
            "/group <group cols> | <aggregates> [| having]",
            "Aggregate by group columns.",
            sql_for_group,
            completion.complete_group,
        ),
        SqlHelperCommandSpec(
            "/agg",
            "/agg <aggregates> [| where]",
            "Run aggregate expression(s).",
            sql_for_agg,
            completion.complete_agg,
        ),
        SqlHelperCommandSpec(
            "/top",
            "/top <column> <n>",
            "Top values by frequency for a column.",
            sql_for_top,
            completion.complete_top,
        ),
        SqlHelperCommandSpec(
            "/dupes",
            "/dupes <key_cols_csv> [n] [| where]",
            "Find duplicate key combinations.",
            sql_for_dupes,
            completion.complete_dupes,
        ),
    ]
    return [
        CommandSpec("/help", "/help", "Show helper command reference.", _bind(engine, cmd_help)),
        CommandSpec("/schema", "/schema", "Show dataset schema.", _bind(engine, cmd_schema)),
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
            "/hist",
            "/hist <numeric_col> [bins] [| where]",
            "Histogram bins for a numeric column.",
            _bind(engine, cmd_hist),
            completion.complete_hist,
        ),
        CommandSpec(
            "/crosstab",
            "/crosstab <col_a> <col_b> [n] [| where]",
            "Top value pairs by frequency.",
            _bind(engine, cmd_crosstab),
            completion.complete_crosstab,
        ),
        CommandSpec(
            "/corr",
            "/corr <numeric_x> <numeric_y> [| where]",
            "Correlation and non-null pair count.",
            _bind(engine, cmd_corr),
            completion.complete_corr,
        ),
        CommandSpec(
            "/profile",
            "/profile <column>",
            "Profile a single column.",
            _bind(engine, cmd_profile),
            completion.complete_profile,
        ),
        CommandSpec(
            "/describe",
            "/describe",
            "Describe columns and nulls.",
            _bind(engine, cmd_describe),
        ),
        CommandSpec(
            "/summary",
            "/summary [n_cols] [| where]",
            "Show per-column summary statistics.",
            _bind(engine, cmd_summary),
            completion.complete_summary,
        ),
        CommandSpec(
            "/history",
            "/history [n]",
            "Show recent query history.",
            _bind(engine, cmd_history),
            completion.complete_history,
        ),
        CommandSpec(
            "/rerun",
            "/rerun <history_index>",
            "Rerun a query from history.",
            _bind(engine, cmd_rerun),
            completion.complete_rerun,
        ),
        CommandSpec(
            "/rows",
            "/rows <n>",
            "Set row display limit.",
            _bind(engine, cmd_rows),
            completion.complete_rows,
        ),
        CommandSpec(
            "/values",
            "/values <n>",
            "Set max display length per value.",
            _bind(engine, cmd_values),
            completion.complete_values,
        ),
        CommandSpec(
            "/limit",
            "/limit <n>",
            "Set helper query + row display limits.",
            _bind(engine, cmd_limit),
            completion.complete_limit,
        ),
        CommandSpec(
            "/save",
            "/save <path.csv|path.parquet|path.json>",
            "Save latest result to disk.",
            _bind(engine, cmd_save),
            completion.complete_save,
        ),
        CommandSpec("/last", "/last", "Load previous SQL into editor.", _bind(engine, cmd_last)),
        CommandSpec("/clear", "/clear", "Clear query editor.", _bind(engine, cmd_clear)),
        CommandSpec(
            "/exit",
            "/exit or /quit",
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
