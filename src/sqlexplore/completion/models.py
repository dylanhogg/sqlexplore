import re
from dataclasses import dataclass
from typing import Literal

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
    "/dupes",
    "/hist",
    "/crosstab",
    "/corr",
    "/profile",
    "/describe",
    "/summary",
    "/history",
    "/history-log",
    "/rerun",
    "/rerun-log",
    "/rows",
    "/values",
    "/limit",
    "/save",
    "/last",
    "/clear",
    "/exit",
    "/quit",
    "/llm-history",
    "/llm-show",
)
HELPER_PREFIX_RE = re.compile(r"(?<!\S)(/[A-Za-z_]*)$")
IDENT_PREFIX_RE = re.compile(r"([A-Za-z_][A-Za-z0-9_$]*)$")
QUOTED_PREFIX_RE = re.compile(r'("(?:""|[^"])*)$')
SIMPLE_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
AGGREGATE_FUNCTIONS = frozenset({"COUNT", "SUM", "AVG", "MIN", "MAX"})


@dataclass(slots=True)
class CompletionItem:
    label: str
    insert_text: str
    kind: CompletionKind
    detail: str = ""
    usage: str = ""
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
class CompletionResult:
    items: list[CompletionItem]
    should_auto_open: bool
    context_mode: Literal["sql", "helper"] | None
    reason: str = ""
