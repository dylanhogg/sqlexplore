from dataclasses import dataclass
from typing import Any, Literal

ResultStatus = Literal["ok", "info", "sql", "error"]
HistoryQueryType = Literal[
    "user_entered_sql",
    "user_entered_command",
    "command_generated_sql",
    "llm_generated_sql",
]
HistoryQueryStatus = Literal["success", "error"]


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
class QueryHistoryEntry:
    query_text: str
    query_type: HistoryQueryType
    query_status: HistoryQueryStatus
