from typing import Callable, Protocol, Sequence

from sqlexplore.core.engine_models import QueryHistoryEntry

from .models import CompletionItem, SqlClause


class CommandSpecLike(Protocol):
    name: str
    usage: str
    description: str
    aliases: tuple[str, ...]
    completer: Callable[[str, bool], list[CompletionItem]] | None


class CompletionCatalogSource(Protocol):
    columns: list[str]
    column_types: dict[str, str]
    table_name: str
    table_names: tuple[str, ...]
    default_limit: int
    executed_sql: list[str]
    query_history: list[QueryHistoryEntry]

    def command_specs(self) -> Sequence[CommandSpecLike]: ...

    def lookup_command(self, raw_name: str) -> CommandSpecLike | None: ...


class CompletionEngineSource(Protocol):
    def helper_command_completion_items(self) -> list[CompletionItem]: ...

    def helper_argument_completion_items(
        self,
        command_name: str,
        args: str,
        trailing_space: bool,
    ) -> list[CompletionItem]: ...

    def has_helper_command(self, raw_name: str) -> bool: ...

    def sql_completion_items_for_clause(self, clause: SqlClause) -> list[CompletionItem]: ...

    def sql_completion_items_for_function_args(self, function_name: str) -> list[CompletionItem]: ...
