import re
from typing import Callable, Literal, Sequence

from sqlglot.tokens import Tokenizer as SqlglotTokenizer

from .completion_interfaces import CommandSpecLike, CompletionCatalogSource, CompletionEngineSource
from .completion_types import (
    AGGREGATE_FUNCTIONS,
    IDENT_PREFIX_RE,
    QUOTED_PREFIX_RE,
    SQL_KEYWORDS,
    CompletionContext,
    CompletionItem,
    CompletionKind,
    CompletionResult,
    SqlClause,
)
from .completion_utils import (
    is_numeric_type,
    is_simple_ident,
    parse_single_positive_int_arg,
    quote_ident,
)


def _is_temporal_type(type_name: str) -> bool:
    upper = type_name.upper()
    return any(marker in upper for marker in ("DATE", "TIME", "TIMESTAMP", "INTERVAL"))


def _is_text_type(type_name: str) -> bool:
    upper = type_name.upper()
    return any(marker in upper for marker in ("CHAR", "TEXT", "STRING", "VARCHAR", "UUID", "JSON"))


class EngineCompletionCatalog:
    def __init__(self, engine: CompletionCatalogSource) -> None:
        self._engine = engine
        self.clear_cache()

    @property
    def columns(self) -> list[str]:
        return self._engine.columns

    @property
    def column_types(self) -> dict[str, str]:
        return self._engine.column_types

    @property
    def table_name(self) -> str:
        return self._engine.table_name

    @property
    def default_limit(self) -> int:
        return self._engine.default_limit

    @property
    def executed_sql(self) -> list[str]:
        return self._engine.executed_sql

    @property
    def _command_specs(self) -> Sequence[CommandSpecLike]:
        return self._engine.command_specs()

    def _lookup_command(self, raw_name: str) -> CommandSpecLike | None:
        return self._engine.lookup_command(raw_name)

    def clear_cache(self) -> None:
        self._column_completion_cache: list[CompletionItem] | None = None
        self._aggregate_completion_cache: list[CompletionItem] | None = None
        self._aggregate_arg_completion_cache: dict[str, list[CompletionItem]] = {}
        self._predicate_completion_cache: list[CompletionItem] | None = None
        self._direction_completion_cache: list[CompletionItem] | None = None
        self._sql_clause_completion_cache: dict[str, list[CompletionItem]] = {}

    def _column_expr(self, column: str) -> str:
        return column if is_simple_ident(column) else quote_ident(column)

    def _base_completion_item(
        self,
        value: str,
        kind: CompletionKind,
        detail: str = "",
        usage: str = "",
        score: int = 0,
    ) -> CompletionItem:
        return CompletionItem(label=value, insert_text=value, kind=kind, detail=detail, usage=usage, score=score)

    @staticmethod
    def _include_all_columns(_column: str, _type_name: str) -> bool:
        return True

    @staticmethod
    def _include_numeric_columns(_column: str, type_name: str) -> bool:
        return is_numeric_type(type_name)

    def _build_column_completion_items(
        self,
        *,
        include_column: Callable[[str, str], bool],
        base_score: int,
        quoted_score: int,
    ) -> list[CompletionItem]:
        items: list[CompletionItem] = []
        seen: set[str] = set()
        for column in self.columns:
            type_name = self.column_types[column]
            if not include_column(column, type_name):
                continue
            expr = self._column_expr(column)
            expr_key = expr.casefold()
            if expr_key in seen:
                continue
            seen.add(expr_key)
            items.append(self._base_completion_item(expr, "column", type_name, score=base_score))
            if not is_simple_ident(column):
                continue
            quoted = quote_ident(column)
            quoted_key = quoted.casefold()
            if quoted_key in seen:
                continue
            seen.add(quoted_key)
            items.append(
                self._base_completion_item(
                    quoted,
                    "column",
                    f"{type_name} (quoted)",
                    score=quoted_score,
                )
            )
        return items

    def _column_completion_items(self) -> list[CompletionItem]:
        if self._column_completion_cache is not None:
            return self._column_completion_cache
        items = self._build_column_completion_items(
            include_column=self._include_all_columns,
            base_score=120,
            quoted_score=112,
        )
        self._column_completion_cache = items
        return items

    def _numeric_column_completion_items(self) -> list[CompletionItem]:
        return self._build_column_completion_items(
            include_column=self._include_numeric_columns,
            base_score=125,
            quoted_score=117,
        )

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
            if is_numeric_type(type_name):
                return score + 42
            return score - 46
        if function in {"MIN", "MAX"}:
            if is_numeric_type(type_name):
                return score + 34
            if _is_temporal_type(type_name):
                return score + 30
            if _is_text_type(type_name):
                return score + 8
            return score + 4
        if function == "COUNT":
            if is_numeric_type(type_name):
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

            if is_simple_ident(column):
                quoted = quote_ident(column)
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
        suffix = " (non-numeric fallback)" if func_name in {"SUM", "AVG"} and not is_numeric_type(column_type) else ""
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

    def _where_filter_completion_item(self) -> CompletionItem:
        return self._base_completion_item("| ", "snippet", "add where filter", score=82)

    def _merge_with_where_filter_completion(self, items: list[CompletionItem]) -> list[CompletionItem]:
        return self._merge_completion_items(items, [self._where_filter_completion_item()])

    def _prefixed_column_completion_items(self, prefix: str) -> list[CompletionItem]:
        return [
            self._base_completion_item(f"{prefix}{item.insert_text}", "column", item.detail, score=item.score)
            for item in self._column_completion_items()
        ]

    def _complete_top(self, args: str, trailing_space: bool) -> list[CompletionItem]:
        parts = args.split()
        if not parts or (len(parts) == 1 and not trailing_space):
            return self._column_completion_items()
        if len(parts) in {1, 2}:
            return self._numeric_completion_items([10, self.default_limit, 25, 50], detail="top rows")
        return []

    def _complete_dupes(self, args: str, trailing_space: bool) -> list[CompletionItem]:
        if "|" in args:
            return self._predicate_completion_items()

        base = args.strip()
        if not base.strip():
            return self._column_completion_items()

        if trailing_space:
            return self._merge_with_where_filter_completion(
                self._numeric_completion_items([self.default_limit, 10, 25, 50], detail="duplicate rows")
            )

        tokens = base.split()
        if len(tokens) >= 2:
            return self._numeric_completion_items([self.default_limit, 10, 25, 50], detail="duplicate rows")

        token = tokens[0]
        if "," in token:
            prefix, _, _ = token.rpartition(",")
            prefix = f"{prefix}," if prefix else ""
            return self._prefixed_column_completion_items(prefix)
        return self._column_completion_items()

    def _complete_summary(self, args: str, trailing_space: bool) -> list[CompletionItem]:
        if "|" in args:
            return self._predicate_completion_items()

        count_part = args.strip()
        items = self._numeric_completion_items(
            [len(self.columns), self.default_limit, 10, 25, 50],
            detail="summary column count",
        )
        if not count_part.strip():
            return items
        if trailing_space and parse_single_positive_int_arg(count_part) is not None:
            return self._merge_with_where_filter_completion(items)
        return items

    def _complete_hist(self, args: str, trailing_space: bool) -> list[CompletionItem]:
        if "|" in args:
            return self._predicate_completion_items()

        base = args.strip()
        parts = base.split()
        if not parts or (len(parts) == 1 and not trailing_space):
            return self._numeric_column_completion_items()

        if len(parts) == 1 and trailing_space:
            return self._merge_with_where_filter_completion(
                self._numeric_completion_items([10, 20, 30, 50], detail="histogram bins")
            )
        if len(parts) == 2:
            return self._numeric_completion_items([10, 20, 30, 50], detail="histogram bins")
        return []

    def _complete_crosstab(self, args: str, trailing_space: bool) -> list[CompletionItem]:
        if "|" in args:
            return self._predicate_completion_items()

        base = args.strip()
        parts = base.split()
        if len(parts) <= 1:
            return self._column_completion_items()
        if len(parts) == 2 and trailing_space:
            return self._merge_with_where_filter_completion(
                self._numeric_completion_items([self.default_limit, 10, 25, 50], detail="top pairs")
            )
        if len(parts) == 2:
            return self._column_completion_items()
        if len(parts) == 3:
            return self._numeric_completion_items([self.default_limit, 10, 25, 50], detail="top pairs")
        return []

    def _complete_corr(self, args: str, trailing_space: bool) -> list[CompletionItem]:
        if "|" in args:
            return self._predicate_completion_items()

        parts = args.strip().split()
        if len(parts) <= 1:
            return self._numeric_column_completion_items()
        if len(parts) == 2 and trailing_space:
            return [self._where_filter_completion_item()]
        if len(parts) == 2:
            return self._numeric_column_completion_items()
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

    def complete_sample(self, args: str, trailing_space: bool) -> list[CompletionItem]:
        return self._complete_sample(args, trailing_space)

    def complete_filter(self, args: str, trailing_space: bool) -> list[CompletionItem]:
        return self._complete_filter(args, trailing_space)

    def complete_sort(self, args: str, trailing_space: bool) -> list[CompletionItem]:
        return self._complete_sort(args, trailing_space)

    def complete_group(self, args: str, trailing_space: bool) -> list[CompletionItem]:
        return self._complete_group(args, trailing_space)

    def complete_agg(self, args: str, trailing_space: bool) -> list[CompletionItem]:
        return self._complete_agg(args, trailing_space)

    def complete_top(self, args: str, trailing_space: bool) -> list[CompletionItem]:
        return self._complete_top(args, trailing_space)

    def complete_dupes(self, args: str, trailing_space: bool) -> list[CompletionItem]:
        return self._complete_dupes(args, trailing_space)

    def complete_hist(self, args: str, trailing_space: bool) -> list[CompletionItem]:
        return self._complete_hist(args, trailing_space)

    def complete_crosstab(self, args: str, trailing_space: bool) -> list[CompletionItem]:
        return self._complete_crosstab(args, trailing_space)

    def complete_corr(self, args: str, trailing_space: bool) -> list[CompletionItem]:
        return self._complete_corr(args, trailing_space)

    def complete_profile(self, args: str, trailing_space: bool) -> list[CompletionItem]:
        return self._complete_profile(args, trailing_space)

    def complete_summary(self, args: str, trailing_space: bool) -> list[CompletionItem]:
        return self._complete_summary(args, trailing_space)

    def complete_history(self, args: str, trailing_space: bool) -> list[CompletionItem]:
        return self._complete_history(args, trailing_space)

    def complete_rerun(self, args: str, trailing_space: bool) -> list[CompletionItem]:
        return self._complete_rerun(args, trailing_space)

    def complete_rows(self, args: str, trailing_space: bool) -> list[CompletionItem]:
        return self._complete_rows(args, trailing_space)

    def complete_values(self, args: str, trailing_space: bool) -> list[CompletionItem]:
        return self._complete_values(args, trailing_space)

    def complete_limit(self, args: str, trailing_space: bool) -> list[CompletionItem]:
        return self._complete_limit(args, trailing_space)

    def complete_save(self, args: str, trailing_space: bool) -> list[CompletionItem]:
        return self._complete_save(args, trailing_space)

    def helper_command_completion_items(self) -> list[CompletionItem]:
        items: list[CompletionItem] = []
        seen: set[str] = set()
        for spec in self._command_specs:
            for raw_name in (spec.name, *spec.aliases):
                key = raw_name.casefold()
                if key in seen:
                    continue
                seen.add(key)
                items.append(
                    self._base_completion_item(
                        raw_name,
                        "helper_command",
                        spec.description,
                        usage=spec.usage,
                        score=180,
                    )
                )
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
        return self._merge_completion_items(
            self._sql_keyword_items(SQL_KEYWORDS, score=90),
            [self._base_completion_item(self.table_name, "table", "active table/view", score=95)],
            self._column_completion_items(),
            self._aggregate_completion_items(),
        )

    def _cache_clause_completion_items(self, clause_key: str, items: list[CompletionItem]) -> list[CompletionItem]:
        self._sql_clause_completion_cache[clause_key] = items
        return items

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
            return self._cache_clause_completion_items(clause_key, items)
        if clause_key in {"from", "join"}:
            items = self._merge_completion_items(
                table_items,
                self._sql_keyword_items(
                    ["JOIN", "LEFT JOIN", "RIGHT JOIN", "INNER JOIN", "WHERE", "GROUP BY", "ORDER BY", "LIMIT"],
                    score=118,
                ),
            )
            return self._cache_clause_completion_items(clause_key, items)
        if clause_key in {"where", "having", "join_on"}:
            items = self._merge_completion_items(
                predicates,
                self._sql_keyword_items(["AND", "OR", "NOT", "IN", "IS", "NULL", "LIKE", "BETWEEN"], score=116),
                aggregate_items if clause_key == "having" else [],
            )
            return self._cache_clause_completion_items(clause_key, items)
        if clause_key == "group_by":
            items = self._merge_completion_items(
                columns,
                self._sql_keyword_items(["HAVING", "ORDER BY", "LIMIT"], score=115),
            )
            return self._cache_clause_completion_items(clause_key, items)
        if clause_key == "order_by":
            items = self._merge_completion_items(
                self._with_direction_completion_items(),
                self._sql_keyword_items(["ASC", "DESC", "LIMIT"], score=114),
            )
            return self._cache_clause_completion_items(clause_key, items)
        if clause_key == "limit":
            items = self._merge_completion_items(
                numeric_values,
                self._sql_keyword_items(["OFFSET"], score=110),
            )
            return self._cache_clause_completion_items(clause_key, items)
        items = self._default_sql_completion_items()
        return self._cache_clause_completion_items(clause_key, items)

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


class CompletionEngine:
    def __init__(self, engine: CompletionEngineSource) -> None:
        self._engine = engine
        self._sqlglot_tokenizer = SqlglotTokenizer()
        self._completion_cache: dict[tuple[str, int, int, int], CompletionResult] = {}
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
            elif token == "WHERE":
                clause = "where"
            elif token == "HAVING":
                clause = "having"
            elif token == "LIMIT":
                clause = "limit"
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
        match = IDENT_PREFIX_RE.search(token_source)
        if match is None:
            return None
        function_name = match.group(1).upper()
        if function_name not in AGGREGATE_FUNCTIONS:
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
        prefix_match = QUOTED_PREFIX_RE.search(before) or IDENT_PREFIX_RE.search(before)
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
                usage=candidate.usage,
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

    @staticmethod
    def _should_auto_open(context: CompletionContext, matched: list[CompletionItem]) -> tuple[bool, str]:
        if not matched:
            return False, "no-matches"
        if context.mode == "helper":
            if context.completing_command_name:
                return True, "helper-command"
            return True, "helper-args"
        if context.inside_function_args:
            return True, "sql-function-args"
        if context.prefix:
            return True, "sql-prefix"
        if context.line_before_cursor.endswith((" ", "\t")):
            return True, "sql-trailing-space"
        return False, "sql-no-trigger"

    @staticmethod
    def _empty_result(
        *,
        context_mode: Literal["sql", "helper"] | None,
        reason: str,
    ) -> CompletionResult:
        return CompletionResult(items=[], should_auto_open=False, context_mode=context_mode, reason=reason)

    def _cache_result(
        self,
        cache_key: tuple[str, int, int, int],
        result: CompletionResult,
    ) -> CompletionResult:
        self._completion_cache[cache_key] = result
        if len(self._completion_cache) > self._completion_cache_limit:
            oldest_key = next(iter(self._completion_cache))
            self._completion_cache.pop(oldest_key)
        return result

    def _helper_candidates(self, context: CompletionContext) -> tuple[list[CompletionItem] | None, str | None]:
        if context.completing_command_name:
            return self._engine.helper_command_completion_items(), None
        if context.helper_command is None:
            return None, "missing-command"

        candidates = self._engine.helper_argument_completion_items(
            context.helper_command,
            context.helper_args,
            context.helper_has_trailing_space,
        )
        if not candidates and not self._engine.has_helper_command(context.helper_command):
            candidates = self._engine.helper_command_completion_items()
        return candidates, None

    def _sql_candidates(self, context: CompletionContext) -> list[CompletionItem]:
        sql_candidates = self._engine.sql_completion_items_for_clause(context.sql_clause)
        if context.inside_function_args and context.sql_function is not None:
            function_candidates = self._engine.sql_completion_items_for_function_args(context.sql_function)
            if function_candidates:
                return function_candidates
        return sql_candidates

    def get_result(self, text: str, cursor_location: tuple[int, int]) -> CompletionResult:
        cache_key = (text, cursor_location[0], cursor_location[1], self._acceptance_revision)
        cached = self._completion_cache.get(cache_key)
        if cached is not None:
            return cached

        context = self._build_context(text, cursor_location)
        if context is None:
            return self._cache_result(cache_key, self._empty_result(context_mode=None, reason="no-context"))

        matched: list[CompletionItem]
        if context.mode == "helper":
            candidates, helper_reason = self._helper_candidates(context)
            if candidates is None:
                return self._cache_result(
                    cache_key,
                    self._empty_result(context_mode="helper", reason=helper_reason or "missing-command"),
                )
            matched = self._match_candidates(
                candidates,
                context.prefix,
                context.replacement_start,
                context.replacement_end,
            )
        else:
            sql_candidates = self._sql_candidates(context)
            matched = self._match_candidates(
                sql_candidates,
                context.prefix,
                context.replacement_start,
                context.replacement_end,
            )

        should_auto_open, reason = self._should_auto_open(context, matched)
        result = CompletionResult(
            items=matched,
            should_auto_open=should_auto_open,
            context_mode=context.mode,
            reason=reason,
        )
        return self._cache_result(cache_key, result)

    def get_items(self, text: str, cursor_location: tuple[int, int]) -> list[CompletionItem]:
        return self.get_result(text, cursor_location).items
