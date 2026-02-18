from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Sequence

import pytest

from sqlexplore.completion.completions import EngineCompletionCatalog
from sqlexplore.completion.models import CompletionItem
from sqlexplore.completion.protocols import CommandSpecLike
from sqlexplore.core.engine import SqlExplorerEngine
from sqlexplore.core.engine_models import QueryHistoryEntry


def _build_engine(tmp_path: Path, csv_text: str = "col_name,x\na,1\nb,2\na,3\n") -> SqlExplorerEngine:
    csv_path = tmp_path / "data.csv"
    csv_path.write_text(csv_text, encoding="utf-8")
    return SqlExplorerEngine(
        data_path=csv_path,
        table_name="data",
        database=":memory:",
        default_limit=10,
        max_rows_display=100,
        max_value_chars=80,
    )


def _completion_values(engine: SqlExplorerEngine, text: str) -> list[str]:
    items = engine.completion_items(text, (0, len(text)))
    return [item.insert_text for item in items]


def test_dupes_completion_handles_pipe_prefixed_csv_and_inline_limit(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        prefixed = _completion_values(engine, "/dupes col_name,")
        assert any(item.startswith("col_name,") for item in prefixed)

        inline_limit = _completion_values(engine, "/dupes col_name 1")
        assert "10" in inline_limit

        with_pipe = _completion_values(engine, "/dupes col_name | ")
        assert "col_name IS NOT NULL" in with_pipe
    finally:
        engine.close()


def test_crosstab_completion_handles_no_trailing_space_and_pipe(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        second_column = _completion_values(engine, "/crosstab col_name c")
        assert "col_name" in second_column
        assert "10" not in second_column

        with_explicit_limit = _completion_values(engine, "/crosstab col_name x 1")
        assert "10" in with_explicit_limit

        with_pipe = _completion_values(engine, "/crosstab col_name x | ")
        assert "col_name IS NOT NULL" in with_pipe
    finally:
        engine.close()


def test_corr_completion_handles_where_gate_and_invalid_extra_tokens(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        where_gate = _completion_values(engine, "/corr x y ")
        assert "| " in where_gate

        second_numeric = _completion_values(engine, '/corr x "x')
        assert '"x"' in second_numeric
        assert "| " not in second_numeric

        with_pipe = _completion_values(engine, "/corr x y | ")
        assert "col_name IS NOT NULL" in with_pipe

        too_many_args = _completion_values(engine, "/corr x y z ")
        assert too_many_args == []
    finally:
        engine.close()


def test_rerun_completion_returns_default_when_history_empty(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        result = engine.completion_result("/rerun ", (0, len("/rerun ")))
        assert [item.insert_text for item in result.items] == ["1"]
        assert result.items[0].detail == "history index"
    finally:
        engine.close()


def test_rerun_completion_lists_recent_queries_and_truncates_details(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        for idx in range(12):
            out = engine.run_sql(f"SELECT {idx} AS value")
            assert out.status == "ok"
        long_sql = "SELECT 999 AS value\n-- " + ("x" * 80)
        out = engine.run_sql(long_sql)
        assert out.status == "ok"

        result = engine.completion_result("/rerun ", (0, len("/rerun ")))
        items = result.items
        history_len = len(engine.query_history)
        expected_recent = {str(idx) for idx in range(history_len, history_len - 10, -1)}

        assert len(items) == 10
        assert {item.insert_text for item in items} == expected_recent
        latest = next(item for item in items if item.insert_text == str(history_len))
        assert "\n" not in latest.detail
        assert latest.detail.endswith("...")
    finally:
        engine.close()


@dataclass(slots=True)
class _FakeCommandSpec:
    name: str
    usage: str
    description: str
    aliases: tuple[str, ...] = ()
    completer: Callable[[str, bool], list[CompletionItem]] | None = None


class _FakeCatalogSource:
    def __init__(self) -> None:
        self.columns = ["city", "City", "x"]
        self.column_types = {"city": "VARCHAR", "City": "VARCHAR", "x": "INTEGER"}
        self.table_name = "data"
        self.table_names: tuple[str, ...] = ("data",)
        self.default_limit = 10
        self.executed_sql: list[str] = []
        self.query_history: list[QueryHistoryEntry] = []
        self._specs: list[CommandSpecLike] = [
            _FakeCommandSpec(name="/help", usage="/help", description="help", aliases=("/HELP",)),
            _FakeCommandSpec(name="/sample", usage="/sample [n]", description="sample"),
        ]

    def command_specs(self) -> Sequence[CommandSpecLike]:
        return self._specs

    def lookup_command(self, raw_name: str) -> CommandSpecLike | None:
        key = raw_name.casefold()
        for spec in self._specs:
            if key == spec.name.casefold():
                return spec
            if any(key == alias.casefold() for alias in spec.aliases):
                return spec
        return None


def test_completion_tokens_are_case_insensitive_deduplicated() -> None:
    catalog = EngineCompletionCatalog(_FakeCatalogSource())
    tokens = catalog.completion_tokens()

    assert len(tokens) == len({token.casefold() for token in tokens})
    assert "/help" in tokens
    assert "/HELP" not in tokens
    assert "city" in tokens
    assert "City" not in tokens
    assert sum(token.casefold() == "data" for token in tokens) == 1


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT SUM(1 -- ignored )\n + 2",
        "SELECT SUM('(' || ')' || 2",
        "SELECT SUM(1 /* outer ( /* inner ) */ still ) */ + 2",
    ],
)
def test_sum_function_context_ignores_comments_and_literals(tmp_path: Path, sql: str) -> None:
    engine = _build_engine(tmp_path)
    try:
        result = engine.completion_result(sql, (0, len(sql)))
        completions = [item.insert_text for item in result.items]
        assert result.context_mode == "sql"
        assert result.should_auto_open is True
        assert "x" in completions
        assert "col_name" in completions
        assert "COUNT(*) AS count" not in completions
    finally:
        engine.close()


def test_balanced_parentheses_do_not_trigger_function_arg_context(tmp_path: Path) -> None:
    sql = "SELECT (1 + 2)"
    engine = _build_engine(tmp_path)
    try:
        result = engine.completion_result(sql, (0, len(sql)))
        completions = [item.insert_text for item in result.items]
        assert result.context_mode == "sql"
        assert result.should_auto_open is False
        assert result.reason == "sql-no-trigger"
        assert "COUNT(*) AS count" in completions
    finally:
        engine.close()


def test_latest_unmatched_paren_controls_function_arg_context(tmp_path: Path) -> None:
    sql = "SELECT SUM(1) + (2"
    engine = _build_engine(tmp_path)
    try:
        result = engine.completion_result(sql, (0, len(sql)))
        completions = [item.insert_text for item in result.items]
        assert result.context_mode == "sql"
        assert result.should_auto_open is False
        assert result.reason == "sql-no-trigger"
        assert "COUNT(*) AS count" in completions
    finally:
        engine.close()
