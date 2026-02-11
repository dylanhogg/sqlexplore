from __future__ import annotations

from pathlib import Path

from sqlexplore.engine import SqlExplorerEngine


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


def test_group_shorthand_generates_count_query(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        out = engine.run_input("/group col_name")
        assert out.status == "ok"
        assert out.generated_sql is not None
        assert out.result is not None
        assert 'COUNT(*) AS count FROM "data"' in out.generated_sql
        assert [tuple(row) for row in out.result.rows] == [("a", 2), ("b", 1)]
    finally:
        engine.close()


def test_group_pipe_syntax_still_works(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        out = engine.run_input("/group col_name | SUM(x) AS total")
        assert out.status == "ok"
        assert out.generated_sql is not None
        assert out.result is not None
        assert "SUM(x) AS total" in out.generated_sql
        assert [tuple(row) for row in out.result.rows] == [("a", 4), ("b", 2)]
    finally:
        engine.close()


def test_help_text_is_generated_from_command_registry(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        help_text = engine.help_text()
        assert "/sample [n]" in help_text
        assert "/group <group cols> | <aggregates> [| having]" in help_text
        assert "/save <path.csv|path.parquet|path.json>" in help_text
        assert "/exit or /quit" in help_text
    finally:
        engine.close()


def test_helper_completion_suggests_command_names(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        completions = _completion_values(engine, "/to")
        assert "/top" in completions
    finally:
        engine.close()


def test_top_completion_suggests_columns_after_command(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        completions = _completion_values(engine, "/top ")
        assert "col_name" in completions
        assert "x" in completions
    finally:
        engine.close()


def test_group_completion_suggests_aggregates_after_pipe(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        completions = _completion_values(engine, "/group col_name | ")
        assert "COUNT(*) AS count" in completions
    finally:
        engine.close()


def test_sql_select_context_suggests_columns_and_functions(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        completions = _completion_values(engine, "SELECT ")
        assert "col_name" in completions
        assert "COUNT(*) AS count" in completions
    finally:
        engine.close()


def test_sql_from_context_suggests_active_table(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        completions = _completion_values(engine, "SELECT col_name FROM ")
        assert "data" in completions
        assert "JOIN" in completions
    finally:
        engine.close()


def test_sql_where_context_suggests_predicates(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        completions = _completion_values(engine, "SELECT * FROM data WHERE ")
        assert "col_name IS NOT NULL" in completions
        assert "AND" in completions
    finally:
        engine.close()


def test_sql_group_by_context_suggests_having_and_columns(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        completions = _completion_values(engine, "SELECT col_name FROM data GROUP BY ")
        assert "col_name" in completions
        assert "HAVING" in completions
    finally:
        engine.close()


def test_sql_order_by_context_suggests_directional_snippets(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        completions = _completion_values(engine, "SELECT * FROM data ORDER BY ")
        assert "col_name DESC" in completions
        assert "x ASC" in completions
    finally:
        engine.close()


def test_sql_limit_context_suggests_numeric_values(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        completions = _completion_values(engine, "SELECT * FROM data LIMIT ")
        assert "10" in completions
        assert "25" in completions
    finally:
        engine.close()


def test_sql_inside_literal_does_not_suggest(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        completions = _completion_values(engine, "SELECT 'unterminated")
        assert completions == []
    finally:
        engine.close()


def test_sql_quoted_prefix_suggests_quoted_simple_identifier(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        completions = _completion_values(engine, 'SELECT "co')
        assert '"col_name"' in completions
    finally:
        engine.close()


def test_accepted_completion_is_ranked_higher_on_next_lookup(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        before = _completion_values(engine, "/s")
        before_index = before.index("/sort")

        for _ in range(4):
            engine.record_completion_acceptance("/sort")

        after = _completion_values(engine, "/s")
        after_index = after.index("/sort")
        assert after_index <= before_index
        assert after[0] == "/sort"
    finally:
        engine.close()


def test_aggregate_argument_provider_prefers_numeric_for_sum(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        completions = [item.insert_text for item in engine.sql_completion_items_for_function_args("SUM")]
        assert "x" in completions
        assert "col_name" in completions
        assert completions.index("x") < completions.index("col_name")
        assert "value" not in completions
    finally:
        engine.close()


def test_aggregate_argument_provider_count_includes_star_and_distinct(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        completions = [item.insert_text for item in engine.sql_completion_items_for_function_args("COUNT")]
        assert "*" in completions
        assert "col_name" in completions
        assert "DISTINCT col_name" in completions
    finally:
        engine.close()


def test_aggregate_snippets_use_schema_based_columns(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        items = engine.completion_items("/group col_name | ", (0, len("/group col_name | ")))
        completions = [item.insert_text for item in items]
        assert "COUNT(*) AS count" in completions
        assert "SUM(x) AS sum_x" in completions
        assert "AVG(x) AS avg_x" in completions
        assert "MIN(x) AS min_x" in completions
        assert "MAX(x) AS max_x" in completions
        assert all("value" not in completion.casefold() for completion in completions)
    finally:
        engine.close()


def test_aggregate_snippets_non_numeric_fallback_is_explicit(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path, csv_text="city,name\nseattle,alice\nmiami,bob\n")
    try:
        items = engine.completion_items("/group city | ", (0, len("/group city | ")))
        sum_item = next(item for item in items if item.insert_text.startswith("SUM("))
        avg_item = next(item for item in items if item.insert_text.startswith("AVG("))
        assert sum_item.insert_text == "SUM(city) AS sum_city"
        assert avg_item.insert_text == "AVG(city) AS avg_city"
        assert "non-numeric fallback" in sum_item.detail
        assert "non-numeric fallback" in avg_item.detail
    finally:
        engine.close()


def test_sql_max_argument_context_routes_to_aggregate_argument_candidates(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        completions = _completion_values(engine, "SELECT MAX(")
        assert "x" in completions
        assert "col_name" in completions
        assert "MAX(value)" not in completions
        assert "COUNT(*) AS count" not in completions
        assert "SUM(x) AS sum_x" not in completions
    finally:
        engine.close()


def test_sql_sum_argument_context_ranks_numeric_before_text(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        completions = _completion_values(engine, "SELECT SUM(")
        assert "x" in completions
        assert "col_name" in completions
        assert completions.index("x") < completions.index("col_name")
    finally:
        engine.close()


def test_sql_count_argument_context_includes_star_and_distinct(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        completions = _completion_values(engine, "SELECT COUNT(")
        assert "*" in completions
        assert "col_name" in completions
        assert "DISTINCT col_name" in completions
        assert "COUNT(*) AS count" not in completions
    finally:
        engine.close()


def test_sql_max_argument_prefix_filters_to_matching_column(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        completions = _completion_values(engine, "SELECT MAX(co")
        assert "col_name" in completions
        assert "x" not in completions
    finally:
        engine.close()


def test_sql_max_argument_quoted_prefix_filters_to_matching_column(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        completions = _completion_values(engine, 'SELECT MAX("co')
        assert '"col_name"' in completions
        assert "x" not in completions
    finally:
        engine.close()


def test_result_columns_strip_duckdb_function_schema_prefix(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path, csv_text="synthetic_address\n  123 Main St  \n")
    try:
        out = engine.run_sql('SELECT trim(synthetic_address), * FROM "data" LIMIT 1')
        assert out.status == "ok"
        assert out.result is not None
        assert out.result.columns == ["trim(synthetic_address)", "synthetic_address"]
    finally:
        engine.close()
