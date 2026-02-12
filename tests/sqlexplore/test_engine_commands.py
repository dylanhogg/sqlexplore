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


def _completion_result(engine: SqlExplorerEngine, text: str):
    return engine.completion_result(text, (0, len(text)))


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
        assert "/top <column> <n>" in help_text
        assert "/dupes <key_cols_csv> [n] [| where]" in help_text
        assert "/hist <numeric_col> [bins] [| where]" in help_text
        assert "/crosstab <col_a> <col_b> [n] [| where]" in help_text
        assert "/corr <numeric_x> <numeric_y> [| where]" in help_text
        assert "/group <group cols> | <aggregates> [| having]" in help_text
        assert "/summary [n_cols] [| where]" in help_text
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


def test_helper_completion_suggests_summary_and_dupes_commands(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        summary_completions = _completion_values(engine, "/su")
        dupes_completions = _completion_values(engine, "/du")
        hist_completions = _completion_values(engine, "/hi")
        crosstab_completions = _completion_values(engine, "/cr")
        corr_completions = _completion_values(engine, "/co")
        assert "/summary" in summary_completions
        assert "/dupes" in dupes_completions
        assert "/hist" in hist_completions
        assert "/crosstab" in crosstab_completions
        assert "/corr" in corr_completions
    finally:
        engine.close()


def test_helper_completion_items_include_usage(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        items = engine.helper_command_completion_items()
        top_item = next(item for item in items if item.insert_text == "/top")
        assert top_item.detail == "Top values by frequency for a column."
        assert top_item.usage == "/top <column> <n>"
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


def test_dupes_completion_suggests_columns_then_limit(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        start = _completion_values(engine, "/dupes ")
        assert "col_name" in start

        after_key = _completion_values(engine, "/dupes col_name ")
        assert "10" in after_key
    finally:
        engine.close()


def test_summary_completion_suggests_counts_and_predicates(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        start = _completion_values(engine, "/summary ")
        assert "10" in start

        after_pipe = _completion_values(engine, "/summary 2 | ")
        assert "col_name IS NOT NULL" in after_pipe
    finally:
        engine.close()


def test_hist_completion_prefers_numeric_columns_and_bins(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        start = _completion_values(engine, "/hist ")
        assert "x" in start
        assert "col_name" not in start

        after_column = _completion_values(engine, "/hist x ")
        assert "10" in after_column
    finally:
        engine.close()


def test_crosstab_completion_suggests_columns_then_limit(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        start = _completion_values(engine, "/crosstab ")
        assert "col_name" in start
        assert "x" in start

        second = _completion_values(engine, "/crosstab col_name ")
        assert "x" in second

        after_second = _completion_values(engine, "/crosstab col_name x ")
        assert "10" in after_second
    finally:
        engine.close()


def test_corr_completion_prefers_numeric_columns(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        start = _completion_values(engine, "/corr ")
        assert "x" in start
        assert "col_name" not in start

        second = _completion_values(engine, "/corr x ")
        assert "x" in second
        assert "col_name" not in second
    finally:
        engine.close()


def test_top_command_requires_explicit_n(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        out = engine.run_input("/top col_name")
        assert out.status == "error"
        assert out.message == "Usage: /top <column> <n>"
    finally:
        engine.close()


def test_top_command_uses_supplied_n(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        out = engine.run_input("/top col_name 1")
        assert out.status == "ok"
        assert out.generated_sql is not None
        assert "LIMIT 1" in out.generated_sql
        assert out.result is not None
        assert len(out.result.rows) == 1
    finally:
        engine.close()


def test_dupes_command_finds_duplicate_key_rows(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        out = engine.run_input("/dupes col_name")
        assert out.status == "ok"
        assert out.generated_sql is not None
        assert "HAVING COUNT(*) > 1" in out.generated_sql
        assert out.result is not None
        assert out.result.columns == ["col_name", "count"]
        assert [tuple(row) for row in out.result.rows] == [("a", 2)]
    finally:
        engine.close()


def test_dupes_command_supports_multiple_keys_and_limit(tmp_path: Path) -> None:
    csv_text = "city,state,x\nseattle,wa,1\nseattle,wa,2\nseattle,or,3\nmiami,fl,4\nmiami,fl,5\n"
    engine = _build_engine(tmp_path, csv_text=csv_text)
    try:
        out = engine.run_input("/dupes city,state 1")
        assert out.status == "ok"
        assert out.generated_sql is not None
        assert "LIMIT 1" in out.generated_sql
        assert out.result is not None
        assert len(out.result.rows) == 1
        assert tuple(out.result.rows[0]) == ("miami", "fl", 2)
    finally:
        engine.close()


def test_dupes_command_supports_where_pipe_filter(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        out = engine.run_input("/dupes col_name | x > 2")
        assert out.status == "ok"
        assert out.generated_sql is not None
        assert "WHERE x > 2" in out.generated_sql
        assert out.result is not None
        assert out.result.rows == []
    finally:
        engine.close()


def test_dupes_command_requires_valid_arguments(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        out = engine.run_input("/dupes")
        assert out.status == "error"
        assert out.message == "Usage: /dupes <key_cols_csv> [n] [| where]"

        out = engine.run_input("/dupes col_name nope")
        assert out.status == "error"
        assert out.message == "Usage: /dupes <key_cols_csv> [n] [| where]"
    finally:
        engine.close()


def test_summary_command_returns_column_metrics(tmp_path: Path) -> None:
    csv_text = "city,score\nseattle,10\nseattle,20\n,30\n"
    engine = _build_engine(tmp_path, csv_text=csv_text)
    try:
        out = engine.run_input("/summary")
        assert out.status == "ok"
        assert out.result is not None
        assert out.result.columns == [
            "column",
            "type",
            "sample",
            "min",
            "max",
            "avg_len",
            "distinct",
            "nulls",
            "null_%",
        ]
        rows = {str(row[0]): tuple(row) for row in out.result.rows}
        city = rows["city"]
        score = rows["score"]
        assert city[2] == "seattle"
        assert city[3] == "seattle"
        assert city[4] == "seattle"
        assert city[6] == 1
        assert city[7] == 1
        assert city[8] == "33.33%"
        assert score[3] == "10"
        assert score[4] == "30"
        assert score[6] == 3
        assert score[7] == 0
        assert score[8] == "0.00%"
    finally:
        engine.close()


def test_summary_command_supports_column_limit_and_filter(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        out = engine.run_input("/summary 1 | x >= 2")
        assert out.status == "ok"
        assert out.result is not None
        assert len(out.result.rows) == 1
        assert out.message.endswith("with filter")
        assert tuple(out.result.rows[0])[:2] == ("col_name", "VARCHAR")
    finally:
        engine.close()


def test_summary_command_requires_valid_arguments(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        out = engine.run_input("/summary nope")
        assert out.status == "error"
        assert out.message == "Usage: /summary [n_cols] [| where]"

        out = engine.run_input("/summary 2 |")
        assert out.status == "error"
        assert out.message == "Usage: /summary [n_cols] [| where]"
    finally:
        engine.close()


def test_hist_command_generates_histogram_for_numeric_column(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        out = engine.run_input("/hist x 2")
        assert out.status == "ok"
        assert out.generated_sql is not None
        assert "bin_start" in out.generated_sql
        assert out.result is not None
        assert out.result.columns == ["bin_start", "bin_end", "count", "pct"]
        rows = [tuple(row) for row in out.result.rows]
        assert len(rows) == 2
        assert rows[0][2] == 1
        assert rows[1][2] == 2
        assert abs(float(rows[0][3]) - 33.33) <= 0.01
        assert abs(float(rows[1][3]) - 66.67) <= 0.01
    finally:
        engine.close()


def test_hist_command_supports_where_filter(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        out = engine.run_input("/hist x 2 | x >= 2")
        assert out.status == "ok"
        assert out.generated_sql is not None
        assert "(x >= 2) AND" in out.generated_sql
        assert out.result is not None
        rows = [tuple(row) for row in out.result.rows]
        assert len(rows) == 2
        assert rows[0][2] == 1
        assert rows[1][2] == 1
    finally:
        engine.close()


def test_hist_command_requires_valid_numeric_column(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        out = engine.run_input("/hist col_name")
        assert out.status == "error"
        assert out.message == "/hist requires numeric column: col_name"

        out = engine.run_input("/hist x nope")
        assert out.status == "error"
        assert out.message == "Usage: /hist <numeric_col> [bins] [| where]"
    finally:
        engine.close()


def test_crosstab_command_returns_top_pairs(tmp_path: Path) -> None:
    csv_text = "city,state,x\nseattle,wa,1\nseattle,wa,2\nseattle,or,3\nmiami,fl,4\nmiami,fl,5\n"
    engine = _build_engine(tmp_path, csv_text=csv_text)
    try:
        out = engine.run_input("/crosstab city state 2")
        assert out.status == "ok"
        assert out.generated_sql is not None
        assert "LIMIT 2" in out.generated_sql
        assert out.result is not None
        assert out.result.columns == ["city", "state", "count"]
        assert [tuple(row) for row in out.result.rows] == [("miami", "fl", 2), ("seattle", "wa", 2)]
    finally:
        engine.close()


def test_crosstab_command_supports_where_filter(tmp_path: Path) -> None:
    csv_text = "city,state,x\nseattle,wa,1\nseattle,wa,2\nseattle,or,3\nmiami,fl,4\nmiami,fl,5\n"
    engine = _build_engine(tmp_path, csv_text=csv_text)
    try:
        out = engine.run_input("/crosstab city state | x > 2")
        assert out.status == "ok"
        assert out.generated_sql is not None
        assert "WHERE x > 2" in out.generated_sql
        assert out.result is not None
        assert [tuple(row) for row in out.result.rows] == [("miami", "fl", 2), ("seattle", "or", 1)]
    finally:
        engine.close()


def test_crosstab_command_requires_valid_arguments(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        out = engine.run_input("/crosstab col_name")
        assert out.status == "error"
        assert out.message == "Usage: /crosstab <col_a> <col_b> [n] [| where]"

        out = engine.run_input("/crosstab col_name x nope")
        assert out.status == "error"
        assert out.message == "Usage: /crosstab <col_a> <col_b> [n] [| where]"

        out = engine.run_input("/crosstab nope x")
        assert out.status == "error"
        assert out.message == "Unknown column: nope"
    finally:
        engine.close()


def test_corr_command_returns_correlation_and_non_null_count(tmp_path: Path) -> None:
    csv_text = "x,y,label\n1,2,a\n2,4,b\n3,6,c\n4,,d\n,8,e\n"
    engine = _build_engine(tmp_path, csv_text=csv_text)
    try:
        out = engine.run_input("/corr x y")
        assert out.status == "ok"
        assert out.generated_sql is not None
        assert "CORR" in out.generated_sql
        assert out.result is not None
        assert out.result.columns == ["corr", "n_non_null"]
        row = tuple(out.result.rows[0])
        assert abs(float(row[0]) - 1.0) <= 1e-6
        assert row[1] == 3
    finally:
        engine.close()


def test_corr_command_supports_where_filter(tmp_path: Path) -> None:
    csv_text = "x,y,label\n1,2,a\n2,4,b\n3,6,c\n4,,d\n,8,e\n"
    engine = _build_engine(tmp_path, csv_text=csv_text)
    try:
        out = engine.run_input("/corr x y | x >= 2")
        assert out.status == "ok"
        assert out.generated_sql is not None
        assert "WHERE x >= 2" in out.generated_sql
        assert out.result is not None
        row = tuple(out.result.rows[0])
        assert abs(float(row[0]) - 1.0) <= 1e-6
        assert row[1] == 2
    finally:
        engine.close()


def test_corr_command_requires_numeric_columns_and_valid_args(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        out = engine.run_input("/corr x")
        assert out.status == "error"
        assert out.message == "Usage: /corr <numeric_x> <numeric_y> [| where]"

        out = engine.run_input("/corr x col_name")
        assert out.status == "error"
        assert out.message == "/corr requires numeric column: col_name"

        out = engine.run_input("/corr x nope")
        assert out.status == "error"
        assert out.message == "Unknown column: nope"
    finally:
        engine.close()


def test_completion_result_auto_opens_for_sql_clause_boundary(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        result = _completion_result(engine, "SELECT ")
        assert result.should_auto_open is True
        assert result.context_mode == "sql"
        assert any(item.insert_text == "col_name" for item in result.items)
    finally:
        engine.close()


def test_completion_result_auto_opens_for_helper_argument_boundary(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        result = _completion_result(engine, "/top ")
        assert result.should_auto_open is True
        assert result.context_mode == "helper"
        assert any(item.insert_text == "col_name" for item in result.items)
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


def test_completion_result_inside_literal_does_not_auto_open(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        result = _completion_result(engine, "SELECT 'unterminated")
        assert result.items == []
        assert result.should_auto_open is False
        assert result.context_mode is None
    finally:
        engine.close()


def test_sql_quoted_prefix_suggests_quoted_simple_identifier(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        completions = _completion_values(engine, 'SELECT "co')
        assert '"col_name"' in completions
    finally:
        engine.close()


def test_known_helper_argument_context_does_not_fallback_to_command_names(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        result = _completion_result(engine, "/top zzz")
        assert result.items == []
        assert result.should_auto_open is False
    finally:
        engine.close()


def test_unknown_helper_command_with_space_falls_back_to_command_names(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        result = _completion_result(engine, "/unknown ")
        completions = [item.insert_text for item in result.items]
        assert "/help" in completions
        assert result.should_auto_open is True
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
