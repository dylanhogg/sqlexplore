import inspect
from pathlib import Path
from typing import Any

import sqlexplore.commands.handlers as command_handlers_module
import sqlexplore.commands.llm_runner as llm_runner_module
import sqlexplore.commands.registry as commands_module
import sqlexplore.completion.completions as completion_module
from sqlexplore.core.engine import (
    SqlExplorerEngine,
    flatten_struct_paths,
    is_struct_type,
    parse_struct_fields,
)
from sqlexplore.core.logging_utils import configure_file_logging, read_log_events_for_trace, reset_file_logging


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


def _reset_log_state() -> None:
    reset_file_logging()


def test_txt_data_source_loads_line_metrics_columns(tmp_path: Path) -> None:
    txt_path = tmp_path / "data.txt"
    txt_path.write_text("alpha,beta\n\nvalue\twith\ttabs\nplain text\n", encoding="utf-8")
    engine = SqlExplorerEngine(
        data_path=txt_path,
        table_name="data",
        database=":memory:",
        default_limit=10,
        max_rows_display=100,
        max_value_chars=80,
    )
    try:
        out = engine.run_sql('SELECT * FROM "data"')
        assert out.status == "ok"
        assert out.result is not None
        assert out.result.columns == [
            "line",
            "line_number",
            "line_length",
            "line_hash",
            "word_count",
            "mean_word_length",
            "median_word_length",
            "max_word_length",
            "min_word_length",
        ]
        rows = [tuple(row) for row in out.result.rows]
        assert [row[0] for row in rows] == ["alpha,beta", "", "value\twith\ttabs", "plain text"]
        assert [row[1] for row in rows] == [1, 2, 3, 4]
        assert [row[2] for row in rows] == [10, 0, 15, 10]
        assert all(isinstance(row[3], int) for row in rows)
        assert [row[4] for row in rows] == [1, 0, 3, 2]
        assert [round(float(row[5]), 6) for row in rows] == [10.0, 0.0, 4.333333, 4.5]
        assert [round(float(row[6]), 6) for row in rows] == [10.0, 0.0, 4.0, 4.5]
        assert [row[7] for row in rows] == [10, 0, 5, 5]
        assert [row[8] for row in rows] == [10, 0, 4, 4]
    finally:
        engine.close()


def test_txt_data_source_handles_literal_backslash_zero(tmp_path: Path) -> None:
    txt_path = tmp_path / "data.txt"
    txt_path.write_text("value\\0with,comma\nsecond line\n", encoding="utf-8")
    engine = SqlExplorerEngine(
        data_path=txt_path,
        table_name="data",
        database=":memory:",
        default_limit=10,
        max_rows_display=100,
        max_value_chars=80,
    )
    try:
        out = engine.run_sql('SELECT line, line_number FROM "data" ORDER BY line_number')
        assert out.status == "ok"
        assert out.result is not None
        assert [tuple(row) for row in out.result.rows] == [("value\\0with,comma", 1), ("second line", 2)]
    finally:
        engine.close()


def _completion_values(engine: SqlExplorerEngine, text: str) -> list[str]:
    items = engine.completion_items(text, (0, len(text)))
    return [item.insert_text for item in items]


def _completion_result(engine: SqlExplorerEngine, text: str):
    return engine.completion_result(text, (0, len(text)))


def test_completion_module_does_not_import_engine() -> None:
    source = inspect.getsource(completion_module)
    assert "from sqlexplore.core.engine import" not in source


def test_command_modules_do_not_import_engine() -> None:
    command_source = inspect.getsource(commands_module)
    handler_source = inspect.getsource(command_handlers_module)
    assert "from sqlexplore.core.engine import" not in command_source
    assert "from sqlexplore.core.engine import" not in handler_source


def test_struct_type_helpers_parse_nested_fields(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        relation = engine.conn.execute("SELECT {'a': 1, 'b': {'c': 2, 'd': 'x'}, 'zip': '98101'} AS s")
        dtype = relation.description[0][1]
        assert is_struct_type(dtype) is True
        assert is_struct_type(dtype.children[0][1]) is False

        fields = parse_struct_fields(dtype)
    finally:
        engine.close()

    assert [field.name for field in fields] == ["a", "b", "zip"]
    assert fields[0].type_name == "INTEGER"
    assert fields[1].type_name == "STRUCT(c INTEGER, d VARCHAR)"
    assert [child.name for child in fields[1].children] == ["c", "d"]
    assert fields[2].type_name == "VARCHAR"

    paths = flatten_struct_paths(fields)
    assert ("a", "INTEGER") in paths
    assert ("b", "STRUCT(c INTEGER, d VARCHAR)") in paths
    assert ("b.c", "INTEGER") in paths
    assert ("b.d", "VARCHAR") in paths
    assert ("zip", "VARCHAR") in paths


def test_refresh_schema_populates_struct_metadata_by_column(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        assert engine.struct_fields_by_column == {}
        assert engine.struct_paths_by_column == {}

        engine.conn.execute(
            """CREATE TABLE struct_source AS
            SELECT {'zip': x, 'city': col_name, 'nested': {'k': x}} AS profile
            FROM "data" """
        )
        engine.conn.execute('DROP VIEW "data"')
        engine.conn.execute('CREATE VIEW "data" AS SELECT * FROM struct_source')
        engine.refresh_schema()

        assert list(engine.struct_fields_by_column) == ["profile"]
        fields = engine.struct_fields_by_column["profile"]
        assert [field.name for field in fields] == ["zip", "city", "nested"]

        path_types = dict(engine.struct_paths_by_column["profile"])
        assert "zip" in path_types
        assert "city" in path_types
        assert "nested" in path_types
        assert "nested.k" in path_types
        assert path_types["city"].upper().startswith("VARCHAR")
    finally:
        engine.close()


def test_refresh_schema_invalidates_completion_caches(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        before = _completion_values(engine, "SELECT ")
        assert "col_name" in before
        assert "x" in before

        engine.conn.execute("CREATE TABLE refreshed_source AS SELECT 1 AS y, 2 AS z")
        engine.conn.execute('DROP VIEW "data"')
        engine.conn.execute('CREATE VIEW "data" AS SELECT * FROM refreshed_source')
        engine.refresh_schema()

        after = _completion_values(engine, "SELECT ")
        assert "y" in after
        assert "z" in after
        assert "col_name" not in after
        assert "x" not in after
    finally:
        engine.close()


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
        assert engine.lookup_command("/llm-query") is not None
        help_text = engine.help_text()
        assert "/llm-query <natural language query>" in help_text
        assert "/sample [n]" in help_text
        assert "/top <column> <n>" in help_text
        assert "/dupes <key_cols_csv> [n] [| where]" in help_text
        assert "/hist <numeric_col> [bins] [| where]" in help_text
        assert "/crosstab <col_a> <col_b> [n] [| where]" in help_text
        assert "/corr <numeric_x> <numeric_y> [| where]" in help_text
        assert "/group <group cols> | <aggregates> [| having]" in help_text
        assert "/summary [n_cols] [| where]" in help_text
        assert "/save <path.csv|path.parquet|path.pq|path.json>" in help_text
        assert "/exit or /quit" in help_text
    finally:
        engine.close()


def test_help_and_schema_commands_validate_args_and_return_tables(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        help_out = engine.run_input("/help")
        assert help_out.status == "ok"
        assert help_out.result is not None
        assert help_out.result.columns == ["command", "usage", "description"]
        help_rows = {tuple(row) for row in help_out.result.rows}
        assert ("/summary", "/summary [n_cols] [| where]", "Show per-column summary statistics.") in help_rows

        help_usage = engine.run_input("/help extra")
        assert help_usage.status == "error"
        assert help_usage.message == "Usage: /help"

        schema_out = engine.run_input("/schema")
        assert schema_out.status == "ok"
        assert schema_out.result is not None
        assert schema_out.result.columns == ["column", "type", "nullable"]
        schema_rows = {tuple(row) for row in schema_out.result.rows}
        assert ("col_name", "VARCHAR", "YES") in schema_rows
        assert ("x", "BIGINT", "YES") in schema_rows
    finally:
        engine.close()


def test_history_and_rerun_commands_cover_success_and_errors(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        engine.run_sql('SELECT * FROM "data" LIMIT 1')
        engine.run_sql('SELECT COUNT(*) AS n FROM "data"')

        history = engine.run_input("/history 1")
        assert history.status == "ok"
        assert history.result is not None
        assert history.result.columns == ["#", "type", "status", "sql"]
        assert len(history.result.rows) == 1
        assert tuple(history.result.rows[0]) == (
            2,
            "user_entered_sql",
            "success",
            'SELECT COUNT(*) AS n FROM "data"',
        )

        rerun = engine.run_input("/rerun 2")
        assert rerun.status == "ok"
        assert rerun.generated_sql == 'SELECT COUNT(*) AS n FROM "data"'
        assert rerun.result is not None
        assert tuple(rerun.result.rows[0]) == (3,)

        rerun_bad = engine.run_input("/rerun nope")
        assert rerun_bad.status == "error"
        assert rerun_bad.message == "/rerun expects an integer index"

        rerun_oob = engine.run_input("/rerun 99")
        assert rerun_oob.status == "error"
        assert rerun_oob.message == "History index out of range"

        rerun_rerun = engine.run_input("/rerun 5")
        assert rerun_rerun.status == "error"
        assert rerun_rerun.message == "Cannot rerun a /rerun entry"
    finally:
        engine.close()


def test_history_includes_helper_commands_and_rerun_replays_them(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        out = engine.run_input('SELECT COUNT(*) AS n FROM "data"')
        assert out.status == "ok"

        out = engine.run_input("/sample 1")
        assert out.status == "ok"

        history = engine.run_input("/history 5")
        assert history.status == "ok"
        assert history.result is not None
        assert [tuple(row) for row in history.result.rows] == [
            (1, "user_entered_sql", "success", 'SELECT COUNT(*) AS n FROM "data"'),
            (2, "command_generated_sql", "success", 'SELECT * FROM "data" LIMIT 1'),
            (3, "user_entered_command", "success", "/sample 1"),
        ]

        rerun_helper = engine.run_input("/rerun 3")
        assert rerun_helper.status == "ok"
        assert rerun_helper.generated_sql == 'SELECT * FROM "data" LIMIT 1'
        assert rerun_helper.result is not None
        assert len(rerun_helper.result.rows) == 1
    finally:
        engine.close()


def test_history_log_and_rerun_log_replay_sql_from_log(tmp_path: Path) -> None:
    _reset_log_state()
    configure_file_logging(log_dir=tmp_path / "logs")
    engine = _build_engine(tmp_path)
    try:
        first = engine.run_input('SELECT COUNT(*) AS n FROM "data"')
        assert first.status == "ok"
        second = engine.run_input('SELECT * FROM "data" LIMIT 1')
        assert second.status == "ok"

        history_log = engine.run_input("/history-log 1")
        assert history_log.status == "ok"
        assert history_log.result is not None
        assert history_log.result.columns == ["event_id", "type", "status", "sql"]
        assert len(history_log.result.rows) == 1
        event_id, query_type, query_status, sql_text = history_log.result.rows[0]
        assert isinstance(event_id, str) and event_id
        assert query_type == "user_entered_sql"
        assert query_status == "success"
        assert sql_text == 'SELECT * FROM "data" LIMIT 1'

        rerun = engine.run_input(f"/rerun-log {event_id}")
        assert rerun.status == "ok"
        assert rerun.generated_sql == 'SELECT * FROM "data" LIMIT 1'
        assert rerun.result is not None
        assert len(rerun.result.rows) == 1
    finally:
        engine.close()
        _reset_log_state()


def test_history_log_and_rerun_log_usage_and_missing_event_errors(tmp_path: Path) -> None:
    _reset_log_state()
    configure_file_logging(log_dir=tmp_path / "logs")
    engine = _build_engine(tmp_path)
    try:
        history_bad = engine.run_input("/history-log 0")
        assert history_bad.status == "error"
        assert history_bad.message == "Usage: /history-log [n]"

        rerun_usage = engine.run_input("/rerun-log")
        assert rerun_usage.status == "error"
        assert rerun_usage.message == "Usage: /rerun-log <event_id>"

        rerun_missing = engine.run_input("/rerun-log missing")
        assert rerun_missing.status == "error"
        assert rerun_missing.message == "Log event not found: missing"
    finally:
        engine.close()
        _reset_log_state()


def test_failed_user_sql_is_recorded_with_error_status(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        out = engine.run_input("SELECT nope FROM data")
        assert out.status == "error"
        latest = engine.query_history[-1]
        assert latest.query_text == "SELECT nope FROM data"
        assert latest.query_type == "user_entered_sql"
        assert latest.query_status == "error"
    finally:
        engine.close()


def test_setting_editor_and_exit_commands_update_engine_state(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        rows_out = engine.run_input("/rows 7")
        assert rows_out.status == "ok"
        assert rows_out.message == "Row display limit set to 7"
        assert engine.max_rows_display == 7

        values_out = engine.run_input("/values 32")
        assert values_out.status == "ok"
        assert values_out.message == "Value display limit set to 32"
        assert engine.max_value_chars == 32

        limit_out = engine.run_input("/limit 9")
        assert limit_out.status == "ok"
        assert limit_out.message == "Default helper query limit set to 9; row display limit set to 9"
        assert limit_out.load_query == 'SELECT * FROM "data" LIMIT 9'
        assert engine.default_limit == 9
        assert engine.max_rows_display == 9

        engine.run_sql('SELECT * FROM "data" LIMIT 2')
        last_out = engine.run_input("/last")
        assert last_out.status == "info"
        assert last_out.load_query == 'SELECT * FROM "data" LIMIT 2'

        clear_out = engine.run_input("/clear")
        assert clear_out.status == "info"
        assert clear_out.clear_editor is True

        exit_out = engine.run_input("/exit")
        assert exit_out.status == "info"
        assert exit_out.should_exit is True

        quit_out = engine.run_input("/quit")
        assert quit_out.status == "info"
        assert quit_out.should_exit is True
    finally:
        engine.close()


def test_limit_command_updates_row_display_for_custom_sql(tmp_path: Path) -> None:
    csv_rows = "".join(f"a,{idx}\n" for idx in range(1, 121))
    engine = _build_engine(tmp_path, csv_text=f"col_name,x\n{csv_rows}")
    try:
        engine.max_rows_display = 50
        limit_out = engine.run_input("/limit 80")
        assert limit_out.status == "ok"
        assert engine.default_limit == 80
        assert engine.max_rows_display == 80

        out = engine.run_sql('SELECT * FROM "data" ORDER BY x')
        assert out.status == "ok"
        assert out.result is not None
        assert len(out.result.rows) == 80
        assert out.result.total_rows == 120
        assert out.result.truncated is True
        assert "(row display limit=80)" in out.message
    finally:
        engine.close()


def test_save_command_requires_result_and_supports_csv_and_json(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        empty_save = engine.run_input("/save out.csv")
        assert empty_save.status == "error"
        assert empty_save.message == "No query result to save yet"

        query_out = engine.run_sql('SELECT col_name, x FROM "data" ORDER BY x')
        assert query_out.status == "ok"
        assert engine.last_result_sql == 'SELECT col_name, x FROM "data" ORDER BY x'

        csv_path = tmp_path / "out.csv"
        csv_out = engine.run_input(f"/save {csv_path}")
        assert csv_out.status == "ok"
        assert csv_path.exists() is True
        csv_text = csv_path.read_text(encoding="utf-8")
        assert "col_name,x" in csv_text
        assert "a,1" in csv_text

        json_path = tmp_path / "out.json"
        json_out = engine.run_input(f"/save {json_path}")
        assert json_out.status == "ok"
        assert json_path.exists() is True
        json_text = json_path.read_text(encoding="utf-8")
        assert '"col_name": "a"' in json_text
        assert '"x": 1' in json_text
    finally:
        engine.close()


def test_describe_and_profile_commands_return_expected_shapes(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        describe_out = engine.run_input("/describe")
        assert describe_out.status == "ok"
        assert describe_out.result is not None
        assert describe_out.result.columns == ["column", "type", "distinct", "nulls", "null_%"]
        assert any(tuple(row)[:2] == ("col_name", "VARCHAR") for row in describe_out.result.rows)

        profile_out = engine.run_input("/profile x")
        assert profile_out.status == "ok"
        assert profile_out.result is not None
        assert profile_out.result.columns == ["metric", "value"]
        metric_names = {str(row[0]) for row in profile_out.result.rows}
        assert {"column", "type", "rows", "distinct", "nulls", "min", "max", "p25", "p50", "p75"} <= metric_names

        unknown_profile = engine.run_input("/profile nope")
        assert unknown_profile.status == "error"
        assert unknown_profile.message == "Unknown column: nope"
    finally:
        engine.close()


def test_helper_completion_suggests_command_names(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        completions = _completion_values(engine, "/to")
        assert "/top" in completions
        llm_completions = _completion_values(engine, "/ll")
        assert "/llm-query" in llm_completions
    finally:
        engine.close()


def test_llm_command_validates_usage(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        cases = [
            ("/llm-query", "Usage: /llm-query <natural language query>"),
            ("/llm-query   ", "Usage: /llm-query <natural language query>"),
            ("/llm", "Unknown command: /llm. Use /help"),
            ("/llm explain counts", "Unknown command: /llm explain counts. Use /help"),
        ]
        for command, expected_message in cases:
            out = engine.run_input(command)
            assert out.status == "error"
            assert out.message == expected_message
        assert [entry.query_text for entry in engine.query_history] == [item.strip() for item, _ in cases]
        assert {entry.query_type for entry in engine.query_history} == {"user_entered_command"}
        assert {entry.query_status for entry in engine.query_history} == {"error"}
    finally:
        engine.close()


def test_llm_command_returns_missing_key_error(tmp_path: Path, monkeypatch: Any) -> None:
    engine = _build_engine(tmp_path)
    try:
        monkeypatch.setattr(
            command_handlers_module, "validate_llm_api_key", lambda: "LLM API key not found in environment."
        )
        out = engine.run_input("/llm-query top values by count")
        assert out.status == "error"
        assert out.message == "LLM API key not found in environment."
    finally:
        engine.close()


def test_llm_command_returns_provider_error(tmp_path: Path, monkeypatch: Any) -> None:
    engine = _build_engine(tmp_path)

    def raise_provider_error(prompt: str, model: str) -> str:
        raise RuntimeError("rate limit")

    try:
        monkeypatch.setattr(command_handlers_module, "validate_llm_api_key", lambda: None)
        monkeypatch.setattr(llm_runner_module, "generate_sql", raise_provider_error)
        out = engine.run_input("/llm-query top values by count")
        assert out.status == "error"
        assert out.message == "LLM request failed. Check API key and network, then try again."
    finally:
        engine.close()


def test_llm_command_returns_invalid_sql_error(tmp_path: Path, monkeypatch: Any) -> None:
    engine = _build_engine(tmp_path)

    def invalid_sql(prompt: str, model: str) -> str:
        return 'DELETE FROM "data"'

    try:
        monkeypatch.setattr(command_handlers_module, "validate_llm_api_key", lambda: None)
        monkeypatch.setattr(llm_runner_module, "generate_sql", invalid_sql)
        out = engine.run_input("/llm-query delete everything")
        assert out.status == "error"
        assert out.generated_sql == 'DELETE FROM "data"'
        assert out.message == (
            "Invalid SQL generated by model: Generated SQL must be SELECT or WITH ... SELECT. "
            "Please rephrase your request and try again."
        )
    finally:
        engine.close()


def test_llm_command_retries_validation_once_then_succeeds(tmp_path: Path, monkeypatch: Any) -> None:
    engine = _build_engine(tmp_path)
    attempts: list[str] = []

    def retry_once(prompt: str, model: str) -> str:
        _ = prompt
        _ = model
        attempts.append("call")
        if len(attempts) == 1:
            return 'DELETE FROM "data"'
        return 'SELECT col_name, COUNT(*) AS count FROM "data" GROUP BY col_name ORDER BY count DESC, col_name LIMIT 10'

    try:
        monkeypatch.setattr(command_handlers_module, "validate_llm_api_key", lambda: None)
        monkeypatch.setattr(llm_runner_module, "generate_sql", retry_once)
        out = engine.run_input("/llm-query count rows by col_name")
        assert out.status == "ok"
        assert out.result is not None
        assert [tuple(row) for row in out.result.rows] == [("a", 2), ("b", 1)]
        assert out.message.endswith("Info: LLM auto-retry fixed SQL (1 retry).")
        assert len(attempts) == 2
    finally:
        engine.close()


def test_llm_command_retries_duckdb_error_once_then_succeeds(tmp_path: Path, monkeypatch: Any) -> None:
    engine = _build_engine(tmp_path)
    attempts: list[str] = []

    def retry_once(prompt: str, model: str) -> str:
        _ = prompt
        _ = model
        attempts.append("call")
        if len(attempts) == 1:
            return 'SELECT missing_function(col_name) FROM "data"'
        return 'SELECT col_name FROM "data" ORDER BY col_name LIMIT 2'

    try:
        monkeypatch.setattr(command_handlers_module, "validate_llm_api_key", lambda: None)
        monkeypatch.setattr(llm_runner_module, "generate_sql", retry_once)
        out = engine.run_input("/llm-query show two names")
        assert out.status == "ok"
        assert out.result is not None
        assert [tuple(row) for row in out.result.rows] == [("a",), ("a",)]
        assert out.message.endswith("Info: LLM auto-retry fixed SQL (1 retry).")
        assert len(attempts) == 2
    finally:
        engine.close()


def test_llm_command_retry_is_capped_at_one(tmp_path: Path, monkeypatch: Any) -> None:
    engine = _build_engine(tmp_path)
    attempts: list[str] = []

    def always_invalid(prompt: str, model: str) -> str:
        _ = prompt
        _ = model
        attempts.append("call")
        return 'DELETE FROM "data"'

    try:
        monkeypatch.setattr(command_handlers_module, "validate_llm_api_key", lambda: None)
        monkeypatch.setattr(llm_runner_module, "generate_sql", always_invalid)
        out = engine.run_input("/llm-query delete everything")
        assert out.status == "error"
        assert len(attempts) == 2
    finally:
        engine.close()


def test_llm_command_executes_generated_sql_via_existing_path(tmp_path: Path, monkeypatch: Any) -> None:
    engine = _build_engine(tmp_path)
    sql = 'SELECT col_name, COUNT(*) AS count FROM "data" GROUP BY col_name ORDER BY count DESC, col_name LIMIT 10'
    captured: dict[str, Any] = {}

    class _Message:
        def __init__(self, content: str) -> None:
            self.content = content

    class _Choice:
        def __init__(self, content: str) -> None:
            self.message = _Message(content)

    class _Response:
        def __init__(self, content: str) -> None:
            self.choices = [_Choice(content)]

    def fake_completion(**kwargs: Any) -> _Response:
        captured.update(kwargs)
        return _Response(f"```sql\n{sql}\n```")

    try:
        monkeypatch.setattr(command_handlers_module, "validate_llm_api_key", lambda: None)
        monkeypatch.setattr(command_handlers_module, "resolve_llm_model", lambda: "openai/gpt-5-mini")
        monkeypatch.setattr("sqlexplore.llm.llm_sql.litellm.completion", fake_completion)
        out = engine.run_input("/llm-query count rows by col_name")
        assert out.status == "ok"
        assert out.generated_sql == sql
        assert out.result is not None
        assert [tuple(row) for row in out.result.rows] == [("a", 2), ("b", 1)]
        assert captured["model"] == "openai/gpt-5-mini"
        assert captured["messages"][1]["role"] == "user"
        assert "count rows by col_name" in captured["messages"][1]["content"]
        assert [entry.query_text for entry in engine.query_history[-2:]] == [sql, "/llm-query count rows by col_name"]
        assert [entry.query_type for entry in engine.query_history[-2:]] == [
            "llm_generated_sql",
            "user_entered_command",
        ]
        assert [entry.query_status for entry in engine.query_history[-2:]] == ["success", "success"]
    finally:
        engine.close()


def test_llm_history_and_show_commands_replay_logged_bundle(tmp_path: Path, monkeypatch: Any) -> None:
    _reset_log_state()
    configure_file_logging(log_dir=tmp_path / "logs")
    engine = _build_engine(tmp_path)
    sql = 'SELECT col_name, COUNT(*) AS count FROM "data" GROUP BY col_name ORDER BY count DESC, col_name LIMIT 10'

    class _Message:
        def __init__(self, content: str) -> None:
            self.content = content

    class _Choice:
        def __init__(self, content: str) -> None:
            self.message = _Message(content)

    class _Response:
        def __init__(self, content: str) -> None:
            self.choices = [_Choice(content)]
            self.model = "openai/gpt-5-mini"
            self.id = "resp-123"
            self.usage = {"prompt_tokens": 10, "completion_tokens": 4, "total_tokens": 14}

    def fake_completion(**kwargs: Any) -> _Response:
        _ = kwargs
        return _Response(f"```sql\n{sql}\n```")

    try:
        monkeypatch.setattr(command_handlers_module, "validate_llm_api_key", lambda: None)
        monkeypatch.setattr("sqlexplore.llm.llm_sql.litellm.completion", fake_completion)

        llm_out = engine.run_input("/llm-query count rows by col_name")
        assert llm_out.status == "ok"
        assert llm_out.generated_sql == sql

        history_out = engine.run_input("/llm-history 1")
        assert history_out.status == "ok"
        assert history_out.result is not None
        assert history_out.result.columns == ["trace_id", "status", "retries", "model", "query", "generated_sql"]
        assert len(history_out.result.rows) == 1

        trace_id, status, retries, _model, query, generated_sql = history_out.result.rows[0]
        assert isinstance(trace_id, str) and trace_id
        assert status == "success"
        assert retries == "0"
        assert query == "count rows by col_name"
        assert generated_sql == sql

        trace_events = read_log_events_for_trace(trace_id)
        assert any(event.get("kind") == "llm.query" for event in trace_events)
        assert any(event.get("kind") == "llm.request" for event in trace_events)
        assert any(event.get("kind") == "llm.response" for event in trace_events)
        assert any(event.get("kind") == "llm.result" for event in trace_events)

        show_out = engine.run_input(f"/llm-show {trace_id}")
        assert show_out.status == "ok"
        assert show_out.result is not None
        fields = {str(row[0]): str(row[1]) for row in show_out.result.rows}
        assert fields["trace_id"] == trace_id
        assert fields["status"] == "success"
        assert fields["query"] == "count rows by col_name"
        assert fields["generated_sql"] == sql
        assert '"requests"' in fields["bundle_json"]
        assert '"responses"' in fields["bundle_json"]
        assert show_out.load_query == sql
    finally:
        engine.close()
        _reset_log_state()


def test_llm_history_and_show_command_usage_and_missing_trace(tmp_path: Path) -> None:
    _reset_log_state()
    configure_file_logging(log_dir=tmp_path / "logs")
    engine = _build_engine(tmp_path)
    try:
        history_usage = engine.run_input("/llm-history 0")
        assert history_usage.status == "error"
        assert history_usage.message == "Usage: /llm-history [n]"

        show_usage = engine.run_input("/llm-show")
        assert show_usage.status == "error"
        assert show_usage.message == "Usage: /llm-show <trace_id>"

        show_missing = engine.run_input("/llm-show missing")
        assert show_missing.status == "error"
        assert show_missing.message == "LLM trace not found: missing"
    finally:
        engine.close()
        _reset_log_state()


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


def test_sample_filter_sort_and_agg_commands_run_queries(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        sample_out = engine.run_input("/sample 2")
        assert sample_out.status == "ok"
        assert sample_out.generated_sql == 'SELECT * FROM "data" LIMIT 2'
        assert sample_out.result is not None
        assert len(sample_out.result.rows) == 2

        filter_out = engine.run_input("/filter x >= 2")
        assert filter_out.status == "ok"
        assert filter_out.generated_sql == 'SELECT * FROM "data" WHERE x >= 2 LIMIT 10'
        assert filter_out.result is not None
        assert [tuple(row) for row in filter_out.result.rows] == [("b", 2), ("a", 3)]

        sort_out = engine.run_input("/sort x DESC")
        assert sort_out.status == "ok"
        assert sort_out.generated_sql == 'SELECT * FROM "data" ORDER BY x DESC LIMIT 10'
        assert sort_out.result is not None
        assert [tuple(row) for row in sort_out.result.rows] == [("a", 3), ("b", 2), ("a", 1)]

        agg_out = engine.run_input("/agg COUNT(*) AS n | x >= 2")
        assert agg_out.status == "ok"
        assert agg_out.generated_sql == 'SELECT COUNT(*) AS n FROM "data" WHERE x >= 2'
        assert agg_out.result is not None
        assert [tuple(row) for row in agg_out.result.rows] == [(2,)]
    finally:
        engine.close()


def test_sample_filter_sort_and_agg_commands_validate_args(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        sample_out = engine.run_input("/sample nope")
        assert sample_out.status == "error"
        assert sample_out.message == "Usage: /sample [n]"

        filter_out = engine.run_input("/filter")
        assert filter_out.status == "error"
        assert filter_out.message == "Usage: /filter <where condition>"

        sort_out = engine.run_input("/sort")
        assert sort_out.status == "error"
        assert sort_out.message == "Usage: /sort <order expressions>"

        agg_out = engine.run_input("/agg")
        assert agg_out.status == "error"
        assert agg_out.message == "Usage: /agg <aggregates> [| where]"
    finally:
        engine.close()


def test_group_and_agg_commands_reject_extra_or_empty_pipe_sections(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        bad_agg = engine.run_input("/agg SUM(x) | x > 1 | y > 2")
        assert bad_agg.status == "error"
        assert bad_agg.message == "Usage: /agg <aggregates> [| where]"

        bad_group = engine.run_input("/group col_name | SUM(x) AS total | SUM(x) > 2 | extra")
        assert bad_group.status == "error"
        assert bad_group.message == "Usage: /group <group cols> | <aggregates> [| having]"

        trailing_pipe = engine.run_input("/agg SUM(x) |")
        assert trailing_pipe.status == "error"
        assert trailing_pipe.message == "Usage: /agg <aggregates> [| where]"

        empty_middle = engine.run_input("/group col_name || SUM(x) AS total")
        assert empty_middle.status == "error"
        assert empty_middle.message == "Usage: /group <group cols> | <aggregates> [| having]"
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


def test_top_and_dupes_commands_return_unknown_column_errors(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        top_out = engine.run_input("/top nope 3")
        assert top_out.status == "error"
        assert top_out.message == "Unknown column: nope"

        dupes_out = engine.run_input("/dupes nope")
        assert dupes_out.status == "error"
        assert dupes_out.message == "Unknown column: nope"

        dupes_multi = engine.run_input("/dupes col_name,nope")
        assert dupes_multi.status == "error"
        assert dupes_multi.message == "Unknown column: nope"
    finally:
        engine.close()


def test_commands_reject_non_positive_integer_args(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        cases = [
            ("/rows 0", "Usage: /rows <n>"),
            ("/values -5", "Usage: /values <n>"),
            ("/limit 0", "Usage: /limit <n>"),
            ("/history 0", "Usage: /history [n]"),
            ("/history-log 0", "Usage: /history-log [n]"),
            ("/llm-history 0", "Usage: /llm-history [n]"),
            ("/sample 0", "Usage: /sample [n]"),
            ("/top col_name 0", "Usage: /top <column> <n>"),
            ("/dupes col_name 0", "Usage: /dupes <key_cols_csv> [n] [| where]"),
            ("/hist x 0", "Usage: /hist <numeric_col> [bins] [| where]"),
            ("/crosstab col_name x 0", "Usage: /crosstab <col_a> <col_b> [n] [| where]"),
        ]
        for command, message in cases:
            out = engine.run_input(command)
            assert out.status == "error"
            assert out.message == message
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


def test_usage_errors_match_help_usage_lines(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    try:
        rerun_out = engine.run_input("/rerun")
        assert rerun_out.status == "error"
        assert rerun_out.message == "Usage: /rerun <history_index>"

        rerun_log_out = engine.run_input("/rerun-log")
        assert rerun_log_out.status == "error"
        assert rerun_log_out.message == "Usage: /rerun-log <event_id>"

        llm_show_out = engine.run_input("/llm-show")
        assert llm_show_out.status == "error"
        assert llm_show_out.message == "Usage: /llm-show <trace_id>"

        save_out = engine.run_input("/save")
        assert save_out.status == "error"
        assert save_out.message == "Usage: /save <path.csv|path.parquet|path.pq|path.json>"

        quit_out = engine.run_input("/quit extra")
        assert quit_out.status == "error"
        assert quit_out.message == "Usage: /exit or /quit"

        help_text = engine.help_text()
        assert "/rerun <history_index>" in help_text
        assert "/rerun-log <event_id>" in help_text
        assert "/llm-show <trace_id>" in help_text
        assert "/save <path.csv|path.parquet|path.pq|path.json>" in help_text
        assert "/exit or /quit" in help_text
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
