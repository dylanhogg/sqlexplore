from __future__ import annotations

from pathlib import Path

from sqlexplore.app import SqlExplorerEngine


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
