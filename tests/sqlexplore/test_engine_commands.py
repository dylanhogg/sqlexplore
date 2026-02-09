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
