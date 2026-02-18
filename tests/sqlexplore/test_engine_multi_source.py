from pathlib import Path

import pytest
import typer

from sqlexplore.core.engine import SqlExplorerEngine


def _build_engine(data_paths: tuple[Path, ...]) -> SqlExplorerEngine:
    return SqlExplorerEngine(
        data_path=data_paths[0],
        data_paths=data_paths,
        table_name="data",
        database=":memory:",
        default_limit=10,
        max_rows_display=100,
        max_value_chars=80,
    )


def test_engine_unions_multiple_sources_with_matching_schema(tmp_path: Path) -> None:
    first = tmp_path / "a.csv"
    second = tmp_path / "b.csv"
    first.write_text("city,x\na,1\nb,2\n", encoding="utf-8")
    second.write_text("city,x\nc,3\n", encoding="utf-8")

    engine = _build_engine((first, second))
    try:
        out = engine.run_sql('SELECT SUM(x) AS total FROM "data"')
        assert out.status == "ok"
        assert out.result is not None
        assert [tuple(row) for row in out.result.rows] == [(6,)]

        tables = {tuple(row) for row in engine.startup_tables()}
        assert ("source", "data_src_1", str(first.resolve()), 2) in tables
        assert ("source", "data_src_2", str(second.resolve()), 1) in tables
        assert (
            "union",
            "data",
            'SELECT * FROM "data_src_1" UNION ALL SELECT * FROM "data_src_2"',
            3,
        ) in tables
    finally:
        engine.close()


def test_engine_raises_bad_parameter_for_schema_mismatch(tmp_path: Path) -> None:
    first = tmp_path / "a.csv"
    second = tmp_path / "b.csv"
    first.write_text("city,x\na,1\n", encoding="utf-8")
    second.write_text("city,x\nb,not-an-int\n", encoding="utf-8")

    with pytest.raises(typer.BadParameter, match=r"Schema mismatch in source 2"):
        _build_engine((first, second))
