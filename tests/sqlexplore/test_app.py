from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, cast

from textual.widgets import DataTable, RichLog

from sqlexplore.app import SqlExplorerEngine, SqlExplorerTui, SqlQueryEditor


def _build_app(
    tmp_path: Path,
    csv_text: str = "a,b\n1,2\n3,4\n",
) -> tuple[SqlExplorerTui, SqlExplorerEngine]:
    csv_path = tmp_path / "data.csv"
    csv_path.write_text(csv_text, encoding="utf-8")
    engine = SqlExplorerEngine(
        data_path=csv_path,
        table_name="data",
        database=":memory:",
        default_limit=10,
        max_rows_display=100,
        max_value_chars=80,
    )
    return SqlExplorerTui(engine), engine


def _log_text(app: SqlExplorerTui) -> str:
    log = cast(Any, app.query_one("#activity_log", RichLog))
    return "\n".join(str(line.text) for line in log.lines)


def _column_values(table: DataTable[str], column_index: int) -> list[str]:
    return [str(table.get_row_at(row_index)[column_index]) for row_index in range(table.row_count)]


def test_ctrl_shortcuts_work_in_editor_focus(tmp_path: Path) -> None:
    async def run() -> None:
        app, engine = _build_app(tmp_path)
        try:
            async with app.run_test() as pilot:
                editor = app.query_one("#query_editor", SqlQueryEditor)
                results = cast(DataTable[str], app.query_one("#results_table", DataTable))
                editor.text = "SELECT 1 AS x"
                await pilot.press("ctrl+enter")
                await pilot.pause()
                assert "1/1 rows shown" in _log_text(app)

                editor.text = "junk"
                await pilot.press("ctrl+n")
                await pilot.pause()
                assert editor.text == engine.default_query

                editor.text = "junk"
                await pilot.press("ctrl+l")
                await pilot.pause()
                assert editor.text == ""

                await pilot.press("ctrl+2")
                await pilot.pause()
                assert app.focused is results

                await pilot.press("ctrl+1")
                await pilot.pause()
                assert app.focused is editor
        finally:
            engine.close()

    asyncio.run(run())


def test_function_key_shortcuts_work_in_editor_focus(tmp_path: Path) -> None:
    async def run() -> None:
        app, engine = _build_app(tmp_path)
        try:
            async with app.run_test() as pilot:
                editor = app.query_one("#query_editor", SqlQueryEditor)
                editor.text = "SELECT 1 AS x"
                await pilot.press("f5")
                await pilot.pause()
                assert "1/1 rows shown" in _log_text(app)

                editor.text = "junk"
                await pilot.press("f6")
                await pilot.pause()
                assert editor.text == engine.default_query

                editor.text = "junk"
                await pilot.press("f7")
                await pilot.pause()
                assert editor.text == ""

                await pilot.press("f1")
                await pilot.pause()
                assert "/help" in _log_text(app)

                await pilot.press("f10")
                await pilot.pause()
                assert app.is_running is False
        finally:
            engine.close()

    asyncio.run(run())


def test_secondary_help_and_quit_shortcuts_work(tmp_path: Path) -> None:
    async def run() -> None:
        app, engine = _build_app(tmp_path)
        try:
            async with app.run_test() as pilot:
                await pilot.press("ctrl+shift+p")
                await pilot.pause()
                assert "/help" in _log_text(app)

                await pilot.press("ctrl+q")
                await pilot.pause()
                assert app.is_running is False
        finally:
            engine.close()

    asyncio.run(run())


def test_ctrl_b_toggles_data_explorer_sidebar(tmp_path: Path) -> None:
    async def run() -> None:
        app, engine = _build_app(tmp_path)
        try:
            async with app.run_test() as pilot:
                sidebar = app.query_one("#sidebar")
                assert sidebar.display is True

                await pilot.press("ctrl+b")
                await pilot.pause()
                assert sidebar.display is False

                await pilot.press("ctrl+b")
                await pilot.pause()
                assert sidebar.display is True
        finally:
            engine.close()

    asyncio.run(run())


def test_results_header_click_sorts_asc_desc(tmp_path: Path) -> None:
    async def run() -> None:
        app, engine = _build_app(tmp_path, csv_text="a,b\nx,20\ny,3\nz,100\n")
        try:
            async with app.run_test() as pilot:
                await pilot.pause()

                results = cast(DataTable[str], app.query_one("#results_table", DataTable))
                second_column = results.ordered_columns[1]
                click = DataTable.HeaderSelected(results, second_column.key, 1, second_column.label)

                app.on_data_table_header_selected(click)
                await pilot.pause()
                assert _column_values(results, 1) == ["3", "20", "100"]

                app.on_data_table_header_selected(click)
                await pilot.pause()
                assert _column_values(results, 1) == ["100", "20", "3"]
        finally:
            engine.close()

    asyncio.run(run())
