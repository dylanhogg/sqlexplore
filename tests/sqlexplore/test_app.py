from __future__ import annotations

import asyncio
import csv
from io import StringIO
from pathlib import Path
from typing import Any, cast

from rich.text import Text
from textual.widgets import DataTable, OptionList, RichLog

from sqlexplore.app import SqlExplorerEngine, SqlExplorerTui, SqlQueryEditor, app_version


def _build_app(
    tmp_path: Path,
    csv_text: str = "a,b\n1,2\n3,4\n",
    max_rows_display: int = 100,
) -> tuple[SqlExplorerTui, SqlExplorerEngine]:
    csv_path = tmp_path / "data.csv"
    csv_path.write_text(csv_text, encoding="utf-8")
    engine = SqlExplorerEngine(
        data_path=csv_path,
        table_name="data",
        database=":memory:",
        default_limit=10,
        max_rows_display=max_rows_display,
        max_value_chars=80,
    )
    return SqlExplorerTui(engine), engine


def _log_text(app: SqlExplorerTui) -> str:
    log = cast(Any, app.query_one("#activity_log", RichLog))
    return "\n".join(str(line.text) for line in log.lines)


def _column_values(table: DataTable[str], column_index: int) -> list[str]:
    return [str(table.get_row_at(row_index)[column_index]) for row_index in range(table.row_count)]


def _parse_tsv(text: str) -> list[list[str]]:
    return list(csv.reader(StringIO(text), delimiter="\t"))


def _visible_binding_order(bindings: list[Any]) -> list[tuple[str, str]]:
    return [
        (binding.key_display or binding.key, binding.description)
        for binding in bindings
        if binding.show and binding.description
    ]


def test_query_and_results_panes_share_status_key_order() -> None:
    assert _visible_binding_order(SqlQueryEditor.BINDINGS) == _visible_binding_order(SqlExplorerTui.BINDINGS)


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


def test_activity_log_shows_version_on_mount(tmp_path: Path) -> None:
    async def run() -> None:
        app, engine = _build_app(tmp_path)
        try:
            async with app.run_test() as pilot:
                await pilot.pause()
                assert f"sqlexplore {app_version()}" in _log_text(app)
        finally:
            engine.close()

    asyncio.run(run())


def test_copy_tsv_shortcut_copies_full_query_result_beyond_display_limit(tmp_path: Path) -> None:
    async def run() -> None:
        app, engine = _build_app(tmp_path, csv_text="a,b\n1,x\n2,y\n3,z\n4,w\n5,v\n", max_rows_display=2)
        try:
            async with app.run_test() as pilot:
                await pilot.pause()

                results = cast(DataTable[str], app.query_one("#results_table", DataTable))
                assert results.row_count == 2

                await pilot.press("f8")
                await pilot.pause()
                copied_rows = _parse_tsv(app.clipboard)
                assert copied_rows == [
                    ["a", "b"],
                    ["1", "x"],
                    ["2", "y"],
                    ["3", "z"],
                    ["4", "w"],
                    ["5", "v"],
                ]
                assert "full query result" in _log_text(app)

                await pilot.press("ctrl+2")
                await pilot.pause()
                await pilot.press("f8")
                await pilot.pause()
                copied_rows = _parse_tsv(app.clipboard)
                assert len(copied_rows) == 6
        finally:
            engine.close()

    asyncio.run(run())


def test_copy_tsv_shortcut_escapes_excel_sensitive_values(tmp_path: Path) -> None:
    async def run() -> None:
        csv_text = 'id,note\n1,"a,b"\n2,"he said ""ok"""\n3,"line1\nline2"\n'
        app, engine = _build_app(tmp_path, csv_text=csv_text, max_rows_display=2)
        try:
            async with app.run_test() as pilot:
                await pilot.pause()
                await pilot.press("f8")
                await pilot.pause()
                assert _parse_tsv(app.clipboard) == [
                    ["id", "note"],
                    ["1", "a,b"],
                    ["2", 'he said "ok"'],
                    ["3", "line1\nline2"],
                ]
        finally:
            engine.close()

    asyncio.run(run())


def test_copy_tsv_shortcut_shows_error_when_no_result_available(tmp_path: Path) -> None:
    async def run() -> None:
        app, engine = _build_app(tmp_path)
        try:
            async with app.run_test() as pilot:
                await pilot.pause()
                app.engine.last_result_sql = None
                await pilot.press("f8")
                await pilot.pause()
                assert "No query result available to copy." in _log_text(app)
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


def test_json_highlighting_applies_to_detected_varchar_json_column(tmp_path: Path) -> None:
    async def run() -> None:
        csv_text = 'id,j\n1,"{""a"":1}"\n2,"{""a"":2}"\n3,"{""a"":3}"\n'
        app, engine = _build_app(tmp_path, csv_text=csv_text)
        try:
            async with app.run_test() as pilot:
                await pilot.pause()
                results = cast(DataTable[Any], app.query_one("#results_table", DataTable))
                cell = results.get_row_at(0)[1]
                assert isinstance(cell, Text)
                assert any("json.key" in str(span.style) for span in cell.spans)
        finally:
            engine.close()

    asyncio.run(run())


def test_json_highlighting_skips_invalid_json_text(tmp_path: Path) -> None:
    async def run() -> None:
        csv_text = 'id,j\n1,{bad}\n2,[not-json]\n3,"{""a"":1"\n'
        app, engine = _build_app(tmp_path, csv_text=csv_text)
        try:
            async with app.run_test() as pilot:
                await pilot.pause()
                results = cast(DataTable[Any], app.query_one("#results_table", DataTable))
                assert isinstance(results.get_row_at(0)[1], str)
        finally:
            engine.close()

    asyncio.run(run())


def test_json_highlighting_disables_when_total_rows_exceeds_threshold(tmp_path: Path) -> None:
    async def run() -> None:
        app, engine = _build_app(tmp_path)
        try:
            async with app.run_test() as pilot:
                await pilot.pause()
                editor = app.query_one("#query_editor", SqlQueryEditor)
                editor.text = "SELECT '{\"a\":1}' AS j FROM range(100001)"
                await pilot.press("ctrl+enter")
                await pilot.pause()
                results = cast(DataTable[Any], app.query_one("#results_table", DataTable))
                assert isinstance(results.get_row_at(0)[0], str)
        finally:
            engine.close()

    asyncio.run(run())


def test_json_highlighting_keeps_sorting_behavior(tmp_path: Path) -> None:
    async def run() -> None:
        csv_text = 'id,j\n1,"{""name"":""z""}"\n2,"{""name"":""a""}"\n3,"{""name"":""m""}"\n'
        app, engine = _build_app(tmp_path, csv_text=csv_text)
        try:
            async with app.run_test() as pilot:
                await pilot.pause()

                results = cast(DataTable[str], app.query_one("#results_table", DataTable))
                json_column = results.ordered_columns[1]
                click = DataTable.HeaderSelected(results, json_column.key, 1, json_column.label)

                app.on_data_table_header_selected(click)
                await pilot.pause()
                assert _column_values(results, 1) == ['{"name":"a"}', '{"name":"m"}', '{"name":"z"}']

                app.on_data_table_header_selected(click)
                await pilot.pause()
                assert _column_values(results, 1) == ['{"name":"z"}', '{"name":"m"}', '{"name":"a"}']
        finally:
            engine.close()

    asyncio.run(run())


def test_completion_menu_auto_opens_for_prefix_and_down_closes_it(tmp_path: Path) -> None:
    async def run() -> None:
        app, engine = _build_app(tmp_path)
        try:
            async with app.run_test() as pilot:
                editor = app.query_one("#query_editor", SqlQueryEditor)
                menu = app.query_one("#completion_menu", OptionList)

                editor.text = ""
                editor.focus()
                await pilot.pause()
                await pilot.press("/", "s")
                await pilot.pause()
                assert menu.display is True
                assert menu.option_count > 0

                await pilot.press("down")
                await pilot.pause()
                assert menu.display is False
        finally:
            engine.close()

    asyncio.run(run())


def test_completion_menu_opens_hides_and_reopens_with_ctrl_space(tmp_path: Path) -> None:
    async def run() -> None:
        app, engine = _build_app(tmp_path)
        try:
            async with app.run_test() as pilot:
                editor = app.query_one("#query_editor", SqlQueryEditor)
                menu = app.query_one("#completion_menu", OptionList)

                editor.text = ""
                editor.focus()
                await pilot.pause()
                await pilot.press("s", "e", "l", "e", "c", "t", "space")
                await pilot.pause()
                assert menu.display is False

                await pilot.press("ctrl+space")
                await pilot.pause()
                assert menu.display is True
                assert menu.option_count > 0

                await pilot.press("escape")
                await pilot.pause()
                assert menu.display is False

                await pilot.press("ctrl+space")
                await pilot.pause()
                assert menu.display is True
        finally:
            engine.close()

    asyncio.run(run())


def test_completion_menu_down_tab_accepts_selected_item(tmp_path: Path) -> None:
    async def run() -> None:
        app, engine = _build_app(tmp_path)
        try:
            async with app.run_test() as pilot:
                editor = app.query_one("#query_editor", SqlQueryEditor)
                menu = app.query_one("#completion_menu", OptionList)

                editor.text = ""
                editor.focus()
                await pilot.pause()
                await pilot.press("/", "s")
                await pilot.pause()
                assert menu.display is True

                await pilot.press("ctrl+space")
                await pilot.pause()
                assert menu.display is True
                assert menu.option_count >= 2

                selected_prompt = str(menu.get_option_at_index(1).prompt)
                selected_label = selected_prompt.split("  [", 1)[0]

                await pilot.press("down")
                await pilot.pause()
                await pilot.press("tab")
                await pilot.pause()

                assert editor.text == selected_label
                assert "\n" not in editor.text
        finally:
            engine.close()

    asyncio.run(run())


def test_completion_menu_tab_accepts_primary_completion(tmp_path: Path) -> None:
    async def run() -> None:
        app, engine = _build_app(tmp_path)
        try:
            async with app.run_test() as pilot:
                editor = app.query_one("#query_editor", SqlQueryEditor)
                menu = app.query_one("#completion_menu", OptionList)

                editor.text = ""
                editor.focus()
                await pilot.pause()
                await pilot.press("/", "t", "o")
                await pilot.pause()
                assert menu.display is True

                await pilot.press("ctrl+space")
                await pilot.pause()
                assert menu.display is True

                await pilot.press("tab")
                await pilot.pause()
                assert editor.text == "/top"
        finally:
            engine.close()

    asyncio.run(run())


def test_completion_tab_falls_back_to_indent_after_cursor_move(tmp_path: Path) -> None:
    async def run() -> None:
        app, engine = _build_app(tmp_path)
        try:
            async with app.run_test() as pilot:
                editor = app.query_one("#query_editor", SqlQueryEditor)
                menu = app.query_one("#completion_menu", OptionList)

                editor.text = ""
                editor.focus()
                await pilot.pause()
                await pilot.press("/", "s")
                await pilot.pause()
                assert menu.display is True

                await pilot.press("ctrl+space")
                await pilot.pause()
                assert menu.display is True

                await pilot.press("left")
                await pilot.pause()
                assert editor.cursor_location == (0, 1)
                assert menu.display is False

                await pilot.press("tab")
                await pilot.pause()
                assert editor.text == "/   s"
        finally:
            engine.close()

    asyncio.run(run())


def test_completion_menu_tracks_selected_item_beyond_visible_window(tmp_path: Path) -> None:
    async def run() -> None:
        app, engine = _build_app(tmp_path)
        try:
            async with app.run_test() as pilot:
                editor = app.query_one("#query_editor", SqlQueryEditor)
                menu = app.query_one("#completion_menu", OptionList)

                editor.text = ""
                editor.focus()
                await pilot.pause()
                await pilot.press("/")
                await pilot.pause()
                assert menu.display is True
                assert menu.option_count == 8
                before_prompts = [str(menu.get_option_at_index(i).prompt) for i in range(menu.option_count)]

                await pilot.press("ctrl+space")
                await pilot.pause()
                for _ in range(10):
                    await pilot.press("down")
                    await pilot.pause()

                after_prompts = [str(menu.get_option_at_index(i).prompt) for i in range(menu.option_count)]
                assert after_prompts != before_prompts
                highlighted_index = menu.highlighted
                assert highlighted_index is not None
                highlighted_prompt = str(menu.get_option_at_index(highlighted_index).prompt)
                highlighted = highlighted_prompt.split("  [", 1)[0]

                await pilot.press("tab")
                await pilot.pause()
                assert editor.text == highlighted
        finally:
            engine.close()

    asyncio.run(run())


def test_completion_menu_enter_does_not_accept_completion(tmp_path: Path) -> None:
    async def run() -> None:
        app, engine = _build_app(tmp_path)
        try:
            async with app.run_test() as pilot:
                editor = app.query_one("#query_editor", SqlQueryEditor)
                menu = app.query_one("#completion_menu", OptionList)

                editor.text = ""
                editor.focus()
                await pilot.pause()
                await pilot.press("/", "s")
                await pilot.pause()
                assert menu.display is True

                await pilot.press("ctrl+space")
                await pilot.pause()
                assert menu.display is True
                assert menu.option_count > 0

                await pilot.press("enter")
                await pilot.pause()
                assert editor.text == "/s\n"
                assert menu.display is False

                await pilot.press("down")
                await pilot.pause()
                assert menu.display is False
        finally:
            engine.close()

    asyncio.run(run())


def test_completion_menu_option_events_sync_and_accept(tmp_path: Path) -> None:
    async def run() -> None:
        app, engine = _build_app(tmp_path)
        try:
            async with app.run_test() as pilot:
                editor = app.query_one("#query_editor", SqlQueryEditor)
                menu = app.query_one("#completion_menu", OptionList)

                editor.text = ""
                editor.focus()
                await pilot.pause()
                await pilot.press("/", "s")
                await pilot.pause()
                assert menu.display is True

                option = menu.get_option_at_index(1)
                expected = str(option.prompt).split("  [", 1)[0]
                app.on_option_list_option_highlighted(OptionList.OptionHighlighted(menu, option, 1))
                await pilot.pause()
                await pilot.press("tab")
                await pilot.pause()
                assert editor.text == expected
                assert menu.display is False

                editor.text = ""
                editor.focus()
                await pilot.pause()
                await pilot.press("/", "s")
                await pilot.pause()
                assert menu.display is True
                option = menu.get_option_at_index(1)
                expected = str(option.prompt).split("  [", 1)[0]

                app.on_option_list_option_selected(OptionList.OptionSelected(menu, option, 1))
                await pilot.pause()
                assert editor.text == expected
                assert menu.display is False
        finally:
            engine.close()

    asyncio.run(run())


def test_query_editor_applies_sql_highlighting_to_single_line() -> None:
    editor = SqlQueryEditor("SELECT a FROM t", lambda: [], lambda: None, lambda: None)
    line = editor.get_line(0)
    assert line.plain == "SELECT a FROM t"
    assert len(line.spans) > 0
    assert any(" on " not in str(span.style) for span in line.spans)


def test_query_editor_styles_helper_commands() -> None:
    editor = SqlQueryEditor("/sample 5", lambda: [], lambda: None, lambda: None)
    line = editor.get_line(0)
    assert line.plain == "/sample 5"
    assert any("cyan" in str(span.style) for span in line.spans)


def test_query_editor_preserves_trailing_spaces() -> None:
    editor = SqlQueryEditor("SELECT ", lambda: [], lambda: None, lambda: None)
    line = editor.get_line(0)
    assert line.plain == "SELECT "
