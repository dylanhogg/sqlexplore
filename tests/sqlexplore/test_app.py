import asyncio
import csv
from io import StringIO
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

from rich.text import Text
from textual.widgets import DataTable, OptionList, Static, TextArea

from sqlexplore.completion.completion_types import CompletionItem
from sqlexplore.engine import SqlExplorerEngine, app_version
from sqlexplore.tui import NULL_VALUE_COLOR, URL_COLOR, ResultsPreview, SqlExplorerTui, SqlQueryEditor


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


def _build_txt_app(tmp_path: Path, txt_text: str) -> tuple[SqlExplorerTui, SqlExplorerEngine]:
    txt_path = tmp_path / "data.txt"
    txt_path.write_text(txt_text, encoding="utf-8")
    engine = SqlExplorerEngine(
        data_path=txt_path,
        table_name="data",
        database=":memory:",
        default_limit=10,
        max_rows_display=100,
        max_value_chars=80,
    )
    return SqlExplorerTui(engine), engine


def _log_text(app: SqlExplorerTui) -> str:
    log = cast(Any, app.query_one("#activity_log", TextArea))
    return str(log.text)


def _preview_text(app: SqlExplorerTui) -> str:
    preview = app.query_one("#results_preview", ResultsPreview)
    rendered = cast(Any, preview.content)
    if isinstance(rendered, Text):
        return rendered.plain
    return str(rendered)


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


def _has_url_color(style: Any) -> bool:
    return URL_COLOR.lower() in str(style).lower()


def _has_null_value_color(style: Any) -> bool:
    return style is not None and NULL_VALUE_COLOR.lower() in str(style).lower()


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


def test_startup_sample_query_is_in_history_immediately(tmp_path: Path) -> None:
    async def run() -> None:
        app, engine = _build_app(tmp_path)
        try:
            async with app.run_test() as pilot:
                editor = app.query_one("#query_editor", SqlQueryEditor)
                await pilot.pause()
                editor.text = ""
                editor.focus()
                await pilot.press("up")
                await pilot.pause()
                assert editor.text == engine.default_query
        finally:
            engine.close()

    asyncio.run(run())


def test_startup_query_sql_is_written_to_activity_for_txt_template(tmp_path: Path) -> None:
    async def run() -> None:
        app, engine = _build_txt_app(tmp_path, txt_text="alpha\nbeta\n")
        try:
            async with app.run_test() as pilot:
                await pilot.pause()
                assert f"[SQL] Executed: {engine.default_query}" in _log_text(app)
        finally:
            engine.close()

    asyncio.run(run())


def test_manual_query_sql_is_written_to_activity(tmp_path: Path) -> None:
    async def run() -> None:
        app, engine = _build_app(tmp_path)
        try:
            async with app.run_test() as pilot:
                editor = app.query_one("#query_editor", SqlQueryEditor)
                await pilot.pause()
                editor.text = "SELECT 1 AS x"
                await pilot.press("ctrl+enter")
                await pilot.pause()
                assert "[SQL] Executed: SELECT 1 AS x" in _log_text(app)
        finally:
            engine.close()

    asyncio.run(run())


def test_slash_helper_query_sql_is_written_to_activity(tmp_path: Path) -> None:
    async def run() -> None:
        app, engine = _build_app(tmp_path)
        try:
            async with app.run_test() as pilot:
                editor = app.query_one("#query_editor", SqlQueryEditor)
                await pilot.pause()
                editor.text = "/sample 2"
                await pilot.press("ctrl+enter")
                await pilot.pause()
                assert '[SQL] Generated: SELECT * FROM "data" LIMIT 2' in _log_text(app)
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


def test_results_header_out_of_uses_full_data_length_when_rows_limit_is_set(tmp_path: Path) -> None:
    async def run() -> None:
        app, engine = _build_app(tmp_path, csv_text="a,b\n1,x\n2,y\n3,z\n4,w\n5,v\n")
        try:
            async with app.run_test() as pilot:
                await pilot.pause()
                editor = app.query_one("#query_editor", SqlQueryEditor)

                editor.text = "/rows 1"
                await pilot.press("ctrl+enter")
                await pilot.pause()

                editor.text = 'SELECT * FROM "data" LIMIT 2'
                await pilot.press("ctrl+enter")
                await pilot.pause()

                header = app.query_one("#results_header", Static)
                assert "Results (1/5 rows," in str(header.render())
        finally:
            engine.close()

    asyncio.run(run())


def test_activity_and_preview_selection_can_be_copied(tmp_path: Path) -> None:
    async def run() -> None:
        app, engine = _build_app(tmp_path)
        try:
            async with app.run_test() as pilot:
                await pilot.pause()

                preview = app.query_one("#results_preview", ResultsPreview)
                preview.update("alpha\nbeta\ngamma")
                preview.focus()
                preview.select_line(1)
                await pilot.press("ctrl+c")
                await pilot.pause()
                assert app.clipboard == "beta"

                activity = app.query_one("#activity_log", TextArea)
                activity.load_text("one\ntwo\nthree")
                activity.focus()
                activity.select_line(1)
                await pilot.press("ctrl+c")
                await pilot.pause()
                assert app.clipboard == "two"
        finally:
            engine.close()

    asyncio.run(run())


def test_activity_and_preview_panes_can_scroll(tmp_path: Path) -> None:
    async def run() -> None:
        app, engine = _build_app(tmp_path)
        try:
            async with app.run_test() as pilot:
                await pilot.pause()
                long_text = "\n".join(f"line {index}" for index in range(160))

                preview = app.query_one("#results_preview", ResultsPreview)
                preview.update(long_text)
                preview.scroll_home(animate=False, immediate=True)
                await pilot.pause()
                preview_start = preview.scroll_y
                preview.action_scroll_down()
                await pilot.pause()
                assert preview.max_scroll_y > 0
                assert preview.scroll_y > preview_start

                activity = app.query_one("#activity_log", TextArea)
                activity.load_text(long_text)
                activity.scroll_home(animate=False, immediate=True)
                await pilot.pause()
                activity_start = activity.scroll_y
                activity.action_scroll_down()
                await pilot.pause()
                assert activity.max_scroll_y > 0
                assert activity.scroll_y > activity_start
        finally:
            engine.close()

    asyncio.run(run())


def test_results_cells_render_http_https_ftp_links_in_blue(tmp_path: Path) -> None:
    async def run() -> None:
        app, engine = _build_app(tmp_path)
        try:
            async with app.run_test() as pilot:
                await pilot.pause()
                editor = app.query_one("#query_editor", SqlQueryEditor)
                editor.text = (
                    "SELECT "
                    "'http://example.com' AS h1, "
                    "'https://example.com/path' AS h2, "
                    "'ftp://example.com/file.txt' AS h3"
                )
                await pilot.press("ctrl+enter")
                await pilot.pause()

                results = cast(DataTable[Any], app.query_one("#results_table", DataTable))
                for column_index in (0, 1, 2):
                    cell = results.get_row_at(0)[column_index]
                    assert isinstance(cell, Text)
                    assert any(_has_url_color(span.style) for span in cell.spans)
        finally:
            engine.close()

    asyncio.run(run())


def test_cell_preview_links_are_clickable_but_results_links_are_not(tmp_path: Path) -> None:
    async def run() -> None:
        app, engine = _build_app(tmp_path)
        try:
            async with app.run_test() as pilot:
                await pilot.pause()
                editor = app.query_one("#query_editor", SqlQueryEditor)
                editor.text = "SELECT 'https://example.com/path' AS url_value"
                await pilot.press("ctrl+enter")
                await pilot.pause()

                results = cast(DataTable[Any], app.query_one("#results_table", DataTable))
                result_cell = results.get_row_at(0)[0]
                assert isinstance(result_cell, Text)
                assert any(_has_url_color(span.style) for span in result_cell.spans)
                assert all(
                    not hasattr(span.style, "meta") or not cast(Any, span.style).meta for span in result_cell.spans
                )

                preview = app.query_one("#results_preview", ResultsPreview)
                rendered_lines = [preview.get_line(index) for index in range(preview.document.line_count)]
                clickable_spans: list[Any] = []
                for line in rendered_lines:
                    clickable_spans.extend(
                        [
                            span
                            for span in line.spans
                            if hasattr(span.style, "meta") and "@click" in cast(Any, span.style).meta
                        ]
                    )
                assert clickable_spans
                assert all(_has_url_color(span.style) for span in clickable_spans)
                assert any("app.open_preview_link(" in span.style.meta["@click"] for span in clickable_spans)
        finally:
            engine.close()

    asyncio.run(run())


def test_preview_pane_preserves_json_highlighting(tmp_path: Path) -> None:
    async def run() -> None:
        app, engine = _build_app(tmp_path)
        try:
            async with app.run_test() as pilot:
                await pilot.pause()
                editor = app.query_one("#query_editor", SqlQueryEditor)
                editor.text = 'SELECT \'{"a":1,"b":{"c":2}}\' AS j'
                await pilot.press("ctrl+enter")
                await pilot.pause()

                preview = app.query_one("#results_preview", ResultsPreview)
                rendered_lines = [preview.get_line(index) for index in range(preview.document.line_count)]
                assert any("json.key" in str(span.style) for line in rendered_lines for span in line.spans)
        finally:
            engine.close()

    asyncio.run(run())


def test_null_value_text_is_darker_in_results_and_preview(tmp_path: Path) -> None:
    async def run() -> None:
        app, engine = _build_app(tmp_path)
        try:
            async with app.run_test() as pilot:
                await pilot.pause()
                editor = app.query_one("#query_editor", SqlQueryEditor)
                editor.text = "SELECT NULL AS n"
                await pilot.press("ctrl+enter")
                await pilot.pause()

                results = cast(DataTable[Any], app.query_one("#results_table", DataTable))
                result_cell = results.get_row_at(0)[0]
                assert isinstance(result_cell, Text)
                assert result_cell.plain == "NULL"
                assert _has_null_value_color(result_cell.style)

                preview = app.query_one("#results_preview", ResultsPreview)
                preview_content = cast(Any, preview.content)
                assert isinstance(preview_content, Text)
                null_start = preview_content.plain.rfind("NULL")
                assert null_start >= 0
                assert any(
                    _has_null_value_color(span.style)
                    for span in preview_content.spans
                    if span.start <= null_start < span.end
                )
        finally:
            engine.close()

    asyncio.run(run())


def test_txt_empty_line_renders_as_empty_not_null_in_results_and_preview(tmp_path: Path) -> None:
    async def run() -> None:
        app, engine = _build_txt_app(tmp_path, "alpha\n\nbeta\n")
        try:
            async with app.run_test() as pilot:
                await pilot.pause()
                results = cast(DataTable[Any], app.query_one("#results_table", DataTable))

                empty_cell = results.get_row_at(1)[0]
                if isinstance(empty_cell, Text):
                    assert empty_cell.plain == ""
                else:
                    assert empty_cell == ""

                results.move_cursor(row=1, column=0)
                await pilot.pause()

                preview = _preview_text(app)
                assert "NULL" not in preview
        finally:
            engine.close()

    asyncio.run(run())


def test_ctrl_b_toggles_data_explorer_sidebar(tmp_path: Path) -> None:
    async def run() -> None:
        app, engine = _build_app(tmp_path)
        try:
            async with app.run_test() as pilot:
                sidebar = app.query_one("#sidebar")
                assert sidebar.display is False

                await pilot.press("ctrl+b")
                await pilot.pause()
                assert sidebar.display is True

                await pilot.press("ctrl+b")
                await pilot.pause()
                assert sidebar.display is False
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


def test_results_selected_cell_adds_subtle_row_indicator(tmp_path: Path) -> None:
    async def run() -> None:
        app, engine = _build_app(tmp_path, csv_text="a,b\n1,x\n2,y\n")
        try:
            async with app.run_test() as pilot:
                await pilot.pause()

                results = cast(DataTable[Any], app.query_one("#results_table", DataTable))
                private_table = cast(Any, results)

                row_indicator = results.get_component_styles("results-table--cursor-row").rich_style
                assert row_indicator.bgcolor is not None

                selected_style = private_table._get_row_style(results.cursor_row, results.rich_style)
                unselected_style = private_table._get_row_style(1, results.rich_style)
                assert selected_style.bgcolor == row_indicator.bgcolor
                assert unselected_style.bgcolor != row_indicator.bgcolor

                results.move_cursor(row=1, column=0)
                await pilot.pause()

                moved_selected_style = private_table._get_row_style(results.cursor_row, results.rich_style)
                moved_unselected_style = private_table._get_row_style(0, results.rich_style)
                assert moved_selected_style.bgcolor == row_indicator.bgcolor
                assert moved_unselected_style.bgcolor != row_indicator.bgcolor
        finally:
            engine.close()

    asyncio.run(run())


def test_results_row_indicator_refreshes_on_cell_cursor_changes(tmp_path: Path) -> None:
    async def run() -> None:
        app, engine = _build_app(tmp_path, csv_text="a,b\n1,x\n2,y\n")
        try:
            async with app.run_test() as pilot:
                await pilot.pause()
                results = cast(DataTable[Any], app.query_one("#results_table", DataTable))

                with patch.object(results, "refresh_row", wraps=results.refresh_row) as refresh_row:
                    results.move_cursor(row=1, column=0)
                    await pilot.pause()
                    refreshed_rows = [cast(int, call.args[0]) for call in refresh_row.call_args_list]
                    assert 0 in refreshed_rows
                    assert 1 in refreshed_rows

                    refresh_row.reset_mock()
                    results.move_cursor(row=1, column=1)
                    await pilot.pause()
                    refreshed_rows = [cast(int, call.args[0]) for call in refresh_row.call_args_list]
                    assert 1 in refreshed_rows

                    refresh_row.reset_mock()
                    results.move_cursor(row=1, column=1)
                    await pilot.pause()
                    refreshed_rows = [cast(int, call.args[0]) for call in refresh_row.call_args_list]
                    assert refreshed_rows == [1]
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


def test_json_highlighting_applies_to_struct_column(tmp_path: Path) -> None:
    async def run() -> None:
        app, engine = _build_app(tmp_path)
        try:
            async with app.run_test() as pilot:
                await pilot.pause()
                editor = app.query_one("#query_editor", SqlQueryEditor)
                editor.text = "SELECT {'a': 1, 'b': {'c': 2}} AS s"
                await pilot.press("ctrl+enter")
                await pilot.pause()
                results = cast(DataTable[Any], app.query_one("#results_table", DataTable))
                cell = results.get_row_at(0)[0]
                assert isinstance(cell, Text)
                assert cell.plain == '{"a":1,"b":{"c":2}}'
                assert any("json.key" in str(span.style) for span in cell.spans)
        finally:
            engine.close()

    asyncio.run(run())


def test_struct_image_bytes_render_compact_token_and_preview_metadata(tmp_path: Path) -> None:
    async def run() -> None:
        app, engine = _build_app(tmp_path)
        try:
            async with app.run_test() as pilot:
                await pilot.pause()
                editor = app.query_one("#query_editor", SqlQueryEditor)
                editor.text = (
                    "SELECT {'bytes': from_hex("
                    "'89504E470D0A1A0A0000000D49484452000000010000000108060000001F15C489"
                    "0000000A49444154789C6360000000020001E221BC330000000049454E44AE426082"
                    "'), 'path': NULL} AS image_blob"
                )
                await pilot.press("ctrl+enter")
                await pilot.pause()
                results = cast(DataTable[Any], app.query_one("#results_table", DataTable))
                cell = results.get_row_at(0)[0]
                assert isinstance(cell, Text)
                assert cell.plain == "[img png 1x1 67 B]"

                preview_text = _preview_text(app)
                assert "image_blob, STRUCT, row 1, col 1" in preview_text
                assert "format: png" in preview_text
                assert "dimensions: 1x1" in preview_text
                assert "size: 67 B (67 bytes)" in preview_text
                assert "source: struct.bytes" in preview_text
                assert "raw:" in preview_text
                assert "'bytes': b'\\x89PNG\\r\\n\\x1a\\n" in preview_text
        finally:
            engine.close()

    asyncio.run(run())


def test_struct_image_bytes_show_raw_value_when_json_view_is_off(tmp_path: Path) -> None:
    async def run() -> None:
        app, engine = _build_app(tmp_path)
        try:
            async with app.run_test() as pilot:
                await pilot.pause()
                editor = app.query_one("#query_editor", SqlQueryEditor)
                editor.text = "SELECT {'bytes': from_hex('FFD8FFE000104A4649460001FFD9'), 'path': NULL} AS image_blob"
                await pilot.press("ctrl+enter")
                await pilot.pause()

                results = cast(DataTable[Any], app.query_one("#results_table", DataTable))
                on_cell = results.get_row_at(0)[0]
                assert isinstance(on_cell, Text)
                assert on_cell.plain.startswith("[img jpeg")

                await pilot.press("f3")
                await pilot.pause()

                off_cell = results.get_row_at(0)[0]
                assert isinstance(off_cell, str)
                assert off_cell.startswith("{'bytes': b'")
                assert "[img " not in off_cell

                preview_text = _preview_text(app)
                assert "[image]" not in preview_text
                assert "{'bytes': b'" in preview_text
        finally:
            engine.close()

    asyncio.run(run())


def test_json_highlighting_keeps_highlighting_when_json_is_truncated(tmp_path: Path) -> None:
    async def run() -> None:
        csv_text = (
            "id,j\n"
            '1,"{""a"":1,""b"":2,""c"":3,""d"":4,""e"":5,""f"":6}"\n'
            '2,"{""a"":7,""b"":8,""c"":9,""d"":10,""e"":11,""f"":12}"\n'
            '3,"{""a"":13,""b"":14,""c"":15,""d"":16,""e"":17,""f"":18}"\n'
        )
        app, engine = _build_app(tmp_path, csv_text=csv_text)
        engine.max_value_chars = 20
        try:
            async with app.run_test() as pilot:
                await pilot.pause()
                results = cast(DataTable[Any], app.query_one("#results_table", DataTable))
                cell = results.get_row_at(0)[1]
                assert isinstance(cell, Text)
                assert cell.plain.endswith("...")
                assert any("json.key" in str(span.style) for span in cell.spans)
        finally:
            engine.close()

    asyncio.run(run())


def test_json_highlighting_keeps_highlighting_when_struct_is_truncated(tmp_path: Path) -> None:
    async def run() -> None:
        app, engine = _build_app(tmp_path)
        engine.max_value_chars = 40
        try:
            async with app.run_test() as pilot:
                await pilot.pause()
                editor = app.query_one("#query_editor", SqlQueryEditor)
                editor.text = "SELECT {'a': 1, 'long_key_name': {'c': 2, 'd': 3, 'e': 4}, 'tail': 'abcdef'} AS s"
                await pilot.press("ctrl+enter")
                await pilot.pause()
                results = cast(DataTable[Any], app.query_one("#results_table", DataTable))
                cell = results.get_row_at(0)[0]
                assert isinstance(cell, Text)
                assert cell.plain.endswith("...")
                assert any("json.key" in str(span.style) for span in cell.spans)
        finally:
            engine.close()

    asyncio.run(run())


def test_json_highlighting_keeps_truncated_array_string_tail_styled(tmp_path: Path) -> None:
    async def run() -> None:
        long_json = (
            '["vertex_ai|gemini-api","vertex_ai|gemini-api","vertex_ai|gemini-api",'
            '"vertex_ai|gemini-api","vertex_ai|gemini-api","vertex_ai|gemini-api","vertex_ai|gemini-api"]'
        )
        escaped_json = long_json.replace('"', '""')
        csv_text = f'id,j\n1,"{escaped_json}"\n2,"{escaped_json}"\n3,"{escaped_json}"\n'
        app, engine = _build_app(tmp_path, csv_text=csv_text)
        engine.max_value_chars = 130
        try:
            async with app.run_test() as pilot:
                await pilot.pause()
                results = cast(DataTable[Any], app.query_one("#results_table", DataTable))
                cell = results.get_row_at(0)[1]
                assert isinstance(cell, Text)
                assert cell.plain.endswith("...")
                end_offset = len(cell.plain) - 1
                assert any("json.str" in str(span.style) and span.start <= end_offset < span.end for span in cell.spans)
        finally:
            engine.close()

    asyncio.run(run())


def test_json_highlighting_decodes_escaped_json_object_strings_in_varchar(tmp_path: Path) -> None:
    async def run() -> None:
        app, engine = _build_app(tmp_path)
        engine.max_value_chars = 500
        escaped_schema_json = (
            '["{\\"type\\":\\"object\\",\\"properties\\":{\\"channel\\":{\\"type\\":\\"string\\"}}}",'
            '"{\\"type\\":\\"object\\",\\"properties\\":{\\"id\\":{\\"type\\":\\"integer\\"}}}"]'
        )
        try:
            async with app.run_test() as pilot:
                await pilot.pause()
                editor = app.query_one("#query_editor", SqlQueryEditor)
                editor.text = (
                    "SELECT * FROM (VALUES "
                    f"('{escaped_schema_json}'), ('{escaped_schema_json}'), ('{escaped_schema_json}')"
                    ") AS t(schema_json)"
                )
                await pilot.press("ctrl+enter")
                await pilot.pause()
                results = cast(DataTable[Any], app.query_one("#results_table", DataTable))
                cell = results.get_row_at(0)[0]
                assert isinstance(cell, Text)
                assert '[{"type":"object"' in cell.plain
                assert '\\"type\\"' not in cell.plain
                assert any("json.key" in str(span.style) for span in cell.spans)
        finally:
            engine.close()

    asyncio.run(run())


def test_results_preview_shows_pretty_struct_and_updates_with_cursor(tmp_path: Path) -> None:
    async def run() -> None:
        app, engine = _build_app(tmp_path)
        try:
            async with app.run_test() as pilot:
                await pilot.pause()
                editor = app.query_one("#query_editor", SqlQueryEditor)
                editor.text = "SELECT {'a': 1, 'deep': {'b': 2}} AS s, 'plain' AS note"
                await pilot.press("ctrl+enter")
                await pilot.pause()

                preview_text = _preview_text(app)
                assert '"a": 1' in preview_text
                assert '"deep": {' in preview_text
                assert '"b": 2' in preview_text
                assert "s, STRUCT, row 1, col 1" in preview_text

                await pilot.press("ctrl+2")
                await pilot.pause()
                await pilot.press("right")
                await pilot.pause()

                preview_text = _preview_text(app)
                assert "note, VARCHAR, row 1, col 2" in preview_text
                assert "plain" in preview_text
        finally:
            engine.close()

    asyncio.run(run())


def test_f2_copies_full_selected_cell_value(tmp_path: Path) -> None:
    async def run() -> None:
        app, engine = _build_app(tmp_path)
        engine.max_value_chars = 30
        try:
            async with app.run_test() as pilot:
                await pilot.pause()
                editor = app.query_one("#query_editor", SqlQueryEditor)
                editor.text = "SELECT {'very_long_key_name': 'abcdefghijklmnopqrstuvwxyz', 'nested': {'x': 1}} AS s"
                await pilot.press("ctrl+enter")
                await pilot.pause()

                results = cast(DataTable[Any], app.query_one("#results_table", DataTable))
                rendered = results.get_row_at(0)[0]
                assert isinstance(rendered, Text)
                assert rendered.plain.endswith("...")

                await pilot.press("ctrl+2")
                await pilot.pause()
                await pilot.press("f2")
                await pilot.pause()

                copied = app.clipboard
                assert '"very_long_key_name": "abcdefghijklmnopqrstuvwxyz"' in copied
                assert '"nested": {' in copied
                assert '"x": 1' in copied
                assert "Copied full cell value" in _log_text(app)
                assert '"very_long_key_name": "abcdefghijklmnopqrstuvwxyz"' in _preview_text(app)
        finally:
            engine.close()

    asyncio.run(run())


def test_results_preview_tracks_correct_row_value_on_cursor_move(tmp_path: Path) -> None:
    async def run() -> None:
        app, engine = _build_app(tmp_path)
        try:
            async with app.run_test() as pilot:
                await pilot.pause()
                editor = app.query_one("#query_editor", SqlQueryEditor)
                editor.text = (
                    "SELECT {'row_name': 'first', 'x': 1} AS s UNION ALL SELECT {'row_name': 'second', 'x': 2} AS s"
                )
                await pilot.press("ctrl+enter")
                await pilot.pause()

                assert '"row_name": "first"' in _preview_text(app)
                assert '"x": 1' in _preview_text(app)

                await pilot.press("ctrl+2")
                await pilot.pause()
                await pilot.press("down")
                await pilot.pause()

                preview_text = _preview_text(app)
                assert '"row_name": "second"' in preview_text
                assert '"x": 2' in preview_text
                assert '"row_name": "first"' not in preview_text
        finally:
            engine.close()

    asyncio.run(run())


def test_results_preview_struct_uses_value_not_schema_type_text(tmp_path: Path) -> None:
    async def run() -> None:
        app, engine = _build_app(tmp_path)
        try:
            async with app.run_test() as pilot:
                await pilot.pause()
                editor = app.query_one("#query_editor", SqlQueryEditor)
                editor.text = (
                    "SELECT {'property_channel': 'rent', 'property_location': "
                    "[{'state': 'NSW', 'suburb': 'Macquarie Park'}]} AS property_filters"
                )
                await pilot.press("ctrl+enter")
                await pilot.pause()

                preview = _preview_text(app)
                assert "property_filters, STRUCT, row 1, col 1" in preview
                assert '"property_channel": "rent"' in preview
                assert '"state": "NSW"' in preview
                assert '"suburb": "Macquarie Park"' in preview
        finally:
            engine.close()

    asyncio.run(run())


def test_results_preview_formats_json_varchar_with_indent(tmp_path: Path) -> None:
    async def run() -> None:
        csv_text = 'id,j\n1,"{""a"":1,""b"":{""c"":2}}"\n'
        app, engine = _build_app(tmp_path, csv_text=csv_text)
        try:
            async with app.run_test() as pilot:
                await pilot.pause()
                await pilot.press("ctrl+2")
                await pilot.pause()
                await pilot.press("right")
                await pilot.pause()

                preview = _preview_text(app)
                assert "j, VARCHAR, row 1, col 2" in preview
                assert '"a": 1' in preview
                assert '"b": {' in preview
                assert '  "c": 2' in preview
        finally:
            engine.close()

    asyncio.run(run())


def test_f3_toggles_results_json_rendering_and_header_status(tmp_path: Path) -> None:
    async def run() -> None:
        csv_text = 'id,j\n1,"{""a"":1}"\n2,"{""a"":2}"\n3,"{""a"":3}"\n'
        app, engine = _build_app(tmp_path, csv_text=csv_text)
        try:
            async with app.run_test() as pilot:
                await pilot.pause()
                results = cast(DataTable[Any], app.query_one("#results_table", DataTable))
                header = app.query_one("#results_header", Static)

                assert isinstance(results.get_row_at(0)[1], Text)
                assert "[json:on]" in str(header.render())

                await pilot.press("f3")
                await pilot.pause()
                assert isinstance(results.get_row_at(0)[1], str)
                assert "[json:off]" in str(header.render())

                await pilot.press("f3")
                await pilot.pause()
                assert isinstance(results.get_row_at(0)[1], Text)
                assert "[json:on]" in str(header.render())
        finally:
            engine.close()

    asyncio.run(run())


def test_f3_toggles_preview_json_formatting_for_struct(tmp_path: Path) -> None:
    async def run() -> None:
        app, engine = _build_app(tmp_path)
        try:
            async with app.run_test() as pilot:
                await pilot.pause()
                editor = app.query_one("#query_editor", SqlQueryEditor)
                editor.text = "SELECT {'a': 1, 'b': {'c': 2}} AS s"
                await pilot.press("ctrl+enter")
                await pilot.pause()

                preview = _preview_text(app)
                assert '"a": 1' in preview
                assert '"b": {' in preview

                await pilot.press("f3")
                await pilot.pause()
                preview = _preview_text(app)
                assert "{'a': 1, 'b': {'c': 2}}" in preview
                assert '"a": 1' not in preview
        finally:
            engine.close()

    asyncio.run(run())


def test_preview_render_value_highlights_json_for_struct_and_varchar(tmp_path: Path) -> None:
    app, engine = _build_app(tmp_path)
    try:
        private_app = cast(Any, app)
        varchar_rendered = private_app._render_preview_value('{"a":1,"b":{"c":2}}', "VARCHAR")
        struct_rendered = private_app._render_preview_value(
            {"a": 1, "b": {"c": 2}},
            "STRUCT(a INTEGER, b STRUCT(c INTEGER))",
        )
        assert any("json.key" in str(span.style) for span in varchar_rendered.spans)
        assert any("json.key" in str(span.style) for span in struct_rendered.spans)

        private_app._json_rendering_enabled = False
        no_highlight = private_app._render_preview_value('{"a":1}', "VARCHAR")
        assert no_highlight.spans == []
    finally:
        engine.close()


def test_completion_menu_auto_opens_for_prefix_and_down_navigates(tmp_path: Path) -> None:
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
                initial_highlight = menu.highlighted
                assert initial_highlight is not None

                await pilot.press("down")
                await pilot.pause()
                assert menu.display is True
                assert menu.highlighted == (initial_highlight + 1) % menu.option_count
        finally:
            engine.close()

    asyncio.run(run())


def test_completion_menu_auto_opens_for_select_space_then_reopens_with_ctrl_space(tmp_path: Path) -> None:
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
                assert menu.display is True
                assert menu.option_count > 0

                await pilot.press("escape")
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


def test_completion_menu_auto_opens_for_helper_argument_boundary(tmp_path: Path) -> None:
    async def run() -> None:
        app, engine = _build_app(tmp_path)
        try:
            async with app.run_test() as pilot:
                editor = app.query_one("#query_editor", SqlQueryEditor)
                menu = app.query_one("#completion_menu", OptionList)

                editor.text = ""
                editor.focus()
                await pilot.pause()
                await pilot.press("/", "t", "o", "p", "space")
                await pilot.pause()
                assert menu.display is True
                assert menu.option_count > 0
        finally:
            engine.close()

    asyncio.run(run())


def test_completion_up_uses_history_only_when_menu_hidden(tmp_path: Path) -> None:
    async def run() -> None:
        app, engine = _build_app(tmp_path)
        try:
            async with app.run_test() as pilot:
                editor = app.query_one("#query_editor", SqlQueryEditor)
                menu = app.query_one("#completion_menu", OptionList)

                editor.focus()
                editor.text = "SELECT * FROM data LIMIT 1"
                await pilot.pause()
                await pilot.press("ctrl+enter")
                await pilot.pause()

                editor.text = ""
                editor.focus()
                await pilot.pause()
                await pilot.press("/", "s")
                await pilot.pause()
                assert menu.display is True

                await pilot.press("up")
                await pilot.pause()
                assert menu.display is True
                assert editor.text == "/s"

                await pilot.press("escape")
                await pilot.pause()
                assert menu.display is False

                await pilot.press("up")
                await pilot.pause()
                assert editor.text != "/s"
        finally:
            engine.close()

    asyncio.run(run())


def test_completion_hint_visibility_tracks_menu_display(tmp_path: Path) -> None:
    async def run() -> None:
        app, engine = _build_app(tmp_path)
        try:
            async with app.run_test() as pilot:
                editor = app.query_one("#query_editor", SqlQueryEditor)
                hint = app.query_one("#completion_hint", Static)
                menu = app.query_one("#completion_menu", OptionList)

                assert hint.display is False
                editor.text = ""
                editor.focus()
                await pilot.pause()
                await pilot.press("/", "s")
                await pilot.pause()
                assert menu.display is True
                assert hint.display is True

                await pilot.press("escape")
                await pilot.pause()
                assert menu.display is False
                assert hint.display is False
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


def test_completion_menu_focus_keeps_options_for_click_selection(tmp_path: Path) -> None:
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

                menu.focus()
                await pilot.pause()
                assert menu.display is True
                assert menu.option_count > 0
        finally:
            engine.close()

    asyncio.run(run())


def test_completion_menu_highlight_then_select_uses_clicked_window_item(tmp_path: Path) -> None:
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

                await pilot.press("ctrl+space")
                await pilot.pause()
                for _ in range(10):
                    await pilot.press("down")
                    await pilot.pause()

                clicked_index = 2
                option = menu.get_option_at_index(clicked_index)
                expected = str(option.prompt).split("  [", 1)[0]
                app.on_option_list_option_highlighted(OptionList.OptionHighlighted(menu, option, clicked_index))
                await pilot.pause()
                app.on_option_list_option_selected(OptionList.OptionSelected(menu, option, clicked_index))
                await pilot.pause()

                assert editor.text == expected
                assert menu.display is False
        finally:
            engine.close()

    asyncio.run(run())


def test_completion_prompt_for_helper_shows_description_then_usage() -> None:
    item = CompletionItem(
        label="/top",
        insert_text="/top",
        kind="helper_command",
        detail="Top values by frequency for a column.",
        usage="/top <column> <n>",
    )
    prompt = SqlExplorerTui.completion_option_prompt(item)
    assert prompt.startswith("/top  [helper command] Top values by frequency for a column.")
    assert "Usage: /top <column> <n>" in prompt
    assert prompt.index("Top values by frequency for a column.") < prompt.index("Usage: /top <column> <n>")


def test_completion_prompt_for_helper_without_args_omits_usage() -> None:
    item = CompletionItem(
        label="/help",
        insert_text="/help",
        kind="helper_command",
        detail="Show helper command reference.",
        usage="/help",
    )
    prompt = SqlExplorerTui.completion_option_prompt(item)
    assert prompt == "/help  [helper command] Show helper command reference."
    assert "Usage:" not in prompt


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


def test_query_editor_preserves_trailing_spaces_for_helper_commands() -> None:
    editor = SqlQueryEditor("/top ", lambda: [], lambda: None, lambda: None)
    line = editor.get_line(0)
    assert line.plain == "/top "
