import asyncio
import csv
from dataclasses import dataclass
from io import StringIO
from typing import Any, Sequence, cast

from rich.highlighter import JSONHighlighter, ReprHighlighter
from rich.text import Text
from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.events import Key
from textual.widget import Widget
from textual.widgets import DataTable, Footer, Header, OptionList, Static, TextArea

from sqlexplore.completion.models import CompletionItem
from sqlexplore.core.engine import (
    EngineResponse,
    QueryResult,
    ResultStatus,
    SqlExplorerEngine,
    app_version,
    format_scalar,
    is_struct_type_name,
    is_varchar_type,
    sort_cell_key,
)
from sqlexplore.core.logging_utils import get_logger, truncate_for_log
from sqlexplore.ui.activity_log import ActivityLog
from sqlexplore.ui.image_cells import format_image_cell_token, format_image_preview_metadata, summarize_image_cell
from sqlexplore.ui.pane_splitter import PaneSplitter
from sqlexplore.ui.query_editor import SqlQueryEditor
from sqlexplore.ui.results_preview import ResultsPreview
from sqlexplore.ui.results_table import ResultsTable
from sqlexplore.ui.tui_shared import (
    DETAIL_PREVIEW_MAX_CHARS,
    DETAIL_PREVIEW_MAX_LINES,
    FIXED_HEIGHT_PANES,
    INLINE_CELL_PREVIEW_MAX_CHARS,
    INLINE_CELL_PREVIEW_MAX_LINES,
    JSON_HIGHLIGHT_MAX_TOTAL_ROWS,
    MAX_ACTIVITY_LOG_CHARS,
    NULL_VALUE_STYLE,
    PANE_ACTIVITY,
    PANE_CELL_DETAIL,
    PANE_ORDER,
    PANE_QUERY,
    PANE_RESULTS,
    SPLITTER_PANE_PAIRS,
    CellValue,
    JsonArray,
    JsonObject,
    PaneId,
    PaneResizePhase,
    RenderedCell,
    build_shortcuts,
    clamp_preview_text,
    column_looks_like_json,
    compact_json_cell,
    pretty_json_cell,
    preview_type_label,
    stylize_links,
    truncate_highlighted_text,
)

logger = get_logger(__name__)
PREVIEW_HEADER_STYLE = "bold #7AB6E8"


@dataclass(slots=True)
class _PaneResizeSession:
    splitter_index: int
    base_heights: dict[PaneId, int]
    did_drag: bool = False


class SqlExplorerTui(App[None]):
    TITLE = f"sqlexplorer v{app_version()}"
    SUB_TITLE = "explore your data"
    QUERY_PANE_MIN_HEIGHT = 5
    RESULTS_PANE_MIN_HEIGHT = 5
    CELL_DETAIL_PANE_MIN_HEIGHT = 5
    ACTIVITY_PANE_MIN_HEIGHT = 5
    PREVIEW_JSON_PRETTY_MAX_STRING_CHARS = 16_384
    PREVIEW_JSON_PRETTY_MAX_CONTAINER_ITEMS = 500
    COLUMN_RESIZE_SYNC_REFRESH_MAX_ROWS = 5_000
    COLUMN_RESIZE_VISIBLE_ROW_BUFFER = 20
    PREVIEW_DEFAULT_TEXT = "Move in Results to preview selected cell. F2 copies full value. F4 opens full cell view."

    CSS = """
    Screen {
        layout: vertical;
    }

    #body {
        height: 1fr;
    }

    #sidebar {
        width: 38;
        border: round #4f9da6;
        padding: 1;
        background: #102025;
        color: #f0f4f8;
    }

    #workspace {
        width: 1fr;
        padding: 0 1;
    }

    .section-title {
        color: #9abed8;
        text-style: bold;
        margin-top: 0;
    }

    #query_editor {
        height: 5;
        min-height: 5;
        border: round #4d7ea8;
    }

    #completion_menu {
        display: none;
        max-height: 8;
        border: round #4d7ea8;
        margin-bottom: 0;
        background: #0f1b22;
    }

    #completion_hint {
        display: none;
        color: #9abed8;
        margin: 0 0 1 1;
    }

    #results_table {
        height: 1fr;
        min-height: 5;
        border: round #5b8f67;
    }

    #results_preview {
        height: 5;
        min-height: 5;
        border: round #5b8f67;
        color: #d8e3ec;
        overflow-y: scroll;
    }

    #activity_log {
        height: 5;
        min-height: 5;
        border: round #b18b3d;
        overflow-y: scroll;
    }

    .pane-splitter {
        height: 1;
        background: $background;
    }
    """

    BINDINGS = build_shortcuts(for_editor=False)

    def __init__(
        self,
        engine: SqlExplorerEngine,
        startup_activity_messages: list[str] | None = None,
        startup_query: str | None = None,
        log_file_path: str | None = None,
    ) -> None:
        super().__init__()
        self.engine = engine
        self._startup_activity_messages = tuple(startup_activity_messages or [])
        self._startup_query = startup_query if startup_query is not None else self.engine.default_query
        self._log_file_path = log_file_path
        self._history_cursor: int | None = None
        self._active_result: QueryResult | None = None
        self._base_rows: list[tuple[CellValue, ...]] = []
        self._sort_column_index: int | None = None
        self._sort_reverse = False
        self._active_data_total_rows: int | None = None
        self._last_results_tsv = ""
        self._results_preview_plain_text = ""
        self._activity_lines: list[str] = []
        self._json_highlighter = JSONHighlighter()
        self._repr_highlighter = ReprHighlighter()
        self._json_rendering_enabled = True
        self._active_json_columns: set[int] = set()
        self._query_task: asyncio.Task[None] | None = None
        self._results_column_width_overrides: dict[int, int] = {}
        self._pane_resize_session: _PaneResizeSession | None = None
        self._fixed_pane_heights: dict[PaneId, int] = {
            PANE_QUERY: self.QUERY_PANE_MIN_HEIGHT,
            PANE_CELL_DETAIL: self.CELL_DETAIL_PANE_MIN_HEIGHT,
            PANE_ACTIVITY: self.ACTIVITY_PANE_MIN_HEIGHT,
        }

    def _reset_history_cursor(self) -> None:
        self._history_cursor = None

    def _history_prev(self) -> str | None:
        history = self.engine.query_history
        if not history:
            return None
        if self._history_cursor is None:
            self._history_cursor = len(history) - 1
        elif self._history_cursor > 0:
            self._history_cursor -= 1
        return history[self._history_cursor].query_text

    def _history_next(self) -> str | None:
        history = self.engine.query_history
        if not history:
            return None
        if self._history_cursor is None:
            self._history_cursor = 0
        elif self._history_cursor < len(history) - 1:
            self._history_cursor += 1
        return history[self._history_cursor].query_text

    def _results_table(self) -> ResultsTable:
        return self.query_one("#results_table", ResultsTable)

    def _query_editor(self) -> SqlQueryEditor:
        return self.query_one("#query_editor", SqlQueryEditor)

    def _completion_menu(self) -> OptionList:
        return self.query_one("#completion_menu", OptionList)

    def _completion_hint(self) -> Static:
        return self.query_one("#completion_hint", Static)

    def _results_preview(self) -> ResultsPreview:
        return self.query_one("#results_preview", ResultsPreview)

    def _activity_log(self) -> ActivityLog:
        return self.query_one("#activity_log", ActivityLog)

    @classmethod
    def _pane_min_height(cls, pane: PaneId) -> int:
        if pane == PANE_QUERY:
            return cls.QUERY_PANE_MIN_HEIGHT
        if pane == PANE_RESULTS:
            return cls.RESULTS_PANE_MIN_HEIGHT
        if pane == PANE_CELL_DETAIL:
            return cls.CELL_DETAIL_PANE_MIN_HEIGHT
        return cls.ACTIVITY_PANE_MIN_HEIGHT

    def _fixed_height_pane_widget(self, pane: PaneId) -> Widget:
        if pane == PANE_QUERY:
            return self._query_editor()
        if pane == PANE_CELL_DETAIL:
            return self._results_preview()
        assert pane == PANE_ACTIVITY, f"unsupported fixed-height pane: {pane}"
        return self._activity_log()

    def _set_fixed_height_pane(self, pane: PaneId, height: int) -> None:
        clamped_height = max(self._pane_min_height(pane), height)
        self._fixed_pane_heights[pane] = clamped_height
        self._fixed_height_pane_widget(pane).styles.height = clamped_height

    def _pane_heights(self) -> dict[PaneId, int]:
        heights = dict(self._fixed_pane_heights)
        heights[PANE_RESULTS] = max(self.RESULTS_PANE_MIN_HEIGHT, self._results_table().size.height)
        return heights

    @staticmethod
    def _sorted_by_height_desc(heights: dict[PaneId, int], panes: Sequence[PaneId]) -> list[PaneId]:
        return sorted(panes, key=lambda pane: (-heights[pane], PANE_ORDER.index(pane)))

    @classmethod
    def _pane_shrink_capacity(cls, heights: dict[PaneId, int], pane: PaneId) -> int:
        return max(0, heights[pane] - cls._pane_min_height(pane))

    def _donor_order_for_growth(
        self,
        heights: dict[PaneId, int],
        target: PaneId,
        preferred_donor: PaneId | None,
    ) -> list[PaneId]:
        donors: list[PaneId] = [pane for pane in PANE_ORDER if pane != target]
        if target != PANE_RESULTS:
            ordered: list[PaneId] = []
            if PANE_RESULTS in donors:
                ordered.append(PANE_RESULTS)
                donors.remove(PANE_RESULTS)
            ordered.extend(self._sorted_by_height_desc(heights, donors))
            return ordered

        ordered_results: list[PaneId] = []
        if preferred_donor is not None and preferred_donor in donors:
            ordered_results.append(preferred_donor)
            donors.remove(preferred_donor)
        ordered_results.extend(self._sorted_by_height_desc(heights, donors))
        return ordered_results

    def _grow_pane(
        self,
        heights: dict[PaneId, int],
        target: PaneId,
        amount: int,
        preferred_donor: PaneId | None = None,
    ) -> dict[PaneId, int]:
        if amount <= 0:
            return heights
        resized = dict(heights)
        remaining = amount
        for donor in self._donor_order_for_growth(resized, target, preferred_donor):
            if remaining <= 0:
                break
            capacity = self._pane_shrink_capacity(resized, donor)
            if capacity <= 0:
                continue
            transfer = min(capacity, remaining)
            resized[donor] -= transfer
            remaining -= transfer
        resized[target] += amount - remaining
        return resized

    def _target_for_splitter_delta(self, splitter_index: int, delta: int) -> tuple[PaneId, int, PaneId | None] | None:
        if delta == 0 or splitter_index < 0 or splitter_index >= len(SPLITTER_PANE_PAIRS):
            return None
        upper, lower = SPLITTER_PANE_PAIRS[splitter_index]
        if delta > 0:
            target = upper
            preferred_donor = lower if target == PANE_RESULTS else None
            return target, delta, preferred_donor
        target = lower
        preferred_donor = upper if target == PANE_RESULTS else None
        return target, -delta, preferred_donor

    def _apply_pane_heights(self, heights: dict[PaneId, int]) -> None:
        for pane in FIXED_HEIGHT_PANES:
            self._set_fixed_height_pane(pane, heights[pane])

    def _apply_pane_resize_delta(self, splitter_index: int, delta: int) -> None:
        session = self._pane_resize_session
        if session is None or session.splitter_index != splitter_index:
            return
        target = self._target_for_splitter_delta(splitter_index, delta)
        if target is None:
            return
        pane, amount, preferred_donor = target
        resized = self._grow_pane(session.base_heights, pane, amount, preferred_donor)
        self._apply_pane_heights(resized)
        if delta != 0:
            session.did_drag = True

    def _refresh_panes_after_resize(self) -> None:
        for widget in (self._results_table(), self._results_preview(), self._activity_log(), self._query_editor()):
            widget.refresh(layout=True)

    def _start_pane_resize_session(self, splitter_index: int) -> None:
        self._pane_resize_session = _PaneResizeSession(
            splitter_index=splitter_index,
            base_heights=self._pane_heights(),
        )

    def _finish_pane_resize_session(self) -> bool:
        session = self._pane_resize_session
        self._pane_resize_session = None
        return session is not None and session.did_drag

    def _on_pane_splitter_resize(self, splitter_index: int, phase: PaneResizePhase, delta: int) -> None:
        if phase == "start":
            self._start_pane_resize_session(splitter_index)
            return
        if phase == "update":
            self._apply_pane_resize_delta(splitter_index, delta)
            return
        self._apply_pane_resize_delta(splitter_index, delta)
        if self._finish_pane_resize_session():
            self._refresh_panes_after_resize()

    def _set_results_loading(self, loading: bool) -> None:
        if not self.screen.is_mounted:
            return
        self._results_table().loading = loading

    @staticmethod
    def completion_option_prompt(item: CompletionItem) -> str:
        kind = item.kind.replace("_", " ")
        prompt = f"{item.insert_text}  [{kind}]"
        if item.kind == "helper_command":
            detail_parts: list[str] = []
            if item.detail:
                detail_parts.append(item.detail)
            if item.usage and ("<" in item.usage or "[" in item.usage):
                detail_parts.append(f"Usage: {item.usage}")
            if detail_parts:
                return f"{prompt} {' '.join(detail_parts)}"
            return prompt
        if item.detail:
            return f"{prompt} {item.detail}"
        return prompt

    def _on_editor_completion_changed(
        self,
        items: list[CompletionItem],
        selected_index: int,
        is_open: bool,
    ) -> None:
        if not self.screen.is_mounted:
            return
        menu = self._completion_menu()
        hint = self._completion_hint()
        if not is_open or not items:
            menu.display = False
            menu.clear_options()
            hint.display = False
            return
        selected = min(max(0, selected_index), len(items) - 1)
        prompts = [self.completion_option_prompt(item) for item in items]
        menu.set_options(prompts)
        menu.highlighted = selected
        menu.display = True
        hint.display = True

    def _on_editor_completion_accepted(self, item: CompletionItem) -> None:
        self.engine.record_completion_acceptance(item.insert_text)
        self._on_editor_completion_changed([], 0, False)

    def on_option_list_option_highlighted(self, event: OptionList.OptionHighlighted) -> None:
        if event.option_list.id != "completion_menu":
            return
        index = self._completion_index_from_menu_option(event.option_index)
        if index is None:
            return
        # Avoid re-windowing the menu during mouse highlight/select event chains.
        self._query_editor().set_completion_index(index, notify=False)

    def on_option_list_option_selected(self, event: OptionList.OptionSelected) -> None:
        if event.option_list.id != "completion_menu":
            return
        index = self._completion_index_from_menu_option(event.option_index)
        if index is None:
            return
        editor = self._query_editor()
        if editor.accept_completion_at_index(index):
            editor.focus()

    def _completion_index_from_menu_option(self, option_index: int) -> int | None:
        menu = self._completion_menu()
        if option_index < 0 or option_index >= menu.option_count:
            return None
        return option_index

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Horizontal(id="body"):
            with Vertical(id="sidebar"):
                yield Static(self.engine.schema_preview(), id="sidebar_text")
            with Vertical(id="workspace"):
                yield Static("Query or command", classes="section-title")
                yield SqlQueryEditor(
                    self._startup_query,
                    self.engine.completion_tokens,
                    self._history_prev,
                    self._history_next,
                    completion_provider=self.engine.completion_result,
                    helper_command_provider=self.engine.helper_commands,
                    completion_changed=self._on_editor_completion_changed,
                    completion_accepted=self._on_editor_completion_accepted,
                    id="query_editor",
                )
                yield OptionList(id="completion_menu")
                yield Static("Tab accept | Esc close | Up/Down navigate", id="completion_hint")
                yield PaneSplitter(
                    0,
                    self._on_pane_splitter_resize,
                    id="pane_splitter_query_results",
                    classes="pane-splitter",
                )
                yield Static("Results", id="results_header", classes="section-title")
                yield ResultsTable(id="results_table", on_column_resized=self._on_results_column_resized)
                yield PaneSplitter(
                    1,
                    self._on_pane_splitter_resize,
                    id="pane_splitter_results_cell",
                    classes="pane-splitter",
                )
                yield ResultsPreview("", id="results_preview")
                yield PaneSplitter(
                    2,
                    self._on_pane_splitter_resize,
                    id="pane_splitter_cell_activity",
                    classes="pane-splitter",
                )
                yield ActivityLog(
                    "",
                    id="activity_log",
                )
        yield Footer()

    async def on_mount(self) -> None:
        table = self._results_table()
        table.zebra_stripes = True
        self.query_one("#sidebar", Vertical).display = False
        self._completion_menu().display = False
        self._completion_hint().display = False
        self._set_results_loading(False)
        self._set_results_preview_text(self.PREVIEW_DEFAULT_TEXT)
        self._log(f"sqlexplore {app_version()}", "info")
        self._log(f"Log file: {self._log_file_path or 'unavailable'}", "info")
        for message in self._startup_activity_messages:
            self._log(message, "info")
        self._log("Ready. Press Ctrl+Enter/F5 to run SQL. F1 opens help, F10 quits.", "info")
        self.action_run_query()
        if self._query_task is not None:
            await self._query_task
        self._query_editor().focus()

    def _set_editor_text(self, text: str, *, log_message: str | None = None) -> None:
        editor = self._query_editor()
        editor.dismiss_completion_menu()
        editor.text = text
        self._reset_history_cursor()
        editor.focus()
        if log_message is not None:
            self._log(log_message, "info")

    def action_run_query(self) -> None:
        editor = self._query_editor()
        editor.dismiss_completion_menu()
        if self._query_task is not None and not self._query_task.done():
            return
        query = editor.text
        self._query_task = asyncio.create_task(self._run_query(query))

    async def _run_query(self, query: str) -> None:
        self._set_results_loading(True)
        try:
            response = await asyncio.to_thread(self.engine.run_input, query)
            self._reset_history_cursor()
            self._apply_response(response)
        except Exception as exc:  # noqa: BLE001
            self._log(f"Query failed: {exc}", "error")
        finally:
            self._set_results_loading(False)
            self._query_task = None

    def action_load_sample(self) -> None:
        self._set_editor_text(self.engine.default_query, log_message="Loaded sample query.")

    def action_clear_editor(self) -> None:
        self._set_editor_text("")

    def action_focus_editor(self) -> None:
        self._query_editor().focus()

    def action_focus_results(self) -> None:
        self._results_table().focus()

    def action_toggle_json_rendering(self) -> None:
        self._json_rendering_enabled = not self._json_rendering_enabled
        state = "on" if self._json_rendering_enabled else "off"
        self._log(f"JSON formatting/highlighting {state}.", "info")
        self._redraw_results_table()

    def action_copy_selected_cell(self) -> None:
        selected = self._selected_cell_context()
        if selected is None:
            self._log("No cell selected to copy.", "error")
            return

        row_index, column_index, value, column_name, type_name = selected
        formatted = self._format_full_cell_value(value, type_name)
        self.copy_to_clipboard(formatted)
        self._update_results_preview(row_index, column_index, column_name, type_name, value)
        self._log(f"Copied full cell value ({column_name}, row {row_index + 1}) to clipboard.", "ok")

    def action_view_selected_cell_full(self) -> None:
        selected = self._selected_cell_context()
        if selected is None:
            self._log("No cell selected to view.", "error")
            return
        row_index, column_index, value, column_name, type_name = selected
        self._update_results_preview(
            row_index,
            column_index,
            column_name,
            type_name,
            value,
            show_full=True,
        )

    def action_toggle_sidebar(self) -> None:
        sidebar = self.query_one("#sidebar", Vertical)
        sidebar.display = not sidebar.display
        state = "shown" if sidebar.display else "hidden"
        self._log(f"Data Explorer {state}.", "info")

    def action_show_help(self) -> None:
        self._log(self.engine.help_text(), "info")

    def action_open_preview_link(self, href: str) -> None:
        if href:
            self.open_url(href)

    @staticmethod
    def _rows_to_tsv(columns: list[str], rows: list[tuple[CellValue, ...]]) -> str:
        output = StringIO(newline="")
        writer = csv.writer(output, delimiter="\t", quoting=csv.QUOTE_MINIMAL, lineterminator="\r\n")
        writer.writerow(columns)
        writer.writerows(rows)
        return output.getvalue()

    def action_copy_results_tsv(self) -> None:
        result = self._active_result
        if result is None or not result.columns:
            self._log("No tabular result available to copy.", "error")
            return

        tsv_text = self._rows_to_tsv(result.columns, result.rows)
        self._last_results_tsv = tsv_text
        self.copy_to_clipboard(tsv_text)

        message = (
            f"Copied {len(result.rows):,} rows x {len(result.columns)} cols as TSV "
            "(Results pane table). Paste into Excel."
        )
        if result.truncated:
            out_of_rows = self._results_out_of_rows(result)
            message += f" Results pane shows {len(result.rows):,}/{out_of_rows:,} rows."
        self._log(message, "ok")

    def _apply_response(self, response: EngineResponse) -> None:
        sql_log = response.activity_sql_log()
        if sql_log is not None:
            title, sql_text = sql_log
            self._log(f"{title}: {sql_text}", "sql")

        if response.result is not None:
            self._render_table(response.result)

        if response.message:
            self._log(response.message, response.status)
        if response.activity_messages:
            for status, message in response.activity_messages:
                self._log(message, status)

        if response.load_query is not None:
            self._set_editor_text(response.load_query)

        if response.clear_editor:
            self.action_clear_editor()

        if response.should_exit:
            self.exit()

        sidebar = self.query_one("#sidebar_text", Static)
        sidebar.update(self.engine.schema_preview())

    def _render_table(self, result: QueryResult) -> None:
        self._active_result = result
        self._base_rows = list(result.rows)
        self._sort_column_index = None
        self._sort_reverse = False
        self._active_data_total_rows = self.engine.row_count() if result.sql else None
        self._clear_results_column_width_overrides()

        self._redraw_results_table()

    def _clear_results_column_width_overrides(self) -> None:
        self._results_column_width_overrides.clear()

    @staticmethod
    def _header_selected_table(event: DataTable.HeaderSelected) -> ResultsTable:
        return cast(ResultsTable, cast(Any, event).data_table)

    def _on_results_column_resized(self, column_index: int, width: int) -> None:
        self._results_column_width_overrides[column_index] = width
        self._refresh_results_column_cells(column_index)

    def _add_results_columns(self, table: ResultsTable, columns: list[str]) -> None:
        for column_index, column_name in enumerate(columns):
            width = self._results_column_width_overrides.get(column_index)
            table.add_column(column_name, width=width)

    def _results_column_render_char_limit(self, table: ResultsTable, column_index: int) -> int:
        column = table.ordered_columns[column_index]
        column_width = column.content_width if column.auto_width else column.width
        return min(INLINE_CELL_PREVIEW_MAX_CHARS, max(self.engine.max_value_chars, column_width))

    def _results_column_char_limits(self, table: ResultsTable, column_count: int) -> list[int]:
        return [self._results_column_render_char_limit(table, column_index) for column_index in range(column_count)]

    def _render_results_cell(
        self,
        column_index: int,
        value: CellValue,
        json_columns: set[int],
        max_value_chars: int,
    ) -> RenderedCell:
        if column_index in json_columns:
            return self._render_json_cell(value, max_value_chars=max_value_chars)
        return self._render_scalar_cell(value, max_value_chars=max_value_chars)

    def _visible_row_bounds(self, table: ResultsTable, total_rows: int) -> tuple[int, int]:
        if total_rows <= 0:
            return 0, 0
        viewport = max(1, table.size.height)
        start = max(0, int(table.scroll_y) - self.COLUMN_RESIZE_VISIBLE_ROW_BUFFER)
        end = min(total_rows, start + viewport + (self.COLUMN_RESIZE_VISIBLE_ROW_BUFFER * 2))
        return start, max(start, end)

    def _refresh_results_column_cells(self, column_index: int) -> None:
        result = self._active_result
        if result is None or not result.rows:
            return
        table = self._results_table()
        if not table.is_valid_column_index(column_index) or column_index >= len(result.columns):
            return

        json_columns = self._active_json_columns
        private_table = cast(Any, table)
        ordered_rows = table.ordered_rows
        if len(ordered_rows) != len(result.rows):
            return

        row_indexes: range
        if len(result.rows) > self.COLUMN_RESIZE_SYNC_REFRESH_MAX_ROWS:
            start, end = self._visible_row_bounds(table, len(result.rows))
            row_indexes = range(start, end)
        else:
            row_indexes = range(len(result.rows))

        # Update only one column after resize commit. For very large datasets,
        # refresh the visible window and lazily refresh newly focused rows.
        char_limits = self._results_column_char_limits(table, len(result.columns))
        for row_index in row_indexes:
            self._refresh_rendered_row_columns(
                table=table,
                private_table=private_table,
                result=result,
                row_index=row_index,
                column_indexes=(column_index,),
                json_columns=json_columns,
                char_limits=char_limits,
            )
        private_table._update_count += 1
        table.refresh_column(column_index)

    def _refresh_rendered_row_columns(
        self,
        *,
        table: ResultsTable,
        private_table: Any,
        result: QueryResult,
        row_index: int,
        column_indexes: Sequence[int],
        json_columns: set[int],
        char_limits: list[int],
    ) -> None:
        ordered_rows = table.ordered_rows
        if row_index < 0 or row_index >= len(ordered_rows) or row_index >= len(result.rows):
            return
        row = result.rows[row_index]
        row_key = ordered_rows[row_index].key
        for column_index in column_indexes:
            if column_index < 0 or column_index >= len(result.columns) or column_index >= len(row):
                continue
            column_key = table.ordered_columns[column_index].key
            rendered = self._render_results_cell(
                column_index,
                row[column_index],
                json_columns,
                char_limits[column_index] if column_index < len(char_limits) else self.engine.max_value_chars,
            )
            private_table._data[row_key][column_key] = rendered

    def _refresh_results_row_cells(self, row_index: int) -> None:
        result = self._active_result
        if result is None or row_index < 0 or row_index >= len(result.rows):
            return
        table = self._results_table()
        json_columns = self._active_json_columns
        private_table = cast(Any, table)
        char_limits = self._results_column_char_limits(table, len(result.columns))
        self._refresh_rendered_row_columns(
            table=table,
            private_table=private_table,
            result=result,
            row_index=row_index,
            column_indexes=range(min(len(result.columns), len(result.rows[row_index]))),
            json_columns=json_columns,
            char_limits=char_limits,
        )
        private_table._update_count += 1
        table.refresh_row(row_index)

    def _results_out_of_rows(self, result: QueryResult) -> int:
        if not result.sql:
            return result.total_rows
        data_total_rows = self._active_data_total_rows
        if data_total_rows is None:
            data_total_rows = self.engine.row_count()
            self._active_data_total_rows = data_total_rows
        return max(result.total_rows, data_total_rows)

    def _detect_json_columns(self, result: QueryResult) -> set[int]:
        if not self._json_rendering_enabled:
            return set()
        if result.total_rows > JSON_HIGHLIGHT_MAX_TOTAL_ROWS:
            return set()
        if not result.rows:
            return set()
        if len(result.column_types) != len(result.columns):
            return set()

        detected: set[int] = set()
        for index, type_name in enumerate(result.column_types):
            if is_struct_type_name(type_name):
                detected.add(index)
                continue
            if not is_varchar_type(type_name):
                continue
            if column_looks_like_json(result.rows, index):
                detected.add(index)
        return detected

    def _clamp_inline_cell_text(self, text: str, max_value_chars: int) -> str:
        clamped, _ = clamp_preview_text(
            text,
            max_chars=max_value_chars,
            max_lines=INLINE_CELL_PREVIEW_MAX_LINES,
            single_line=True,
        )
        return clamped

    @classmethod
    def _json_value_too_large_for_inline_render(cls, value: CellValue) -> bool:
        if isinstance(value, str):
            return len(value) > INLINE_CELL_PREVIEW_MAX_CHARS
        if isinstance(value, dict):
            return len(cast(JsonObject, value)) > cls.PREVIEW_JSON_PRETTY_MAX_CONTAINER_ITEMS
        if isinstance(value, list):
            return len(cast(JsonArray, value)) > cls.PREVIEW_JSON_PRETTY_MAX_CONTAINER_ITEMS
        return False

    def _render_json_cell(self, value: CellValue, *, max_value_chars: int | None = None) -> RenderedCell:
        value_chars = self.engine.max_value_chars if max_value_chars is None else max_value_chars
        if not self._json_rendering_enabled:
            return self._render_scalar_cell(value, max_value_chars=value_chars)
        if self._json_value_too_large_for_inline_render(value):
            return self._render_scalar_cell(value, max_value_chars=value_chars)
        image = summarize_image_cell(value)
        if image is not None:
            return Text(format_image_cell_token(image), end="", no_wrap=True)
        compact = compact_json_cell(value)
        if compact is None:
            return self._render_scalar_cell(value, max_value_chars=value_chars)
        highlighted = Text(compact, end="", no_wrap=True)
        self._json_highlighter.highlight(highlighted)
        truncated = truncate_highlighted_text(highlighted, value_chars)
        stylize_links(truncated, clickable=False)
        return truncated

    def _render_scalar_cell(self, value: CellValue, *, max_value_chars: int | None = None) -> RenderedCell:
        value_chars = self.engine.max_value_chars if max_value_chars is None else max_value_chars
        if value is None:
            return Text("NULL", style=NULL_VALUE_STYLE, end="", no_wrap=True)
        if self._json_rendering_enabled:
            image = summarize_image_cell(value)
            if image is not None:
                return Text(format_image_cell_token(image), end="", no_wrap=True)
        text = self._clamp_inline_cell_text(format_scalar(value, value_chars), value_chars)
        return self._render_linkified_scalar_text(text)

    @staticmethod
    def _render_linkified_scalar_text(text: str) -> RenderedCell:
        rendered = Text(text, end="", no_wrap=True)
        if not stylize_links(rendered, clickable=False):
            return text
        return rendered

    def _json_rendering_status_suffix(self) -> str:
        state = "on" if self._json_rendering_enabled else "off"
        return f" [json:{state}]"

    def _redraw_results_table(self) -> None:
        result = self._active_result
        table = self._results_table()
        table.clear(columns=True)
        if result is None or not result.columns:
            self._active_json_columns = set()
            self.query_one("#results_header", Static).update(f"Results{self._json_rendering_status_suffix()}")
            self._set_results_preview_text(self.PREVIEW_DEFAULT_TEXT)
            return

        self._add_results_columns(table, result.columns)
        json_columns = self._detect_json_columns(result)
        self._active_json_columns = json_columns
        char_limits = self._results_column_char_limits(table, len(result.columns))
        for row in result.rows:
            rendered_row: list[RenderedCell] = []
            for index, value in enumerate(row):
                rendered_row.append(
                    self._render_results_cell(
                        index,
                        value,
                        json_columns,
                        char_limits[index] if index < len(char_limits) else self.engine.max_value_chars,
                    )
                )
            table.add_row(*rendered_row)

        out_of_rows = self._results_out_of_rows(result)
        header = f"Results ({len(result.rows):,}/{out_of_rows:,} rows, {result.elapsed_ms:.1f} ms)"
        if result.truncated:
            header += " [truncated]"
        if self._sort_column_index is not None and self._sort_column_index < len(result.columns):
            direction = "desc" if self._sort_reverse else "asc"
            header += f" [sorted: {result.columns[self._sort_column_index]} {direction}]"
        header += self._json_rendering_status_suffix()
        self.query_one("#results_header", Static).update(header)
        self._refresh_results_preview_from_cursor()

    def _set_results_preview_text(self, text: str) -> None:
        preview = self._results_preview()
        self._results_preview_plain_text = text
        preview.update(text)
        preview.scroll_home(animate=False, immediate=True)

    def _selected_cell_context_at(
        self,
        row_index: int,
        column_index: int,
    ) -> tuple[int, int, CellValue, str, str] | None:
        result = self._active_result
        if result is None or not result.rows:
            return None
        if row_index < 0 or row_index >= len(result.rows):
            return None
        row = result.rows[row_index]
        if column_index < 0 or column_index >= len(row):
            return None
        column_name = result.columns[column_index] if column_index < len(result.columns) else f"col_{column_index + 1}"
        type_name = result.column_types[column_index] if column_index < len(result.column_types) else ""
        return row_index, column_index, row[column_index], column_name, type_name

    def _selected_cell_context(self) -> tuple[int, int, CellValue, str, str] | None:
        table = self._results_table()
        return self._selected_cell_context_at(table.cursor_row, table.cursor_column)

    def _format_full_cell_value(self, value: CellValue, type_name: str) -> str:
        formatted_value, _, _ = self._format_preview_value(value, type_name, show_full=True)
        return formatted_value

    @staticmethod
    def _clip_detail_preview_text(text: str, *, show_full: bool) -> tuple[str, bool]:
        if show_full:
            return text, False
        return clamp_preview_text(
            text,
            max_chars=DETAIL_PREVIEW_MAX_CHARS,
            max_lines=DETAIL_PREVIEW_MAX_LINES,
        )

    def _should_pretty_json_preview(self, value: CellValue, *, show_full: bool) -> bool:
        if show_full:
            return True
        if isinstance(value, str):
            return len(value) <= self.PREVIEW_JSON_PRETTY_MAX_STRING_CHARS
        if isinstance(value, dict):
            return len(cast(JsonObject, value)) <= self.PREVIEW_JSON_PRETTY_MAX_CONTAINER_ITEMS
        if isinstance(value, list):
            return len(cast(JsonArray, value)) <= self.PREVIEW_JSON_PRETTY_MAX_CONTAINER_ITEMS
        return True

    def _format_preview_value(self, value: CellValue, type_name: str, *, show_full: bool) -> tuple[str, bool, bool]:
        value_any = cast(Any, value)
        if value is None:
            return "NULL", False, False
        if not self._json_rendering_enabled:
            clamped, truncated = self._clip_detail_preview_text(str(value_any), show_full=show_full)
            return clamped, False, truncated
        image = summarize_image_cell(value_any)
        if image is not None:
            metadata = format_image_preview_metadata(image)
            image_text = f"{metadata}\nraw:\n{value_any}"
            clamped, truncated = self._clip_detail_preview_text(image_text, show_full=show_full)
            return clamped, False, truncated
        should_try_json = is_struct_type_name(type_name) or is_varchar_type(type_name) or isinstance(value, dict | list)
        if should_try_json and self._should_pretty_json_preview(cast(CellValue, value_any), show_full=show_full):
            pretty = pretty_json_cell(value_any)
            if pretty is not None:
                clamped, truncated = self._clip_detail_preview_text(pretty, show_full=show_full)
                return clamped, True, truncated
        clamped, truncated = self._clip_detail_preview_text(str(value_any), show_full=show_full)
        return clamped, False, truncated

    def _render_preview_value(self, value: CellValue, type_name: str, *, show_full: bool = False) -> tuple[Text, bool]:
        if value is None:
            return Text("NULL", style=NULL_VALUE_STYLE, end=""), False
        formatted_value, is_json, is_truncated = self._format_preview_value(value, type_name, show_full=show_full)
        rendered = Text(formatted_value, end="")
        if is_json:
            self._json_highlighter.highlight(rendered)
        else:
            self._repr_highlighter.highlight(rendered)
        return rendered, is_truncated

    def _safe_len(self, value: CellValue) -> int | None:
        if not value:
            return None
        if isinstance(value, dict):
            return len(cast(JsonObject, value))
        if isinstance(value, list):
            return len(cast(JsonArray, value))
        if isinstance(value, str):
            return len(value)
        return len(str(value))

    def _update_results_preview(
        self,
        row_index: int,
        column_index: int,
        column_name: str,
        type_name: str,
        value: CellValue,
        *,
        show_full: bool = False,
    ) -> None:
        type_label = preview_type_label(type_name)
        value_len = self._safe_len(value)
        rendered_value, is_truncated = self._render_preview_value(value, type_name, show_full=show_full)
        header = f"{column_name}, {type_label}, row {row_index + 1}, col {column_index + 1}, len {value_len}"
        if is_truncated and not show_full:
            header += " [preview clipped]"
        preview_text = Text(end="")
        preview_text.append(header, style=PREVIEW_HEADER_STYLE)
        preview_text.append("\n")
        if is_truncated and not show_full:
            preview_text.append("Preview clipped for performance. Press F4 for full value.\n", style="dim")
        preview_text.append_text(rendered_value)
        preview = self._results_preview()
        self._results_preview_plain_text = preview_text.plain
        preview.update(preview_text)
        preview.scroll_home(animate=False, immediate=True)

    def _refresh_results_preview_from_cursor(self) -> None:
        selected = self._selected_cell_context()
        if selected is None:
            self._set_results_preview_text(self.PREVIEW_DEFAULT_TEXT)
            return
        row_index, column_index, value, column_name, type_name = selected
        self._update_results_preview(row_index, column_index, column_name, type_name, value)

    def _refresh_results_preview_at(self, row_index: int, column_index: int) -> None:
        selected = self._selected_cell_context_at(row_index, column_index)
        if selected is None:
            return
        selected_row, selected_column, value, column_name, type_name = selected
        self._update_results_preview(selected_row, selected_column, column_name, type_name, value)

    def on_data_table_header_selected(self, event: DataTable.HeaderSelected) -> None:
        table = self._results_table()
        if self._header_selected_table(event) is not table:
            return
        if table.consume_pending_header_sort_suppression():
            return
        result = self._active_result
        if result is None or not result.rows:
            return

        if self._sort_column_index == event.column_index:
            self._sort_reverse = not self._sort_reverse
        else:
            self._sort_column_index = event.column_index
            self._sort_reverse = False

        column_index = event.column_index
        result.rows = sorted(
            self._base_rows,
            key=lambda row: sort_cell_key(row[column_index]) if column_index < len(row) else (2, 0, ""),
            reverse=self._sort_reverse,
        )
        self._redraw_results_table()
        direction = "DESC" if self._sort_reverse else "ASC"
        self._log(f"Sorted results by {event.label.plain} {direction}", "info")

    def on_data_table_cell_highlighted(self, event: DataTable.CellHighlighted) -> None:
        self._refresh_results_row_cells(event.coordinate.row)
        self._refresh_results_preview_at(event.coordinate.row, event.coordinate.column)

    def on_data_table_cell_selected(self, event: DataTable.CellSelected) -> None:
        self._refresh_results_preview_at(event.coordinate.row, event.coordinate.column)

    def on_key(self, event: Key) -> None:
        if event.key != "ctrl+c":
            return
        focused = self.focused
        if focused is None:
            return
        if focused is self._results_preview():
            selected = self._selected_text(focused)
            event.stop()
            event.prevent_default()
            self.copy_to_clipboard(selected if selected is not None else self._results_preview_plain_text)
            return
        if focused is self._activity_log():
            selected = self._selected_text(focused)
            if selected is None:
                return
            event.stop()
            event.prevent_default()
            self.copy_to_clipboard(selected)

    @staticmethod
    def _selected_text(widget: Widget) -> str | None:
        if isinstance(widget, TextArea):
            if widget.selected_text:
                return widget.selected_text
        selection = widget.text_selection
        if selection is None:
            return None
        extracted = widget.get_selection(selection)
        if extracted is None:
            return None
        text, _ = extracted
        if not text:
            return None
        return text

    def _log(self, message: str, status: ResultStatus) -> None:
        activity_log = self._activity_log()
        self._activity_lines.append(f"[{status.upper()}] {message}")
        activity_log.load_text("\n".join(self._activity_lines))
        activity_log.scroll_end(animate=False, immediate=True)
        payload = truncate_for_log(message, max_chars=MAX_ACTIVITY_LOG_CHARS)
        if status == "error":
            logger.error("activity status=%s message=%s", status, payload)
        elif status == "sql":
            logger.info("activity status=%s sql=%s", status, payload)
        else:
            logger.info("activity status=%s message=%s", status, payload)
