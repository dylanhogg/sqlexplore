from dataclasses import dataclass
from typing import Any, Callable

from rich.style import Style
from textual.coordinate import Coordinate
from textual.events import MouseDown, MouseMove, MouseUp
from textual.widgets import DataTable

from sqlexplore.ui.tui_shared import RenderedCell


@dataclass(slots=True)
class _ColumnResizeState:
    column_index: int
    start_screen_x: float
    start_content_width: int
    did_drag: bool = False


class ResultsTable(DataTable[RenderedCell]):
    COMPONENT_CLASSES = DataTable.COMPONENT_CLASSES | {"results-table--cursor-row"}
    RESIZE_HANDLE_WIDTH = 1
    MIN_RESIZED_CONTENT_WIDTH = 3
    DEFAULT_CSS = """
    ResultsTable > .results-table--cursor-row {
        background: #223744;
    }
    """

    def __init__(
        self,
        *args: Any,
        on_column_resized: Callable[[int, int], None] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._on_column_resized = on_column_resized
        self._resize_state: _ColumnResizeState | None = None
        self._suppress_next_header_select = False

    def _get_row_style(self, row_index: int, base_style: Style) -> Style:
        row_style = super()._get_row_style(row_index, base_style)
        if not self.show_cursor or self.cursor_type != "cell":
            return row_style
        if row_index < 0 or row_index != self.cursor_row:
            return row_style
        return row_style + self.get_component_styles("results-table--cursor-row").rich_style

    def watch_cursor_coordinate(
        self,
        old_coordinate: Coordinate,
        new_coordinate: Coordinate,
    ) -> None:
        super().watch_cursor_coordinate(old_coordinate, new_coordinate)
        if self.cursor_type != "cell":
            return
        for row_index in {old_coordinate.row, new_coordinate.row}:
            self.refresh_row(row_index)

    @staticmethod
    def _event_screen_x(event: MouseDown | MouseMove | MouseUp) -> float:
        return float(event.screen_x)

    def _event_virtual_x(self, event: MouseDown | MouseMove | MouseUp) -> int:
        return int(round(event.x + self.scroll_x))

    def _resize_candidate_column(self, event: MouseDown) -> int | None:
        meta = event.style.meta
        if not meta or meta.get("row") != -1 or meta.get("out_of_bounds", False):
            return None
        column_index = meta.get("column")
        if not isinstance(column_index, int) or not self.is_valid_column_index(column_index):
            return None
        pointer_x = self._event_virtual_x(event)
        region = self._get_column_region(column_index)
        right_edge = region.x + region.width
        if abs(pointer_x - right_edge) <= self.RESIZE_HANDLE_WIDTH:
            return column_index
        left_edge = region.x
        if column_index > 0 and abs(pointer_x - left_edge) <= self.RESIZE_HANDLE_WIDTH:
            return column_index - 1
        return None

    def _column_content_width(self, column_index: int) -> int:
        column = self.ordered_columns[column_index]
        return column.content_width if column.auto_width else column.width

    def _set_column_content_width(self, column_index: int, content_width: int) -> None:
        if not self.is_valid_column_index(column_index):
            return
        width = max(self.MIN_RESIZED_CONTENT_WIDTH, content_width)
        column = self.ordered_columns[column_index]
        if not column.auto_width and column.width == width:
            return
        column.auto_width = False
        column.width = width
        self._update_count += 1
        self._require_update_dimensions = True
        self.check_idle()
        self.refresh(layout=True)

    def _start_resize(self, column_index: int, event: MouseDown) -> None:
        self._resize_state = _ColumnResizeState(
            column_index=column_index,
            start_screen_x=self._event_screen_x(event),
            start_content_width=self._column_content_width(column_index),
        )
        self.capture_mouse()

    def _end_resize(self) -> _ColumnResizeState | None:
        state = self._resize_state
        self._resize_state = None
        self.release_mouse()
        return state

    def consume_pending_header_sort_suppression(self) -> bool:
        if not self._suppress_next_header_select:
            return False
        self._suppress_next_header_select = False
        return True

    def on_mouse_down(self, event: MouseDown) -> None:
        if event.button != 1:
            return
        resize_column_index = self._resize_candidate_column(event)
        if resize_column_index is None:
            return
        self._start_resize(resize_column_index, event)
        event.stop()

    def on_mouse_move(self, event: MouseMove) -> None:
        state = self._resize_state
        if state is None:
            return
        delta = int(round(self._event_screen_x(event) - state.start_screen_x))
        if delta == 0:
            return
        state.did_drag = True
        self._set_column_content_width(state.column_index, state.start_content_width + delta)
        event.stop()

    def on_mouse_up(self, event: MouseUp) -> None:
        if self._resize_state is None:
            return
        state = self._end_resize()
        if state is None:
            return
        if state.did_drag:
            self._suppress_next_header_select = True
            if self._on_column_resized is not None:
                self._on_column_resized(state.column_index, self._column_content_width(state.column_index))
        event.stop()
