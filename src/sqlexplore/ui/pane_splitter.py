from dataclasses import dataclass
from typing import Any, Callable

from textual.events import Blur, MouseDown, MouseMove, MouseUp
from textual.widgets import Static

from sqlexplore.ui.tui_shared import PaneResizePhase


@dataclass(slots=True)
class _PaneSplitterDragState:
    start_screen_y: float
    last_delta: int = 0
    did_drag: bool = False


class PaneSplitter(Static):
    DEFAULT_CSS = """
    PaneSplitter {
        height: 1;
    }
    """

    def __init__(
        self,
        splitter_index: int,
        on_resize: Callable[[int, PaneResizePhase, int], None],
        **kwargs: Any,
    ) -> None:
        super().__init__("", **kwargs)
        self._splitter_index = splitter_index
        self._on_resize = on_resize
        self._drag_state: _PaneSplitterDragState | None = None

    @staticmethod
    def _event_screen_y(event: MouseDown | MouseMove | MouseUp) -> float:
        return float(event.screen_y)

    def _end_drag(self) -> _PaneSplitterDragState | None:
        state = self._drag_state
        self._drag_state = None
        self.release_mouse()
        return state

    def _commit_drag(self) -> None:
        state = self._end_drag()
        if state is None:
            return
        if state.did_drag:
            self._on_resize(self._splitter_index, "end", state.last_delta)

    def on_mouse_down(self, event: MouseDown) -> None:
        if event.button != 1:
            return
        self._drag_state = _PaneSplitterDragState(start_screen_y=self._event_screen_y(event))
        self.capture_mouse()
        self._on_resize(self._splitter_index, "start", 0)
        event.stop()

    def on_mouse_move(self, event: MouseMove) -> None:
        state = self._drag_state
        if state is None:
            return
        delta = int(round(self._event_screen_y(event) - state.start_screen_y))
        if delta == state.last_delta:
            return
        state.last_delta = delta
        if delta != 0:
            state.did_drag = True
        self._on_resize(self._splitter_index, "update", delta)
        event.stop()

    def on_mouse_up(self, event: MouseUp) -> None:
        if self._drag_state is None:
            return
        self._commit_drag()
        event.stop()

    def on_blur(self, _event: Blur) -> None:
        if self._drag_state is None:
            return
        self._commit_drag()
