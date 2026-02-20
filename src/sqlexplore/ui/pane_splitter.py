from time import monotonic
from typing import Any, Callable

from textual.events import Blur, MouseDown, MouseMove, MouseUp
from textual.widgets import Static

from sqlexplore.ui.tui_shared import DragDeltaState, PaneResizePhase


class PaneSplitter(Static):
    RESIZE_APPLY_INTERVAL_SECS = 1 / 60
    DEFAULT_CSS = """
    PaneSplitter {
        height: 1;
        min-height: 1;
        background: $background;
        border-top: heavy #5f7b8c;
    }

    PaneSplitter:hover {
        background: $background;
        border-top: heavy #b7d2e2;
    }

    PaneSplitter.-dragging {
        background: $background;
        border-top: heavy #f0f9ff;
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
        self._drag_state: DragDeltaState | None = None
        self._last_resize_apply_ts = 0.0
        self.tooltip = "Drag to resize panes"

    @staticmethod
    def _event_screen_y(event: MouseDown | MouseMove | MouseUp) -> float:
        return float(event.screen_y)

    def _end_drag(self) -> DragDeltaState | None:
        state = self._drag_state
        self._drag_state = None
        self.release_mouse()
        self.remove_class("-dragging")
        return state

    def _commit_drag(self) -> None:
        state = self._end_drag()
        if state is None:
            return
        if state.last_applied_delta != state.last_delta:
            self._on_resize(self._splitter_index, "update", state.last_delta)
        if state.did_drag:
            self._on_resize(self._splitter_index, "end", state.last_delta)

    def on_mouse_down(self, event: MouseDown) -> None:
        if event.button != 1:
            return
        self._drag_state = DragDeltaState(start_screen=self._event_screen_y(event))
        self._last_resize_apply_ts = 0.0
        self.capture_mouse()
        self.add_class("-dragging")
        self._on_resize(self._splitter_index, "start", 0)
        event.stop()

    def on_mouse_move(self, event: MouseMove) -> None:
        state = self._drag_state
        if state is None:
            return
        delta = int(round(self._event_screen_y(event) - state.start_screen))
        if delta == state.last_delta:
            return
        state.last_delta = delta
        if delta != 0:
            state.did_drag = True
        now = monotonic()
        if now - self._last_resize_apply_ts < self.RESIZE_APPLY_INTERVAL_SECS:
            event.stop()
            return
        self._on_resize(self._splitter_index, "update", delta)
        state.last_applied_delta = delta
        self._last_resize_apply_ts = now
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
