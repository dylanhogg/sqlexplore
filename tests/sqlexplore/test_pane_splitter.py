from dataclasses import dataclass
from typing import Any, cast

from textual.events import MouseDown, MouseMove, MouseUp

from sqlexplore.ui.pane_splitter import PaneSplitter


@dataclass(slots=True)
class _FakeMouseEvent:
    screen_y: float
    button: int = 1
    stopped: bool = False

    def stop(self) -> None:
        self.stopped = True


def test_pane_splitter_default_render_is_empty() -> None:
    splitter = PaneSplitter(0, lambda _idx, _phase, _delta: None)
    assert str(splitter.render()) == ""


def test_pane_splitter_drag_updates_handle_and_resets_state(monkeypatch: Any) -> None:
    calls: list[tuple[int, str, int]] = []
    splitter = PaneSplitter(2, lambda idx, phase, delta: calls.append((idx, phase, delta)))

    monkeypatch.setattr(splitter, "capture_mouse", lambda: None)
    monkeypatch.setattr(splitter, "release_mouse", lambda: None)

    splitter.on_mouse_down(cast(MouseDown, _FakeMouseEvent(screen_y=10)))
    assert splitter.has_class("-dragging")

    splitter.on_mouse_move(cast(MouseMove, _FakeMouseEvent(screen_y=13)))
    splitter.on_mouse_up(cast(MouseUp, _FakeMouseEvent(screen_y=13)))

    assert not splitter.has_class("-dragging")
    assert calls == [(2, "start", 0), (2, "update", 3), (2, "end", 3)]
