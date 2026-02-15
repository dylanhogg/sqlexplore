from io import StringIO

import pytest
import typer

from sqlexplore.cli import stdin_io


class _FakeStream(StringIO):
    def __init__(self, value: str, *, is_tty: bool) -> None:
        super().__init__(value)
        self._is_tty = is_tty

    def isatty(self) -> bool:
        return self._is_tty


def test_should_use_stdin_dash() -> None:
    assert stdin_io.should_use_stdin("-", stdin_stream=_FakeStream("", is_tty=True)) is True


def test_should_use_stdin_when_data_missing_depends_on_tty() -> None:
    assert stdin_io.should_use_stdin(None, stdin_stream=_FakeStream("", is_tty=False)) is True
    assert stdin_io.should_use_stdin(None, stdin_stream=_FakeStream("", is_tty=True)) is False


def test_read_stdin_to_temp_file_strips_ansi_and_cleans_up() -> None:
    capture = stdin_io.read_stdin_to_temp_file(stdin_stream=_FakeStream("\x1b[31malpha\x1b[0m\n", is_tty=False))
    try:
        assert capture.path.read_text(encoding="utf-8") == "alpha\n"
    finally:
        capture.cleanup()
    assert capture.path.exists() is False


def test_read_stdin_to_temp_file_streams_chunks(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(stdin_io, "_STDIN_CHUNK_CHARS", 4)
    payload = "abcdefghijklmnopqrstuvwxyz\n"
    capture = stdin_io.read_stdin_to_temp_file(stdin_stream=_FakeStream(payload, is_tty=False))
    try:
        assert capture.path.read_text(encoding="utf-8") == payload
    finally:
        capture.cleanup()


def test_read_stdin_to_temp_file_empty_raises() -> None:
    with pytest.raises(typer.BadParameter, match=stdin_io.STDIN_EMPTY_ERROR):
        stdin_io.read_stdin_to_temp_file(stdin_stream=_FakeStream("", is_tty=False))


def test_stdin_tty_for_tui_enabled_with_tty_stream() -> None:
    with stdin_io.stdin_tty_for_tui(True, stdin_stream=_FakeStream("", is_tty=True)) as can_run_tui:
        assert can_run_tui is True


def test_stdin_tty_for_tui_no_tty_returns_false(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_dup(fd: int) -> int:
        return 123

    def fake_dup2(src: int, dst: int) -> None:
        return None

    def fake_close(fd: int) -> None:
        return None

    monkeypatch.setattr(stdin_io.os, "dup", fake_dup)
    monkeypatch.setattr(stdin_io.os, "dup2", fake_dup2)
    monkeypatch.setattr(stdin_io.os, "close", fake_close)

    def _raise_open(path: str, flags: int) -> int:
        raise OSError("no tty")

    monkeypatch.setattr(stdin_io.os, "open", _raise_open)

    with stdin_io.stdin_tty_for_tui(True, stdin_stream=_FakeStream("", is_tty=False)) as can_run_tui:
        assert can_run_tui is False
