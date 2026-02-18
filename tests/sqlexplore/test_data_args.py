from pathlib import Path
from typing import Any, cast

import pytest
import typer

from sqlexplore.cli import data_args, stdin_io


class _FakeCapture:
    def __init__(self, path: Path) -> None:
        self.path = path

    @property
    def startup_message(self) -> str:
        return f"{stdin_io.STDIN_LOCAL_PREFIX}{self.path}"


def test_resolve_main_data_sources_resolves_multiple_values(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, Path, bool]] = []
    activity_messages: list[str] = []

    def fake_resolve(
        raw_value: str,
        download_dir: Path,
        overwrite: bool,
        startup_activity_messages: list[str] | None = None,
    ) -> Path:
        calls.append((raw_value, download_dir, overwrite))
        assert startup_activity_messages is activity_messages
        return (tmp_path / raw_value).resolve()

    monkeypatch.setattr(data_args.data_paths, "resolve_data_path", fake_resolve)

    out = data_args.resolve_main_data_sources(
        ["a.csv", "b.csv"],
        download_dir=tmp_path,
        overwrite=True,
        startup_activity_messages=activity_messages,
    )

    assert out.use_stdin is False
    assert out.stdin_capture is None
    assert out.paths == ((tmp_path / "a.csv").resolve(), (tmp_path / "b.csv").resolve())
    assert calls == [("a.csv", tmp_path, True), ("b.csv", tmp_path, True)]


def test_resolve_main_data_sources_requires_exclusive_stdin_marker(tmp_path: Path) -> None:
    with pytest.raises(typer.BadParameter, match=r"only --data -"):
        data_args.resolve_main_data_sources(
            ["-", "a.csv"],
            download_dir=tmp_path,
            overwrite=False,
            startup_activity_messages=[],
        )


def test_resolve_main_data_sources_reads_stdin_when_no_data(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    capture = _FakeCapture((tmp_path / "stdin.txt").resolve())

    def use_stdin(_data: str | None = None) -> bool:
        _ = _data
        return True

    def read_stdin_to_temp_file() -> stdin_io.StdinCapture:
        return cast(Any, capture)

    monkeypatch.setattr(data_args.stdin_io, "should_use_stdin", use_stdin)
    monkeypatch.setattr(data_args.stdin_io, "read_stdin_to_temp_file", read_stdin_to_temp_file)

    messages: list[str] = []
    out = data_args.resolve_main_data_sources(
        None,
        download_dir=tmp_path,
        overwrite=False,
        startup_activity_messages=messages,
    )

    assert out.use_stdin is True
    assert out.paths == (capture.path,)
    assert out.stdin_capture is capture
    assert messages == [capture.startup_message]


def test_resolve_main_data_sources_errors_without_data_or_stdin(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def no_stdin(_data: str | None = None) -> bool:
        _ = _data
        return False

    monkeypatch.setattr(data_args.stdin_io, "should_use_stdin", no_stdin)

    with pytest.raises(typer.BadParameter, match=stdin_io.STDIN_MISSING_SOURCE_ERROR):
        data_args.resolve_main_data_sources(
            None,
            download_dir=tmp_path,
            overwrite=False,
            startup_activity_messages=[],
        )
