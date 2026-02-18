import os
import re
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TextIO

import typer

_ANSI_ESCAPE_RE = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~]|\][^\x1B\x07]*(?:\x07|\x1B\\))")
_STDIN_TEMP_FILENAME = "stdin.txt"
_STDIN_CHUNK_CHARS = 1024 * 1024
STDIN_EMPTY_ERROR = "No stdin input received."
STDIN_MISSING_SOURCE_ERROR = "Provide one or more --data values, or pipe text via stdin."
STDIN_TTY_FALLBACK_MESSAGE = "[stdin] No controlling TTY available; falling back to --no-ui."
STDIN_LOCAL_PREFIX = "[stdin] local="


@dataclass(slots=True, frozen=True)
class StdinCapture:
    path: Path
    _temp_dir: TemporaryDirectory[str]

    def cleanup(self) -> None:
        self._temp_dir.cleanup()

    @property
    def startup_message(self) -> str:
        return f"{STDIN_LOCAL_PREFIX}{self.path}"


def should_use_stdin(data: str | None, stdin_stream: TextIO | None = None) -> bool:
    stream = stdin_stream or sys.stdin
    if data is None:
        return not stream.isatty()
    return data.strip() == "-"


def _strip_ansi(text: str) -> str:
    return _ANSI_ESCAPE_RE.sub("", text)


def read_stdin_to_temp_file(stdin_stream: TextIO | None = None) -> StdinCapture:
    stream = stdin_stream or sys.stdin
    temp_dir = TemporaryDirectory(prefix="sqlexplore-stdin-")
    temp_path = Path(temp_dir.name) / _STDIN_TEMP_FILENAME
    chars_written = 0
    try:
        with temp_path.open("w", encoding="utf-8") as file_handle:
            while True:
                chunk = stream.read(_STDIN_CHUNK_CHARS)
                if chunk == "":
                    break
                cleaned = _strip_ansi(chunk)
                if not cleaned:
                    continue
                file_handle.write(cleaned)
                chars_written += len(cleaned)
    except Exception:
        temp_dir.cleanup()
        raise
    if chars_written == 0:
        temp_dir.cleanup()
        raise typer.BadParameter(STDIN_EMPTY_ERROR)
    return StdinCapture(path=temp_path.resolve(), _temp_dir=temp_dir)


@contextmanager
def stdin_tty_for_tui(enabled: bool, stdin_stream: TextIO | None = None) -> Iterator[bool]:
    if not enabled:
        yield True
        return

    stream = stdin_stream or sys.__stdin__
    if stream is not None and stream.isatty():
        yield True
        return

    saved_fd = os.dup(0)
    tty_fd: int | None = None
    try:
        tty_fd = os.open("/dev/tty", os.O_RDONLY)
        os.dup2(tty_fd, 0)
        yield True
    except OSError:
        yield False
    finally:
        os.dup2(saved_fd, 0)
        os.close(saved_fd)
        if tty_fd is not None:
            os.close(tty_fd)
