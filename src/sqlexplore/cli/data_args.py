from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

import typer

from sqlexplore.cli import data_paths, stdin_io


@dataclass(slots=True, frozen=True)
class ResolvedMainDataSources:
    paths: tuple[Path, ...]
    use_stdin: bool
    stdin_capture: stdin_io.StdinCapture | None


def _normalize_data_values(data_values: Sequence[str] | None) -> tuple[str, ...]:
    if data_values is None:
        return ()
    normalized = tuple(raw_value.strip() for raw_value in data_values)
    if not normalized:
        return ()
    if any(not raw_value for raw_value in normalized):
        raise typer.BadParameter("Data path cannot be empty.")
    return normalized


def resolve_main_data_sources(
    data_values: Sequence[str] | None,
    *,
    download_dir: Path,
    overwrite: bool,
    startup_activity_messages: list[str],
) -> ResolvedMainDataSources:
    normalized = _normalize_data_values(data_values)
    has_stdin_marker = any(raw_value == "-" for raw_value in normalized)

    if has_stdin_marker:
        if len(normalized) != 1:
            raise typer.BadParameter("When using stdin, provide only --data -.")
        capture = stdin_io.read_stdin_to_temp_file()
        startup_activity_messages.append(capture.startup_message)
        return ResolvedMainDataSources(paths=(capture.path,), use_stdin=True, stdin_capture=capture)

    if normalized:
        paths: tuple[Path, ...] = tuple(
            data_paths.resolve_data_path(
                raw_value,
                download_dir,
                overwrite,
                startup_activity_messages,
            )
            for raw_value in normalized
        )
        return ResolvedMainDataSources(paths=paths, use_stdin=False, stdin_capture=None)

    if stdin_io.should_use_stdin(None):
        capture = stdin_io.read_stdin_to_temp_file()
        startup_activity_messages.append(capture.startup_message)
        return ResolvedMainDataSources(paths=(capture.path,), use_stdin=True, stdin_capture=capture)

    raise typer.BadParameter(stdin_io.STDIN_MISSING_SOURCE_ERROR)
