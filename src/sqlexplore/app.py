import re
import sys
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from urllib.request import Request, urlopen

import typer
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from tqdm import tqdm

from sqlexplore.cli import stdin_io
from sqlexplore.core.engine import (
    STATUS_STYLE_BY_RESULT,
    EngineResponse,
    QueryResult,
    SqlExplorerEngine,
    app_version,
    format_scalar,
)
from sqlexplore.core.logging_utils import configure_file_logging, get_logger, truncate_for_log
from sqlexplore.ui.tui import SqlExplorerTui, SqlQueryEditor

app = typer.Typer(
    help="Interactive DuckDB SQL explorer for CSV/TSV/TXT/Parquet files.",
    pretty_exceptions_enable=False,
    pretty_exceptions_show_locals=True,
)

_REMOTE_FILENAME_SAFE_CHARS_RE = re.compile(r"[^A-Za-z0-9._-]+")
_REMOTE_DOWNLOAD_CHUNK_SIZE = 5 * 1024 * 1024
MAX_SQL_LOG_CHARS = 8_000
logger = get_logger(__name__)


__all__ = [
    "EngineResponse",
    "QueryResult",
    "SqlExplorerEngine",
    "SqlExplorerTui",
    "SqlQueryEditor",
    "app",
    "app_version",
    "main",
]


def _is_http_url(value: str) -> bool:
    parsed = urlparse(value.strip())
    return parsed.scheme.lower() in {"http", "https"} and bool(parsed.netloc)


def _default_download_dir() -> Path:
    return Path(typer.get_app_dir("sqlexplore")) / "downloads"


def _format_byte_count(size_bytes: int) -> str:
    if size_bytes < 1024:
        return f"{size_bytes} B"
    if size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    if size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"


def _remote_filename(url: str) -> str:
    parsed = urlparse(url)
    file_name = Path(parsed.path).name
    if not file_name:
        host_name = _REMOTE_FILENAME_SAFE_CHARS_RE.sub("-", parsed.netloc).strip("-")
        file_name = f"{host_name or 'download'}.parquet"
    if Path(file_name).suffix.lower() not in {".csv", ".tsv", ".txt", ".parquet", ".pq"}:
        raise typer.BadParameter("Remote URL must end with .csv, .tsv, .txt, .parquet, or .pq.")
    return file_name


def _emit_download_log(
    message: str,
    activity_messages: list[str] | None = None,
    *,
    err: bool = False,
    echo: bool = True,
    include_activity: bool = True,
) -> None:
    if echo:
        typer.echo(message, err=err)
    if include_activity and activity_messages is not None:
        activity_messages.append(message)
    if err:
        logger.error(message)
    else:
        logger.info(message)


def _remote_content_length(response: Any) -> int | None:
    headers = getattr(response, "headers", None)
    if headers is None:
        return None
    raw_value = headers.get("Content-Length")
    if raw_value is None:
        return None
    try:
        content_length = int(raw_value)
    except (TypeError, ValueError):
        return None
    return content_length if content_length > 0 else None


def _ensure_download_dir(download_dir: Path) -> Path:
    expanded = download_dir.expanduser()
    if expanded.exists() and not expanded.is_dir():
        raise typer.BadParameter(f"Download path is not a directory: {expanded}")
    try:
        expanded.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise typer.BadParameter(f"Download directory is not writable: {expanded}: {exc}") from exc
    return expanded.resolve()


def _download_remote_data_file(
    url: str,
    download_dir: Path,
    overwrite: bool = False,
    activity_messages: list[str] | None = None,
) -> Path:
    destination_dir = _ensure_download_dir(download_dir)
    file_name = _remote_filename(url)
    destination = (destination_dir / file_name).resolve()

    _emit_download_log(f"[download] remote={url}", activity_messages)
    _emit_download_log(f"[download] local={destination}", activity_messages)

    if destination.exists() and not overwrite:
        _emit_download_log(
            (
                f"[download] Cached local download file {destination.name} already exists, skipping download. "
                "Use --overwrite to replace it."
            ),
            activity_messages,
        )
        return destination
    elif destination.exists() and overwrite:
        _emit_download_log(f"[download] Overwriting local download file {destination.name}", activity_messages)

    start = time.perf_counter()
    progress_bar: Any | None = None
    try:
        request = Request(url, headers={"User-Agent": "sqlexplore"})
        with urlopen(request) as response, destination.open("wb") as file_handle:
            total_bytes = _remote_content_length(response)
            progress_bar = tqdm(
                total=total_bytes,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                desc="download",
                leave=False,
                disable=not sys.stderr.isatty(),
            )
            while True:
                chunk = response.read(_REMOTE_DOWNLOAD_CHUNK_SIZE)
                if not chunk:
                    break
                file_handle.write(chunk)
                progress_bar.update(len(chunk))
    except Exception as exc:
        destination.unlink(missing_ok=True)
        logger.exception(
            "download failed url=%s destination=%s overwrite=%s",
            url,
            destination,
            overwrite,
        )
        raise typer.BadParameter(f"Failed to download data file from {url} to {destination}: {exc}") from exc
    finally:
        if progress_bar is not None:
            progress_bar.close()

    elapsed_seconds = time.perf_counter() - start
    file_size_bytes = destination.stat().st_size
    _emit_download_log(
        (
            "[download] Complete "
            f"elapsed={elapsed_seconds:.3f}s "
            f"size={_format_byte_count(file_size_bytes)} ({file_size_bytes:,} bytes) "
        ),
        activity_messages,
    )
    return destination


def _resolve_data_path(
    data: str,
    download_dir: Path,
    overwrite: bool = False,
    startup_activity_messages: list[str] | None = None,
) -> Path:
    value = data.strip()
    if not value:
        raise typer.BadParameter("Data path cannot be empty.")
    if _is_http_url(value):
        resolved_download_dir = download_dir.expanduser().resolve()
        logger.info("resolving remote data source url=%s download_dir=%s", value, resolved_download_dir)
        return _download_remote_data_file(
            value,
            resolved_download_dir,
            overwrite=overwrite,
            activity_messages=startup_activity_messages,
        )

    file_path = Path(value).expanduser().resolve()
    logger.info("resolving local data source path=%s", file_path)
    if not file_path.exists():
        raise typer.BadParameter(f"Data file not found: {file_path}")
    if not file_path.is_file():
        raise typer.BadParameter(f"Data path is not a file: {file_path}")
    try:
        with file_path.open("rb"):
            pass
    except OSError as exc:
        raise typer.BadParameter(f"Data file is not readable: {file_path}: {exc}") from exc
    return file_path


def _file_debug_metadata(file_path: Path) -> dict[str, Any]:
    stat = file_path.stat()
    return {
        "path": str(file_path),
        "suffix": file_path.suffix.lower(),
        "size_bytes": stat.st_size,
        "mtime_epoch": stat.st_mtime,
        "is_symlink": file_path.is_symlink(),
    }


def _render_console_response(console: Console, response: EngineResponse, max_value_chars: int) -> int:
    if response.generated_sql:
        console.print(Panel(response.generated_sql, title="Generated SQL", border_style="magenta"))

    if response.result is not None:
        result = response.result
        table = Table(
            title=f"Result ({len(result.rows)}/{result.total_rows} rows, {result.elapsed_ms:.1f} ms)",
            box=box.SIMPLE,
        )
        for column in result.columns:
            table.add_column(column)
        for row in result.rows:
            table.add_row(*[format_scalar(value, max_value_chars) for value in row])
        console.print(table)

    if response.message:
        border_style = STATUS_STYLE_BY_RESULT[response.status]
        console.print(Panel(response.message, border_style=border_style))

    return 1 if response.status == "error" else 0


def _run_console_query_and_exit(engine: SqlExplorerEngine, query: str | None) -> None:
    logger.info("console mode query_provided=%s", query is not None)
    if query is None:
        response = engine.run_sql(engine.default_query, remember=False)
    else:
        response = engine.run_input(query)
    logger.info(
        "console query complete status=%s has_result=%s message=%s",
        response.status,
        response.result is not None,
        truncate_for_log(response.message, max_chars=2_000),
    )
    exit_code = _render_console_response(Console(), response, engine.max_value_chars)
    raise typer.Exit(code=exit_code)


def _resolve_main_data_sources(
    data: str | None,
    download_dir: Path,
    overwrite: bool,
    startup_activity_messages: list[str],
) -> tuple[tuple[Path, ...], bool, stdin_io.StdinCapture | None]:
    use_stdin = stdin_io.should_use_stdin(data)
    if data is None and not use_stdin:
        raise typer.BadParameter(stdin_io.STDIN_MISSING_SOURCE_ERROR)
    if use_stdin:
        capture = stdin_io.read_stdin_to_temp_file()
        startup_activity_messages.append(capture.startup_message)
        return (capture.path,), True, capture
    assert data is not None
    file_path = _resolve_data_path(
        data,
        download_dir=download_dir,
        overwrite=overwrite,
        startup_activity_messages=startup_activity_messages,
    )
    return (file_path,), False, None


def _resolve_main_data_source(
    data: str | None,
    download_dir: Path,
    overwrite: bool,
    startup_activity_messages: list[str],
) -> tuple[Path, bool, stdin_io.StdinCapture | None]:
    data_paths, use_stdin, stdin_capture = _resolve_main_data_sources(
        data,
        download_dir=download_dir,
        overwrite=overwrite,
        startup_activity_messages=startup_activity_messages,
    )
    assert data_paths
    return data_paths[0], use_stdin, stdin_capture


def _version_callback(version: bool) -> None:
    if not version:
        return
    typer.echo(f"sqlexplore {app_version()}")
    raise typer.Exit()


@app.command()
def main(
    data: str | None = typer.Argument(
        None,
        help="CSV/TSV/TXT/Parquet local path, HTTP(S) URL, or '-' for stdin text.",
    ),
    table_name: str = typer.Option("data", "--table", "-t", help="Logical table/view name inside DuckDB."),
    limit: int = typer.Option(100, "--limit", "-l", min=1, help="Default helper query row limit."),
    max_rows: int = typer.Option(1000, "--max-rows", min=1, help="Maximum rows displayed in the result grid."),
    max_value_chars: int = typer.Option(
        160,
        "--max-value-chars",
        min=8,
        help="Maximum characters displayed per cell.",
    ),
    database: str = typer.Option(
        ":memory:",
        "--db",
        help="DuckDB database path. Use :memory: for in-memory session.",
    ),
    execute: str | None = typer.Option(
        None,
        "--execute",
        "-e",
        help="Startup SQL query. Runs once and exits only when --no-ui is enabled.",
    ),
    query_file: Path | None = typer.Option(
        None,
        "--file",
        "-f",
        exists=True,
        readable=True,
        resolve_path=True,
        help="Load startup SQL from file. Runs once and exits only when --no-ui is enabled.",
    ),
    no_ui: bool = typer.Option(False, "--no-ui", help="Run once in standard terminal output mode."),
    overwrite: bool = typer.Option(
        False,
        "--overwrite",
        help="When data is an HTTP(S) URL, overwrite existing local download if present.",
    ),
    download_dir: Path = typer.Option(
        _default_download_dir(),
        "--download-dir",
        help="When data is an HTTP(S) URL, directory used for downloaded files.",
    ),
    version: bool = typer.Option(
        False,
        "--version",
        callback=_version_callback,
        is_eager=True,
        help="Show sqlexplore version and exit.",
    ),
) -> None:
    log_path = configure_file_logging()
    logger.info("startup version=%s log_path=%s", app_version(), log_path)
    logger.debug(
        "startup args data=%s table_name=%s limit=%s max_rows=%s max_value_chars=%s database=%s no_ui=%s overwrite=%s "
        "download_dir=%s execute_chars=%s query_file=%s",
        data,
        table_name,
        limit,
        max_rows,
        max_value_chars,
        database,
        no_ui,
        overwrite,
        download_dir,
        len(execute) if execute is not None else 0,
        query_file,
    )

    if execute and query_file:
        raise typer.BadParameter("Use either --execute or --file, not both.")

    startup_activity_messages: list[str] = []
    file_path, use_stdin, stdin_capture = _resolve_main_data_source(
        data,
        download_dir=download_dir,
        overwrite=overwrite,
        startup_activity_messages=startup_activity_messages,
    )
    logger.info("data source resolved path=%s use_stdin=%s", file_path, use_stdin)
    try:
        logger.debug("data file metadata=%s", _file_debug_metadata(file_path))
    except OSError:
        logger.exception("failed to stat data file path=%s", file_path)

    engine = SqlExplorerEngine(
        data_path=file_path,
        table_name=table_name,
        database=database,
        default_limit=limit,
        max_rows_display=max_rows,
        max_value_chars=max_value_chars,
    )
    logger.info("engine initialized table=%s database=%s", table_name, database)

    try:
        non_interactive_sql = execute
        if query_file is not None:
            non_interactive_sql = query_file.read_text(encoding="utf-8").strip()
            logger.info(
                "loaded startup query file=%s chars=%s query=%s",
                query_file,
                len(non_interactive_sql),
                truncate_for_log(non_interactive_sql, max_chars=MAX_SQL_LOG_CHARS),
            )
        elif non_interactive_sql is not None:
            logger.info(
                "startup --execute chars=%s query=%s",
                len(non_interactive_sql),
                truncate_for_log(non_interactive_sql, max_chars=MAX_SQL_LOG_CHARS),
            )

        if no_ui:
            _run_console_query_and_exit(engine, non_interactive_sql)

        with stdin_io.stdin_tty_for_tui(use_stdin) as can_run_tui:
            if not can_run_tui:
                typer.echo(stdin_io.STDIN_TTY_FALLBACK_MESSAGE)
                logger.info("stdin tty unavailable, falling back to console mode")
                _run_console_query_and_exit(engine, non_interactive_sql)
            if non_interactive_sql is None:
                logger.info("launching tui with default startup query")
                SqlExplorerTui(
                    engine,
                    startup_activity_messages=startup_activity_messages,
                    log_file_path=str(log_path) if log_path is not None else None,
                ).run()
            else:
                logger.info("launching tui with startup query chars=%s", len(non_interactive_sql))
                SqlExplorerTui(
                    engine,
                    startup_activity_messages=startup_activity_messages,
                    startup_query=non_interactive_sql,
                    log_file_path=str(log_path) if log_path is not None else None,
                ).run()
    finally:
        logger.info("shutdown begin")
        engine.close()
        logger.info("engine closed")
        if stdin_capture is not None:
            stdin_capture.cleanup()
            logger.info("stdin temp file cleaned path=%s", stdin_capture.path)
        logger.info("shutdown complete")


if __name__ == "__main__":
    app()
