from pathlib import Path

import typer
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from sqlexplore.cli import data_args, data_paths, stdin_io
from sqlexplore.core.engine import (
    STATUS_STYLE_BY_RESULT,
    EngineResponse,
    QueryResult,
    SqlExplorerEngine,
    app_version,
    format_scalar,
)
from sqlexplore.core.logging_utils import (
    configure_file_logging,
    get_logger,
    log_event,
    start_session_id,
    truncate_for_log,
)
from sqlexplore.ui.query_editor import SqlQueryEditor
from sqlexplore.ui.tui_app import SqlExplorerTui

app = typer.Typer(
    help="Interactive DuckDB SQL explorer for CSV/TSV/TXT/Parquet files.",
    pretty_exceptions_enable=False,
    pretty_exceptions_show_locals=True,
)

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


def _build_engine(
    resolved_paths: tuple[Path, ...],
    table_name: str,
    database: str,
    limit: int,
    max_rows: int,
    max_value_chars: int,
) -> SqlExplorerEngine:
    first_data_path = resolved_paths[0]
    if len(resolved_paths) > 1:
        return SqlExplorerEngine(
            data_path=first_data_path,
            data_paths=resolved_paths,
            table_name=table_name,
            database=database,
            default_limit=limit,
            max_rows_display=max_rows,
            max_value_chars=max_value_chars,
        )
    return SqlExplorerEngine(
        data_path=first_data_path,
        table_name=table_name,
        database=database,
        default_limit=limit,
        max_rows_display=max_rows,
        max_value_chars=max_value_chars,
    )


def _version_callback(version: bool) -> None:
    if not version:
        return
    typer.echo(f"sqlexplore {app_version()}")
    raise typer.Exit()


@app.command()
def main(
    data: list[str] | None = typer.Option(
        None,
        "--data",
        help="Data source path/URL. Repeat --data to load multiple sources. Use --data - for stdin text.",
    ),
    table_name: str = typer.Option("data", "--table", "-t", help="Logical table/view name inside DuckDB."),
    limit: int = typer.Option(100, "--limit", "-l", min=1, help="Default helper query row limit."),
    max_rows: int = typer.Option(10000, "--max-rows", min=1, help="Maximum rows displayed in the result grid."),
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
        help="When any --data value is an HTTP(S) URL, overwrite existing local download if present.",
    ),
    download_dir: Path = typer.Option(
        data_paths.default_download_dir(),
        "--download-dir",
        help="When any --data value is an HTTP(S) URL, directory used for downloaded files.",
    ),
    version: bool = typer.Option(
        False,
        "--version",
        callback=_version_callback,
        is_eager=True,
        help="Show sqlexplore version and exit.",
    ),
) -> None:
    start_session_id()
    version_text = app_version()
    log_path = configure_file_logging()
    logger.info("startup version=%s log_path=%s", version_text, log_path)
    logger.debug(
        "startup args data_count=%s data=%s table_name=%s limit=%s max_rows=%s max_value_chars=%s database=%s "
        "no_ui=%s overwrite=%s download_dir=%s execute_chars=%s query_file=%s",
        len(data) if data is not None else 0,
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
    resolved_data_sources = data_args.resolve_main_data_sources(
        data,
        download_dir=download_dir,
        overwrite=overwrite,
        startup_activity_messages=startup_activity_messages,
    )
    resolved_paths = resolved_data_sources.paths
    use_stdin = resolved_data_sources.use_stdin
    stdin_capture = resolved_data_sources.stdin_capture
    log_event(
        "session.start",
        {
            "version": version_text,
            "table_name": table_name,
            "database": database,
            "data_paths": [str(path) for path in resolved_paths],
            "use_stdin": use_stdin,
            "overwrite": overwrite,
        },
        logger=logger,
    )
    logger.info("data sources resolved count=%s use_stdin=%s paths=%s", len(resolved_paths), use_stdin, resolved_paths)
    for file_path in resolved_paths:
        try:
            logger.debug("data file metadata=%s", data_paths.file_debug_metadata(file_path))
        except OSError:
            logger.exception("failed to stat data file path=%s", file_path)

    engine = _build_engine(
        resolved_paths,
        table_name,
        database,
        limit,
        max_rows,
        max_value_chars,
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
