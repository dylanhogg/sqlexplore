import re
import sys
import time
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse
from urllib.request import Request, urlopen

import typer
from tqdm import tqdm

from sqlexplore.core.logging_utils import get_logger

REMOTE_FILENAME_SAFE_CHARS_RE = re.compile(r"[^A-Za-z0-9._-]+")
REMOTE_DOWNLOAD_CHUNK_SIZE = 5 * 1024 * 1024
logger = get_logger(__name__)


type EmitLogFn = Callable[..., None]
type RequestFactoryFn = Callable[..., Request]
type UrlOpenFn = Callable[..., Any]
type IsattyFn = Callable[[], bool]
type DownloadRemoteFn = Callable[[str, Path, bool, list[str] | None], Path]


def is_http_url(value: str) -> bool:
    parsed = urlparse(value.strip())
    return parsed.scheme.lower() in {"http", "https"} and bool(parsed.netloc)


def default_download_dir() -> Path:
    return Path(typer.get_app_dir("sqlexplore")) / "downloads"


def format_byte_count(size_bytes: int) -> str:
    if size_bytes < 1024:
        return f"{size_bytes} B"
    if size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    if size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"


def remote_filename(url: str) -> str:
    parsed = urlparse(url)
    file_name = Path(parsed.path).name
    if not file_name:
        host_name = REMOTE_FILENAME_SAFE_CHARS_RE.sub("-", parsed.netloc).strip("-")
        file_name = f"{host_name or 'download'}.parquet"
    if Path(file_name).suffix.lower() not in {".csv", ".tsv", ".txt", ".parquet", ".pq"}:
        raise typer.BadParameter("Remote URL must end with .csv, .tsv, .txt, .parquet, or .pq.")
    return file_name


def emit_download_log(
    message: str,
    activity_messages: list[str] | None = None,
    *,
    err: bool = False,
    echo: bool = True,
    include_activity: bool = True,
    echo_fn: Callable[..., None] | None = None,
) -> None:
    if echo:
        if echo_fn is None:
            typer.echo(message, err=err)
        else:
            echo_fn(message, err=err)
    if include_activity and activity_messages is not None:
        activity_messages.append(message)
    if err:
        logger.error(message)
    else:
        logger.info(message)


def remote_content_length(response: Any) -> int | None:
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


def ensure_download_dir(download_dir: Path) -> Path:
    expanded = download_dir.expanduser()
    if expanded.exists() and not expanded.is_dir():
        raise typer.BadParameter(f"Download path is not a directory: {expanded}")
    try:
        expanded.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise typer.BadParameter(f"Download directory is not writable: {expanded}: {exc}") from exc
    return expanded.resolve()


def download_remote_data_file(
    url: str,
    download_dir: Path,
    overwrite: bool = False,
    activity_messages: list[str] | None = None,
    *,
    emit_log: EmitLogFn = emit_download_log,
    request_factory: RequestFactoryFn = Request,
    urlopen_fn: UrlOpenFn = urlopen,
    isatty_fn: IsattyFn = lambda: sys.stderr.isatty(),
) -> Path:
    destination_dir = ensure_download_dir(download_dir)
    file_name = remote_filename(url)
    destination = (destination_dir / file_name).resolve()

    emit_log(f"[download] remote={url}", activity_messages)
    emit_log(f"[download] local={destination}", activity_messages)

    if destination.exists() and not overwrite:
        emit_log(
            (
                f"[download] Cached local download file {destination.name} already exists, skipping download. "
                "Use --overwrite to replace it."
            ),
            activity_messages,
        )
        return destination
    if destination.exists() and overwrite:
        emit_log(f"[download] Overwriting local download file {destination.name}", activity_messages)

    start = time.perf_counter()
    progress_bar: Any | None = None
    try:
        request = request_factory(url, headers={"User-Agent": "sqlexplore"})
        with urlopen_fn(request) as response, destination.open("wb") as file_handle:
            total_bytes = remote_content_length(response)
            progress_bar = tqdm(
                total=total_bytes,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                desc="download",
                leave=False,
                disable=not isatty_fn(),
            )
            while True:
                chunk = response.read(REMOTE_DOWNLOAD_CHUNK_SIZE)
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
    emit_log(
        (
            "[download] Complete "
            f"elapsed={elapsed_seconds:.3f}s "
            f"size={format_byte_count(file_size_bytes)} ({file_size_bytes:,} bytes) "
        ),
        activity_messages,
    )
    return destination


def resolve_data_path(
    data: str,
    download_dir: Path,
    overwrite: bool = False,
    startup_activity_messages: list[str] | None = None,
    *,
    download_remote: DownloadRemoteFn = download_remote_data_file,
) -> Path:
    value = data.strip()
    if not value:
        raise typer.BadParameter("Data path cannot be empty.")
    if is_http_url(value):
        resolved_download_dir = download_dir.expanduser().resolve()
        logger.info("resolving remote data source url=%s download_dir=%s", value, resolved_download_dir)
        return download_remote(
            value,
            resolved_download_dir,
            overwrite,
            startup_activity_messages,
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


def file_debug_metadata(file_path: Path) -> dict[str, Any]:
    stat = file_path.stat()
    return {
        "path": str(file_path),
        "suffix": file_path.suffix.lower(),
        "size_bytes": stat.st_size,
        "mtime_epoch": stat.st_mtime,
        "is_symlink": file_path.is_symlink(),
    }
