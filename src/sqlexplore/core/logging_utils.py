import json
import logging
import tempfile
from collections import deque
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Iterator, Mapping, cast
from uuid import uuid4

import typer

APP_LOGGER_NAME = "sqlexplore"
APP_LOG_FILE_NAME = "sqlexplore.log"
MAX_LOG_FILE_BYTES = 8 * 1024 * 1024
MAX_LOG_FILE_BACKUPS = 5
MAX_LOG_TEXT_CHARS = 64_000
EVENT_LOG_MESSAGE_PREFIX = "event_json="

_configured_log_path: Path | None = None
_session_id = uuid4().hex


def _default_log_dir() -> Path:
    return Path(typer.get_app_dir(APP_LOGGER_NAME)) / "logs"


def _fallback_log_dirs() -> tuple[Path, Path]:
    return (Path.cwd() / ".sqlexplore" / "logs", Path(tempfile.gettempdir()) / APP_LOGGER_NAME / "logs")


def truncate_for_log(value: str, *, max_chars: int = MAX_LOG_TEXT_CHARS) -> str:
    if max_chars < 32:
        max_chars = 32
    if len(value) <= max_chars:
        return value
    over = len(value) - max_chars
    return f"{value[:max_chars]} ... [truncated {over} chars]"


def to_json_for_log(payload: Any, *, max_chars: int = MAX_LOG_TEXT_CHARS) -> str:
    try:
        text = json.dumps(payload, ensure_ascii=True, default=str, sort_keys=True)
    except Exception:  # noqa: BLE001
        text = repr(payload)
    return truncate_for_log(text, max_chars=max_chars)


def _build_file_handler(log_path: Path) -> RotatingFileHandler:
    handler = RotatingFileHandler(
        filename=log_path,
        maxBytes=MAX_LOG_FILE_BYTES,
        backupCount=MAX_LOG_FILE_BACKUPS,
        encoding="utf-8",
    )
    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    handler.setLevel(logging.DEBUG)
    return handler


def configure_file_logging(log_dir: Path | None = None) -> Path | None:
    global _configured_log_path
    if _configured_log_path is not None:
        return _configured_log_path

    candidates: list[Path] = []
    if log_dir is not None:
        candidates.append(log_dir.expanduser())
    else:
        candidates.append(_default_log_dir())
        candidates.extend(_fallback_log_dirs())

    for candidate_dir in candidates:
        try:
            candidate_dir.mkdir(parents=True, exist_ok=True)
            log_path = (candidate_dir / APP_LOG_FILE_NAME).resolve()
            handler = _build_file_handler(log_path)
        except OSError:
            continue

        root_logger = logging.getLogger(APP_LOGGER_NAME)
        root_logger.setLevel(logging.DEBUG)
        root_logger.propagate = False
        for existing in tuple(root_logger.handlers):
            root_logger.removeHandler(existing)
            existing.close()
        root_logger.addHandler(handler)
        _configured_log_path = log_path
        root_logger.info("log initialized path=%s", log_path)
        return log_path

    return None


def reset_file_logging() -> None:
    global _configured_log_path
    root_logger = logging.getLogger(APP_LOGGER_NAME)
    for existing in tuple(root_logger.handlers):
        root_logger.removeHandler(existing)
        existing.close()
    _configured_log_path = None


def get_configured_log_path() -> Path | None:
    return _configured_log_path


def new_trace_id() -> str:
    return uuid4().hex


def _event_payload(event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    event = {
        "event_id": uuid4().hex,
        "kind": event_type,
        "session_id": _session_id,
        "ts": datetime.now(timezone.utc).isoformat(),
    }
    event.update(payload)
    return event


def log_event(
    event_type: str,
    payload: dict[str, Any],
    *,
    logger: logging.Logger | None = None,
    level: int = logging.INFO,
) -> str:
    event = _event_payload(event_type, payload)
    event_text = json.dumps(event, ensure_ascii=True, default=str, sort_keys=True)
    active_logger = logger or get_logger(APP_LOGGER_NAME)
    active_logger.log(level, f"{EVENT_LOG_MESSAGE_PREFIX}{event_text}")
    return str(event["event_id"])


def _parse_event_line(line: str) -> dict[str, Any] | None:
    marker_index = line.find(EVENT_LOG_MESSAGE_PREFIX)
    if marker_index < 0:
        return None
    payload = line[marker_index + len(EVENT_LOG_MESSAGE_PREFIX) :].strip()
    if not payload:
        return None
    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError:
        return None
    if not isinstance(parsed, Mapping):
        return None
    payload_map = cast(Mapping[Any, Any], parsed)
    return {str(key): value for key, value in payload_map.items()}


def _iter_log_files(log_path: Path) -> tuple[Path, ...]:
    files: list[Path] = []
    for backup_index in range(MAX_LOG_FILE_BACKUPS, 0, -1):
        candidate = log_path.with_name(f"{log_path.name}.{backup_index}")
        if candidate.is_file():
            files.append(candidate)
    if log_path.is_file():
        files.append(log_path)
    return tuple(files)


def _iter_parsed_log_events(log_path: Path) -> Iterator[dict[str, Any]]:
    for event_file in _iter_log_files(log_path):
        try:
            with event_file.open("r", encoding="utf-8") as handle:
                for line in handle:
                    parsed = _parse_event_line(line)
                    if parsed is None:
                        continue
                    yield parsed
        except OSError:
            continue


def read_log_events(
    *,
    event_type: str | None = None,
    limit: int = 100,
    log_path: Path | None = None,
) -> list[dict[str, Any]]:
    if limit <= 0:
        return []
    path = log_path or get_configured_log_path()
    if path is None:
        return []

    recent: deque[dict[str, Any]] = deque(maxlen=limit)
    for event in _iter_parsed_log_events(path):
        if event_type is not None and event.get("kind") != event_type:
            continue
        recent.append(event)
    return list(recent)


def find_log_event(
    event_id: str,
    *,
    event_type: str | None = None,
    log_path: Path | None = None,
) -> dict[str, Any] | None:
    payload = event_id.strip()
    if not payload:
        return None
    path = log_path or get_configured_log_path()
    if path is None:
        return None

    found: dict[str, Any] | None = None
    for event in _iter_parsed_log_events(path):
        if event_type is not None and event.get("kind") != event_type:
            continue
        if event.get("event_id") != payload:
            continue
        found = event
    return found


def read_log_events_for_trace(
    trace_id: str,
    *,
    limit: int | None = None,
    log_path: Path | None = None,
) -> list[dict[str, Any]]:
    payload = trace_id.strip()
    if not payload:
        return []
    path = log_path or get_configured_log_path()
    if path is None:
        return []

    maxlen = limit if limit is not None and limit > 0 else None
    events: deque[dict[str, Any]] = deque(maxlen=maxlen)
    for event in _iter_parsed_log_events(path):
        if event.get("trace_id") == payload:
            events.append(event)
    return list(events)


def get_logger(name: str | None = None) -> logging.Logger:
    if name is None:
        return logging.getLogger(APP_LOGGER_NAME)
    if name == APP_LOGGER_NAME:
        return logging.getLogger(name)
    if name.startswith(f"{APP_LOGGER_NAME}."):
        return logging.getLogger(name)
    return logging.getLogger(f"{APP_LOGGER_NAME}.{name}")
