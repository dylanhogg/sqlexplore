import json
import logging
import tempfile
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

import typer

APP_LOGGER_NAME = "sqlexplore"
APP_LOG_FILE_NAME = "sqlexplore.log"
MAX_LOG_FILE_BYTES = 8 * 1024 * 1024
MAX_LOG_FILE_BACKUPS = 5
MAX_LOG_TEXT_CHARS = 64_000

_configured_log_path: Path | None = None


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


def get_logger(name: str | None = None) -> logging.Logger:
    if name is None:
        return logging.getLogger(APP_LOGGER_NAME)
    if name == APP_LOGGER_NAME:
        return logging.getLogger(name)
    if name.startswith(f"{APP_LOGGER_NAME}."):
        return logging.getLogger(name)
    return logging.getLogger(f"{APP_LOGGER_NAME}.{name}")
