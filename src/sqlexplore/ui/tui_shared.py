import json
import math
import re
from dataclasses import dataclass
from enum import Enum
from typing import Literal, cast

from rich.style import Style
from rich.text import Text
from textual.binding import Binding, BindingType

from sqlexplore.core.engine import is_struct_type_name

CellValue = object
RenderedCell = str | Text
JsonObject = dict[str, object]
JsonArray = list[object]
JsonContainer = JsonObject | JsonArray
PreviewContent = str | Text


class CompletionMode(Enum):
    CLOSED = "closed"
    AUTO = "auto"
    MANUAL = "manual"


@dataclass(frozen=True, slots=True)
class ShortcutSpec:
    key: str
    action: str
    description: str
    show: bool = True
    key_display: str | None = None


@dataclass(slots=True)
class DragDeltaState:
    start_screen: float
    last_delta: int = 0
    last_applied_delta: int = 0
    did_drag: bool = False


PaneId = Literal["query", "results", "cell_detail", "activity"]
PaneResizePhase = Literal["start", "update", "end"]

PANE_QUERY: PaneId = "query"
PANE_RESULTS: PaneId = "results"
PANE_CELL_DETAIL: PaneId = "cell_detail"
PANE_ACTIVITY: PaneId = "activity"
PANE_ORDER: tuple[PaneId, ...] = (PANE_QUERY, PANE_RESULTS, PANE_CELL_DETAIL, PANE_ACTIVITY)
SPLITTER_PANE_PAIRS: tuple[tuple[PaneId, PaneId], ...] = (
    (PANE_QUERY, PANE_RESULTS),
    (PANE_RESULTS, PANE_CELL_DETAIL),
    (PANE_CELL_DETAIL, PANE_ACTIVITY),
)
FIXED_HEIGHT_PANES: tuple[PaneId, ...] = (PANE_QUERY, PANE_CELL_DETAIL, PANE_ACTIVITY)


SHORTCUT_SPECS: tuple[ShortcutSpec, ...] = (
    ShortcutSpec("ctrl+enter", "run_query", "Run", key_display="F5"),
    ShortcutSpec("f5", "run_query", "Run", show=False),
    ShortcutSpec("ctrl+n", "load_sample", "Sample"),
    ShortcutSpec("f6", "load_sample", "Sample", show=False),
    ShortcutSpec("ctrl+l", "clear_editor", "Clear"),
    ShortcutSpec("f7", "clear_editor", "Clear", show=False),
    ShortcutSpec("ctrl+1", "focus_editor", "Editor"),
    ShortcutSpec("ctrl+2", "focus_results", "Results"),
    ShortcutSpec("ctrl+b", "toggle_sidebar", "Data Explorer", key_display="^b"),
    ShortcutSpec("f3", "toggle_json_rendering", "JSON View"),
    ShortcutSpec("f4", "view_selected_cell_full", "View Full"),
    ShortcutSpec("f2", "copy_selected_cell", "Copy Cell"),
    ShortcutSpec("f8", "copy_results_tsv", "Copy TSV"),
    ShortcutSpec("f1", "show_help", "Help"),
    ShortcutSpec("ctrl+shift+p", "show_help", "Help", show=False),
    ShortcutSpec("f10", "quit", "Quit"),
    ShortcutSpec("ctrl+q", "quit", "Quit", show=False),
)


def build_shortcuts(*, for_editor: bool) -> list[BindingType]:
    bindings: list[BindingType] = []
    for shortcut in SHORTCUT_SPECS:
        action = f"app.{shortcut.action}" if for_editor else shortcut.action
        bindings.append(
            Binding(
                shortcut.key,
                action,
                shortcut.description,
                show=shortcut.show,
                key_display=shortcut.key_display,
                priority=True,
            )
        )
    return bindings


JSON_HIGHLIGHT_MAX_TOTAL_ROWS = 100_000
JSON_DETECTION_SAMPLE_SIZE = 12
JSON_DETECTION_MIN_VALID = 3
JSON_DETECTION_MIN_RATIO = 0.7
JSON_CELL_MAX_PARSE_CHARS = 4_096
INLINE_CELL_PREVIEW_MAX_CHARS = 4_096
INLINE_CELL_PREVIEW_MAX_LINES = 1
DETAIL_PREVIEW_MAX_CHARS = 8_192
DETAIL_PREVIEW_MAX_LINES = 64
URL_COLOR = "#74B6E6"
URL_STYLE = Style(color=URL_COLOR, underline=True)
NULL_VALUE_COLOR = "#9CB0C2"
NULL_VALUE_STYLE = Style(color=NULL_VALUE_COLOR)
URL_TRAILING_PUNCTUATION = ".,;:!?)]}"
URL_RE = re.compile(r"(?P<url>(?:https?|ftp)://[^\s<>'\"`]+)", re.IGNORECASE)
MAX_ACTIVITY_LOG_CHARS = 8_000


def _iter_url_matches(text: str) -> list[tuple[int, int, str]]:
    matches: list[tuple[int, int, str]] = []
    for match in URL_RE.finditer(text):
        raw = match.group("url")
        trimmed = raw.rstrip(URL_TRAILING_PUNCTUATION)
        if not trimmed:
            continue
        start = match.start("url")
        end = start + len(trimmed)
        if end <= start:
            continue
        matches.append((start, end, trimmed))
    return matches


def _url_style(url: str, *, clickable: bool) -> Style:
    if not clickable:
        return URL_STYLE
    return URL_STYLE + Style.from_meta({"@click": f"app.open_preview_link({url!r})"})


def stylize_links(text: Text, *, clickable: bool) -> bool:
    matched = False
    for start, end, url in _iter_url_matches(text.plain):
        text.stylize(_url_style(url, clickable=clickable), start, end)
        matched = True
    return matched


def _looks_like_json_container(text: str) -> bool:
    return (text[0] == "{" and text[-1] == "}") or (text[0] == "[" and text[-1] == "]")


def column_looks_like_json(rows: list[tuple[CellValue, ...]], column_index: int) -> bool:
    sample_count = 0
    valid_count = 0
    for row in rows:
        if column_index >= len(row):
            continue
        value = row[column_index]
        if value is None:
            continue
        sample_count += 1
        if isinstance(value, dict | list):
            valid_count += 1
            if sample_count >= JSON_DETECTION_SAMPLE_SIZE:
                break
            continue

        text = str(value).strip()
        if len(text) <= JSON_CELL_MAX_PARSE_CHARS and text and _looks_like_json_container(text):
            parsed = _parse_json_container_text(text)
            if parsed is not None:
                valid_count += 1

        if sample_count >= JSON_DETECTION_SAMPLE_SIZE:
            break

    if sample_count == 0:
        return False
    if valid_count < JSON_DETECTION_MIN_VALID:
        return False
    required = max(JSON_DETECTION_MIN_VALID, math.ceil(sample_count * JSON_DETECTION_MIN_RATIO))
    return valid_count >= required


def _coerce_json_container(value: object) -> JsonContainer | None:
    if isinstance(value, dict):
        normalized: JsonObject = {}
        for key, item in cast(dict[object, object], value).items():
            normalized[str(key)] = item
        return normalized
    if isinstance(value, list):
        normalized_list: JsonArray = []
        for item in cast(list[object], value):
            normalized_list.append(item)
        return normalized_list
    if not isinstance(value, str):
        return None
    inner = value.strip()
    if not inner or not _looks_like_json_container(inner):
        return None
    try:
        parsed_obj = cast(object, json.loads(inner))
    except (TypeError, ValueError):
        return None
    return _coerce_json_container(parsed_obj)


def _parse_json_container_text(text: str) -> JsonContainer | None:
    if not text:
        return None
    candidates = [text]
    if '\\"' in text:
        candidates.append(text.replace('\\"', '"'))
    for candidate in candidates:
        try:
            parsed_obj = cast(object, json.loads(candidate))
        except (TypeError, ValueError):
            continue
        container = _coerce_json_container(parsed_obj)
        if container is not None:
            return container
    return None


def _normalize_embedded_json(value: object, *, max_decode_depth: int = 2, decode_depth: int = 0) -> object:
    if isinstance(value, dict):
        normalized: JsonObject = {}
        for key, item in cast(dict[object, object], value).items():
            normalized[str(key)] = _normalize_embedded_json(
                item,
                max_decode_depth=max_decode_depth,
                decode_depth=decode_depth,
            )
        return normalized
    if isinstance(value, list):
        normalized_list: JsonArray = []
        for item in cast(list[object], value):
            normalized_list.append(
                _normalize_embedded_json(
                    item,
                    max_decode_depth=max_decode_depth,
                    decode_depth=decode_depth,
                )
            )
        return normalized_list
    if not isinstance(value, str) or decode_depth >= max_decode_depth:
        return value
    parsed = _parse_json_container_text(value.strip())
    if parsed is None:
        return value
    return _normalize_embedded_json(parsed, max_decode_depth=max_decode_depth, decode_depth=decode_depth + 1)


def compact_json_cell(value: object) -> str | None:
    if isinstance(value, dict | list):
        try:
            normalized = _normalize_embedded_json(cast(object, value))
            return json.dumps(normalized, separators=(",", ":"), default=str)
        except (TypeError, ValueError):
            return None

    text = str(value).strip()
    if not text or len(text) > JSON_CELL_MAX_PARSE_CHARS:
        return None
    if not _looks_like_json_container(text):
        return None
    parsed = _parse_json_container_text(text)
    if parsed is None:
        return None
    normalized = _normalize_embedded_json(parsed)
    return json.dumps(normalized, separators=(",", ":"), default=str)


def _style_at_offset(text: Text, offset: int) -> str | None:
    for span in reversed(text.spans):
        if span.start <= offset < span.end:
            return str(span.style)
    return None


def truncate_highlighted_text(text: Text, max_chars: int | None) -> Text:
    plain = text.plain
    if max_chars is None or len(plain) <= max_chars:
        return text
    if max_chars <= 3:
        return text[:max_chars]
    kept = max_chars - 3
    out = text[:kept]
    ellipsis_style = _style_at_offset(text, kept - 1)
    out.append("...", style=ellipsis_style)
    out.end = ""
    out.no_wrap = True
    return out


def clamp_preview_text(
    text: str,
    *,
    max_chars: int,
    max_lines: int,
    single_line: bool = False,
) -> tuple[str, bool]:
    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    truncated = False
    if max_lines > 0:
        lines = normalized.split("\n")
        if len(lines) > max_lines:
            normalized = "\n".join(lines[:max_lines])
            truncated = True
    if single_line and "\n" in normalized:
        normalized = normalized.replace("\n", " \\n ")
        truncated = True
    if max_chars >= 0 and len(normalized) > max_chars:
        normalized = normalized[:max_chars]
        truncated = True
    if not truncated:
        return normalized, False
    if max_chars == 0:
        return "", True
    if max_chars <= 3:
        return "." * max_chars, True
    if len(normalized) >= max_chars:
        normalized = normalized[: max_chars - 3]
    return f"{normalized}...", True


def preview_type_label(type_name: str) -> str:
    if not type_name:
        return "unknown"
    if is_struct_type_name(type_name):
        return "STRUCT"
    stripped = type_name.strip()
    if len(stripped) <= 40:
        return stripped
    return f"{stripped[:37]}..."


def pretty_json_cell(value: object) -> str | None:
    if isinstance(value, dict | list):
        try:
            normalized = _normalize_embedded_json(cast(object, value))
            return json.dumps(normalized, indent=2, default=str)
        except (TypeError, ValueError):
            return None

    text = str(value).strip()
    if not text or not _looks_like_json_container(text):
        return None
    parsed = _parse_json_container_text(text)
    if parsed is None:
        return None
    normalized = _normalize_embedded_json(parsed)
    return json.dumps(normalized, indent=2, default=str)
