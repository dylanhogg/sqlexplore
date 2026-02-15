import csv
import json
import math
import re
from dataclasses import dataclass
from enum import Enum
from io import StringIO
from typing import Any, Callable, cast

from rich.highlighter import JSONHighlighter
from rich.style import Style
from rich.syntax import Syntax
from rich.text import Text
from textual.app import App, ComposeResult
from textual.binding import Binding, BindingType
from textual.containers import Horizontal, Vertical
from textual.coordinate import Coordinate
from textual.events import Blur, Focus, Key
from textual.widget import Widget
from textual.widgets import DataTable, Footer, Header, OptionList, Static, TextArea

from sqlexplore.completion.models import (
    DEFAULT_HELPER_COMMANDS,
    HELPER_PREFIX_RE,
    IDENT_PREFIX_RE,
    QUOTED_PREFIX_RE,
    CompletionItem,
    CompletionResult,
)
from sqlexplore.engine import (
    EngineResponse,
    QueryResult,
    ResultStatus,
    SqlExplorerEngine,
    app_version,
    format_scalar,
    is_struct_type_name,
    is_varchar_type,
    result_columns,
    sort_cell_key,
)
from sqlexplore.image_cells import format_image_cell_token, format_image_preview_metadata, summarize_image_cell

CellValue = object
RenderedCell = str | Text
JsonObject = dict[str, object]
JsonArray = list[object]
JsonContainer = JsonObject | JsonArray


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


SHORTCUT_SPECS: tuple[ShortcutSpec, ...] = (
    ShortcutSpec("ctrl+enter", "run_query", "Run"),
    ShortcutSpec("f5", "run_query", "Run", show=False),
    ShortcutSpec("ctrl+n", "load_sample", "Sample"),
    ShortcutSpec("f6", "load_sample", "Sample", show=False),
    ShortcutSpec("ctrl+l", "clear_editor", "Clear"),
    ShortcutSpec("f7", "clear_editor", "Clear", show=False),
    ShortcutSpec("ctrl+1", "focus_editor", "Editor"),
    ShortcutSpec("ctrl+2", "focus_results", "Results"),
    ShortcutSpec("ctrl+b", "toggle_sidebar", "Data Explorer", key_display="^b"),
    ShortcutSpec("f3", "toggle_json_rendering", "JSON View"),
    ShortcutSpec("f2", "copy_selected_cell", "Copy Cell"),
    ShortcutSpec("f8", "copy_results_tsv", "Copy TSV"),
    ShortcutSpec("f1", "show_help", "Help"),
    ShortcutSpec("ctrl+shift+p", "show_help", "Help", show=False),
    ShortcutSpec("f10", "quit", "Quit"),
    ShortcutSpec("ctrl+q", "quit", "Quit", show=False),
)


def _build_shortcuts(*, for_editor: bool) -> list[BindingType]:
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
URL_COLOR = "#74B6E6"
URL_STYLE = Style(color=URL_COLOR, underline=True)
NULL_VALUE_COLOR = "#9CB0C2"
NULL_VALUE_STYLE = Style(color=NULL_VALUE_COLOR)
URL_TRAILING_PUNCTUATION = ".,;:!?)]}"
URL_RE = re.compile(r"(?P<url>(?:https?|ftp)://[^\s<>'\"`]+)", re.IGNORECASE)


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


def _stylize_links(text: Text, *, clickable: bool) -> bool:
    matched = False
    for start, end, url in _iter_url_matches(text.plain):
        text.stylize(_url_style(url, clickable=clickable), start, end)
        matched = True
    return matched


def _looks_like_json_container(text: str) -> bool:
    return (text[0] == "{" and text[-1] == "}") or (text[0] == "[" and text[-1] == "]")


def _column_looks_like_json(rows: list[tuple[CellValue, ...]], column_index: int) -> bool:
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


def _compact_json_cell(value: object) -> str | None:
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


def _truncate_highlighted_text(text: Text, max_chars: int | None) -> Text:
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


def _preview_type_label(type_name: str) -> str:
    if not type_name:
        return "unknown"
    if is_struct_type_name(type_name):
        return "STRUCT"
    stripped = type_name.strip()
    if len(stripped) <= 40:
        return stripped
    return f"{stripped[:37]}..."


def _pretty_json_cell(value: object) -> str | None:
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


class SqlQueryEditor(TextArea):
    BINDINGS = _build_shortcuts(for_editor=True)

    def __init__(
        self,
        text: str,
        token_provider: Callable[[], list[str]],
        history_prev: Callable[[], str | None],
        history_next: Callable[[], str | None],
        completion_provider: Callable[[str, tuple[int, int]], CompletionResult] | None = None,
        helper_command_provider: Callable[[], list[str]] | None = None,
        completion_changed: Callable[[list[CompletionItem], int, bool], None] | None = None,
        completion_accepted: Callable[[CompletionItem], None] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(text, language="sql", theme="monokai", tab_behavior="indent", soft_wrap=False, **kwargs)
        self._token_provider = token_provider
        self._history_prev = history_prev
        self._history_next = history_next
        self._completion_provider = completion_provider
        self._helper_command_provider = helper_command_provider or (lambda: list(DEFAULT_HELPER_COMMANDS))
        self._completion_changed = completion_changed
        self._completion_accepted = completion_accepted
        self._completion_items: list[CompletionItem] = []
        self._completion_index = 0
        self._completion_mode = CompletionMode.CLOSED
        self._suspend_completion_refresh = False
        self._last_completion_signature: tuple[str, tuple[int, int], bool, CompletionMode] | None = None
        self._sql_syntax = Syntax(
            "",
            "sql",
            theme="monokai",
            word_wrap=False,
            line_numbers=False,
            indent_guides=False,
            background_color="default",
        )
        self.indent_width = 4

    def dismiss_completion_menu(self) -> None:
        self._completion_items = []
        self._completion_index = 0
        self._completion_mode = CompletionMode.CLOSED
        self.suggestion = ""
        self._notify_completion_change()

    def _is_completion_open(self) -> bool:
        return self._completion_mode is not CompletionMode.CLOSED

    def _notify_completion_change(self) -> None:
        if self._completion_changed is None:
            return
        is_open = self._is_completion_open()
        items = self._completion_items if is_open else []
        index = self._completion_index if items else 0
        self._completion_changed(items, index, is_open)

    def _refresh_completion_state(self, *, force_open: bool = False) -> None:
        if self._completion_provider is None:
            self.dismiss_completion_menu()
            return
        signature = (self.text, self.cursor_location, self.has_focus, self._completion_mode)
        if not force_open and signature == self._last_completion_signature:
            return
        self._last_completion_signature = signature
        result = self._completion_provider(self.text, self.cursor_location)
        completions = result.items
        if not completions:
            self.dismiss_completion_menu()
            return
        self._completion_items = completions
        self._completion_index = max(0, min(self._completion_index, len(self._completion_items) - 1))
        if not self.has_focus:
            self._completion_mode = CompletionMode.CLOSED
        elif force_open:
            self._completion_mode = CompletionMode.MANUAL
        elif self._completion_mode is CompletionMode.MANUAL:
            self._completion_mode = CompletionMode.MANUAL
        elif result.should_auto_open:
            self._completion_mode = CompletionMode.AUTO
        else:
            self._completion_mode = CompletionMode.CLOSED
        self._notify_completion_change()

    def _apply_inline_suggestion_from_selected_completion(self) -> None:
        if not self._completion_items:
            self.suggestion = ""
            return
        row, col = self.cursor_location
        item = self._completion_items[self._completion_index]
        if item.replacement_end != col:
            self.suggestion = ""
            return
        replacement_start = max(0, min(item.replacement_start, col))
        current_text = self.document[row][replacement_start:col]
        if not current_text:
            self.suggestion = ""
            return
        if not item.insert_text.casefold().startswith(current_text.casefold()):
            self.suggestion = ""
            return
        self.suggestion = item.insert_text[len(current_text) :]

    def _move_completion_selection(self, delta: int) -> None:
        if not self._is_completion_open() or not self._completion_items:
            return
        self._completion_index = (self._completion_index + delta) % len(self._completion_items)
        self._notify_completion_change()
        self._apply_inline_suggestion_from_selected_completion()

    def _prepare_for_cursor_motion(self) -> None:
        if self._is_completion_open():
            self.dismiss_completion_menu()

    def accept_completion_at_index(self, index: int) -> bool:
        if not self._completion_items:
            return False
        target_index = max(0, min(index, len(self._completion_items) - 1))
        self._completion_index = target_index
        item = self._completion_items[self._completion_index]
        row, col = self.cursor_location
        if item.replacement_end != col:
            return False
        start = (row, item.replacement_start)
        end = (row, item.replacement_end)
        self._suspend_completion_refresh = True
        try:
            result = self.replace(item.insert_text, start, end, maintain_selection_offset=False)
            self.move_cursor(result.end_location)
        finally:
            self._suspend_completion_refresh = False
        self.dismiss_completion_menu()
        if self._completion_accepted is not None:
            self._completion_accepted(item)
        return True

    def set_completion_index(self, index: int, *, notify: bool = True) -> None:
        if not self._completion_items:
            return
        target_index = max(0, min(index, len(self._completion_items) - 1))
        if target_index == self._completion_index:
            return
        self._completion_index = target_index
        if notify:
            self._notify_completion_change()
        self._apply_inline_suggestion_from_selected_completion()

    def _accept_selected_completion(self) -> bool:
        if not self._is_completion_open() or not self._completion_items:
            return False
        return self.accept_completion_at_index(self._completion_index)

    async def _on_key(self, event: Key) -> None:
        if event.key == "ctrl+space":
            event.stop()
            event.prevent_default()
            self._refresh_completion_state(force_open=True)
            self._apply_inline_suggestion_from_selected_completion()
            return
        if event.key == "escape" and self._is_completion_open():
            event.stop()
            event.prevent_default()
            self.dismiss_completion_menu()
            return
        if event.key == "enter" and self._is_completion_open():
            self.dismiss_completion_menu()
        if event.key == "up" and self._is_completion_open():
            event.stop()
            event.prevent_default()
            self._move_completion_selection(-1)
            return
        if event.key == "down" and self._is_completion_open():
            event.stop()
            event.prevent_default()
            self._move_completion_selection(1)
            return
        if event.key == "tab":
            event.stop()
            event.prevent_default()
            if self._is_completion_open():
                self._refresh_completion_state(force_open=self._completion_mode is CompletionMode.MANUAL)
                self._apply_inline_suggestion_from_selected_completion()
                if self._accept_selected_completion():
                    return
                self.dismiss_completion_menu()
            self.insert(" " * self._find_columns_to_next_tab_stop())
            return
        await super()._on_key(event)

    def action_cursor_left(self, select: bool = False) -> None:
        self._prepare_for_cursor_motion()
        super().action_cursor_left(select)

    def action_cursor_right(self, select: bool = False) -> None:
        self._prepare_for_cursor_motion()
        super().action_cursor_right(select)

    def action_cursor_word_left(self, select: bool = False) -> None:
        self._prepare_for_cursor_motion()
        super().action_cursor_word_left(select)

    def action_cursor_word_right(self, select: bool = False) -> None:
        self._prepare_for_cursor_motion()
        super().action_cursor_word_right(select)

    def action_cursor_line_start(self, select: bool = False) -> None:
        self._prepare_for_cursor_motion()
        super().action_cursor_line_start(select)

    def action_cursor_line_end(self, select: bool = False) -> None:
        self._prepare_for_cursor_motion()
        super().action_cursor_line_end(select)

    def action_cursor_page_up(self) -> None:
        self._prepare_for_cursor_motion()
        super().action_cursor_page_up()

    def action_cursor_page_down(self) -> None:
        self._prepare_for_cursor_motion()
        super().action_cursor_page_down()

    def action_cursor_up(self, select: bool = False) -> None:
        self._prepare_for_cursor_motion()
        if select:
            super().action_cursor_up(select)
            return
        row, _ = self.cursor_location
        if row == 0:
            prior = self._history_prev()
            if prior is not None:
                self.load_text(prior)
                self.move_cursor((self.document.line_count - 1, len(self.document[-1])))
                return
        super().action_cursor_up(select)

    def action_cursor_down(self, select: bool = False) -> None:
        self._prepare_for_cursor_motion()
        if select:
            super().action_cursor_down(select)
            return
        row, _ = self.cursor_location
        if row == self.document.line_count - 1:
            nxt = self._history_next()
            if nxt is not None:
                self.load_text(nxt)
                self.move_cursor((self.document.line_count - 1, len(self.document[-1])))
                return
        super().action_cursor_down(select)

    def update_suggestion(self) -> None:
        if self._suspend_completion_refresh:
            self.suggestion = ""
            return
        if self._completion_provider is not None:
            self._refresh_completion_state()
            if not self._is_completion_open():
                self.suggestion = ""
                return
            self._apply_inline_suggestion_from_selected_completion()
            return

        row, col = self.cursor_location
        left_text = self.document[row][:col]
        prefix_match = (
            HELPER_PREFIX_RE.search(left_text)
            or QUOTED_PREFIX_RE.search(left_text)
            or IDENT_PREFIX_RE.search(left_text)
        )
        if prefix_match is None:
            self.suggestion = ""
            return
        prefix = prefix_match.group(1)
        prefix_lower = prefix.lower()
        for token in self._token_provider():
            if not token.lower().startswith(prefix_lower):
                continue
            candidate = token
            if token.isupper() and prefix.islower():
                candidate = token.lower()
            elif token.islower() and prefix.isupper():
                candidate = token.upper()
            if candidate.lower() == prefix_lower:
                continue
            self.suggestion = candidate[len(prefix) :]
            return
        self.suggestion = ""

    def on_focus(self, _event: Focus) -> None:
        self.suggestion = ""

    def on_blur(self, _event: Blur) -> None:
        self._last_completion_signature = None
        self.call_after_refresh(self._dismiss_completion_after_blur)

    def _dismiss_completion_after_blur(self) -> None:
        focused = self.screen.focused
        if focused is self:
            return
        if focused is not None and focused.id == "completion_menu":
            return
        self.dismiss_completion_menu()

    def get_line(self, line_index: int) -> Text:
        line_string = self.document.get_line(line_index)
        if not line_string:
            return Text("", end="", no_wrap=True)
        if line_string.lstrip().startswith("/"):
            return self._highlight_helper_command_line(line_string)

        highlighted = self._sql_syntax.highlight(line_string)
        # Rich appends a trailing newline to highlighted output; remove only that
        # so user-typed trailing spaces remain intact for correct cursor rendering.
        if highlighted.plain.endswith("\n"):
            highlighted = highlighted[:-1]
        highlighted.end = ""
        highlighted.no_wrap = True
        return highlighted

    def _highlight_helper_command_line(self, line: str) -> Text:
        rendered = Text(end="", no_wrap=True)
        indent_count = len(line) - len(line.lstrip())
        if indent_count:
            rendered.append(line[:indent_count])

        command_line = line[indent_count:]
        if not command_line.startswith("/"):
            rendered.append(command_line, style="bright_white")
            return rendered

        command_end = len(command_line)
        for idx, char in enumerate(command_line):
            if char.isspace():
                command_end = idx
                break
        command = command_line[:command_end]
        remainder = command_line[command_end:]
        helper_commands = {item.casefold() for item in self._helper_command_provider()}
        command_style = "bold cyan" if command.casefold() in helper_commands else "bold red"
        rendered.append(command, style=command_style)
        if remainder:
            rendered.append(remainder, style="bright_white")
        return rendered


PreviewContent = str | Text


class ResultsPreview(TextArea):
    def __init__(self, content: PreviewContent = "", **kwargs: Any) -> None:
        super().__init__("", read_only=True, soft_wrap=True, **kwargs)
        self._content: PreviewContent = ""
        self._rendered_lines: list[Text] | None = None
        self.update(content)

    @property
    def content(self) -> PreviewContent:
        return self._content

    def update(self, content: PreviewContent = "", *, layout: bool = True) -> None:
        self._content = content
        if isinstance(content, Text):
            self._rendered_lines = list(content.split("\n", allow_blank=True))
            content_text = content.plain
        else:
            self._rendered_lines = None
            content_text = str(content)
        self.load_text(content_text)
        self.move_cursor((0, 0))

    def get_line(self, line_index: int) -> Text:
        if self._rendered_lines is not None and line_index < len(self._rendered_lines):
            rendered = self._rendered_lines[line_index].copy()
            rendered.end = ""
        else:
            line_text = self.document.get_line(line_index)
            rendered = Text(line_text, end="")
        _stylize_links(rendered, clickable=True)
        return rendered


class ResultsTable(DataTable[RenderedCell]):
    COMPONENT_CLASSES = DataTable.COMPONENT_CLASSES | {"results-table--cursor-row"}
    DEFAULT_CSS = """
    ResultsTable > .results-table--cursor-row {
        background: #223744;
    }
    """

    def _get_row_style(self, row_index: int, base_style: Style) -> Style:
        row_style = super()._get_row_style(row_index, base_style)
        if not self.show_cursor or self.cursor_type != "cell":
            return row_style
        if row_index < 0 or row_index != self.cursor_row:
            return row_style
        return row_style + self.get_component_styles("results-table--cursor-row").rich_style

    def watch_cursor_coordinate(
        self,
        old_coordinate: Coordinate,
        new_coordinate: Coordinate,
    ) -> None:
        super().watch_cursor_coordinate(old_coordinate, new_coordinate)
        if self.cursor_type != "cell":
            return
        for row_index in {old_coordinate.row, new_coordinate.row}:
            self.refresh_row(row_index)


class SqlExplorerTui(App[None]):
    TITLE = f"sqlexplorer v{app_version()}"
    SUB_TITLE = "explore your data"

    CSS = """
    Screen {
        layout: vertical;
    }

    #body {
        height: 1fr;
    }

    #sidebar {
        width: 38;
        border: round #4f9da6;
        padding: 1;
        background: #102025;
        color: #f0f4f8;
    }

    #workspace {
        width: 1fr;
        padding: 0 1;
    }

    .section-title {
        color: #9abed8;
        text-style: bold;
        margin-top: 1;
    }

    #query_editor {
        height: 5;
        border: round #4d7ea8;
    }

    #completion_menu {
        display: none;
        max-height: 8;
        border: round #4d7ea8;
        margin-bottom: 0;
        background: #0f1b22;
    }

    #completion_hint {
        display: none;
        color: #9abed8;
        margin: 0 0 1 1;
    }

    #results_table {
        height: 1fr;
        border: round #5b8f67;
    }

    #results_preview {
        height: 5;
        border: round #5b8f67;
        color: #d8e3ec;
        overflow-y: scroll;
    }

    #activity_log {
        height: 5;
        border: round #b18b3d;
        overflow-y: scroll;
    }
    """

    BINDINGS = _build_shortcuts(for_editor=False)

    def __init__(
        self,
        engine: SqlExplorerEngine,
        startup_activity_messages: list[str] | None = None,
        startup_query: str | None = None,
    ) -> None:
        super().__init__()
        self.engine = engine
        self._startup_activity_messages = tuple(startup_activity_messages or [])
        self._startup_query = startup_query if startup_query is not None else self.engine.default_query
        self._history_cursor: int | None = None
        self._completion_window_start = 0
        self._active_result: QueryResult | None = None
        self._base_rows: list[tuple[CellValue, ...]] = []
        self._sort_column_index: int | None = None
        self._sort_reverse = False
        self._active_data_total_rows: int | None = None
        self._last_results_tsv = ""
        self._results_preview_plain_text = ""
        self._activity_lines: list[str] = []
        self._json_highlighter = JSONHighlighter()
        self._json_rendering_enabled = True

    def _reset_history_cursor(self) -> None:
        self._history_cursor = None

    def _history_prev(self) -> str | None:
        history = self.engine.executed_sql
        if not history:
            return None
        if self._history_cursor is None:
            self._history_cursor = len(history) - 1
        else:
            self._history_cursor = (self._history_cursor - 1) % len(history)
        return history[self._history_cursor]

    def _history_next(self) -> str | None:
        history = self.engine.executed_sql
        if not history:
            return None
        if self._history_cursor is None:
            self._history_cursor = 0
        else:
            self._history_cursor = (self._history_cursor + 1) % len(history)
        return history[self._history_cursor]

    def _results_table(self) -> DataTable[RenderedCell]:
        return cast(DataTable[RenderedCell], self.query_one("#results_table", DataTable))

    def _query_editor(self) -> SqlQueryEditor:
        return self.query_one("#query_editor", SqlQueryEditor)

    def _completion_menu(self) -> OptionList:
        return self.query_one("#completion_menu", OptionList)

    def _completion_hint(self) -> Static:
        return self.query_one("#completion_hint", Static)

    def _results_preview(self) -> ResultsPreview:
        return self.query_one("#results_preview", ResultsPreview)

    def _activity_log(self) -> TextArea:
        return self.query_one("#activity_log", TextArea)

    @staticmethod
    def completion_option_prompt(item: CompletionItem) -> str:
        kind = item.kind.replace("_", " ")
        prompt = f"{item.insert_text}  [{kind}]"
        if item.kind == "helper_command":
            detail_parts: list[str] = []
            if item.detail:
                detail_parts.append(item.detail)
            if item.usage and ("<" in item.usage or "[" in item.usage):
                detail_parts.append(f"Usage: {item.usage}")
            if detail_parts:
                return f"{prompt} {' '.join(detail_parts)}"
            return prompt
        if item.detail:
            return f"{prompt} {item.detail}"
        return prompt

    def _on_editor_completion_changed(
        self,
        items: list[CompletionItem],
        selected_index: int,
        is_open: bool,
    ) -> None:
        if not self.screen.is_mounted:
            return
        menu = self._completion_menu()
        hint = self._completion_hint()
        if not is_open or not items:
            menu.display = False
            menu.clear_options()
            hint.display = False
            self._completion_window_start = 0
            return
        selected = min(max(0, selected_index), len(items) - 1)
        window_size = 8
        max_window_start = max(0, len(items) - window_size)
        self._completion_window_start = min(max(0, selected - window_size + 1), max_window_start)
        visible_items = items[self._completion_window_start : self._completion_window_start + window_size]
        prompts = [self.completion_option_prompt(item) for item in visible_items]
        menu.set_options(prompts)
        menu.highlighted = selected - self._completion_window_start
        menu.display = True
        hint.display = True

    def _on_editor_completion_accepted(self, item: CompletionItem) -> None:
        self.engine.record_completion_acceptance(item.insert_text)
        self._on_editor_completion_changed([], 0, False)

    def on_option_list_option_highlighted(self, event: OptionList.OptionHighlighted) -> None:
        if event.option_list.id != "completion_menu":
            return
        index = self._completion_index_from_menu_option(event.option_index)
        if index is None:
            return
        # Avoid re-windowing the menu during mouse highlight/select event chains.
        self._query_editor().set_completion_index(index, notify=False)

    def on_option_list_option_selected(self, event: OptionList.OptionSelected) -> None:
        if event.option_list.id != "completion_menu":
            return
        index = self._completion_index_from_menu_option(event.option_index)
        if index is None:
            return
        editor = self._query_editor()
        if editor.accept_completion_at_index(index):
            editor.focus()

    def _completion_index_from_menu_option(self, option_index: int) -> int | None:
        menu = self._completion_menu()
        if option_index < 0 or option_index >= menu.option_count:
            return None
        return self._completion_window_start + option_index

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Horizontal(id="body"):
            with Vertical(id="sidebar"):
                yield Static(self.engine.schema_preview(), id="sidebar_text")
            with Vertical(id="workspace"):
                yield Static("Query", classes="section-title")
                yield SqlQueryEditor(
                    self._startup_query,
                    self.engine.completion_tokens,
                    self._history_prev,
                    self._history_next,
                    completion_provider=self.engine.completion_result,
                    helper_command_provider=self.engine.helper_commands,
                    completion_changed=self._on_editor_completion_changed,
                    completion_accepted=self._on_editor_completion_accepted,
                    id="query_editor",
                )
                yield OptionList(id="completion_menu")
                yield Static("Tab accept | Esc close | Up/Down navigate", id="completion_hint")
                yield Static("Results", id="results_header", classes="section-title")
                yield ResultsTable(id="results_table")
                yield ResultsPreview("", id="results_preview")
                yield Static("Activity", classes="section-title")
                yield TextArea(
                    "",
                    id="activity_log",
                    read_only=True,
                    soft_wrap=True,
                )
        yield Footer()

    def on_mount(self) -> None:
        table = self._results_table()
        table.zebra_stripes = True
        self.query_one("#sidebar", Vertical).display = False
        self._completion_menu().display = False
        self._completion_hint().display = False
        self._set_results_preview_text("Move in Results to preview full cell value. F2 copies selected full value.")
        self._log(f"sqlexplore {app_version()}", "info")
        for message in self._startup_activity_messages:
            self._log(message, "info")
        self._log("Ready. Press Ctrl+Enter/F5 to run SQL. F1 opens help, F10 quits.", "info")
        self.action_run_query()
        self._query_editor().focus()

    def _set_editor_text(self, text: str, *, log_message: str | None = None) -> None:
        editor = self._query_editor()
        editor.dismiss_completion_menu()
        editor.text = text
        self._reset_history_cursor()
        editor.focus()
        if log_message is not None:
            self._log(log_message, "info")

    def action_run_query(self) -> None:
        editor = self._query_editor()
        editor.dismiss_completion_menu()
        query = editor.text
        response = self.engine.run_input(query)
        self._reset_history_cursor()
        self._apply_response(response)

    def action_load_sample(self) -> None:
        self._set_editor_text(self.engine.default_query, log_message="Loaded sample query.")

    def action_clear_editor(self) -> None:
        self._set_editor_text("")

    def action_focus_editor(self) -> None:
        self._query_editor().focus()

    def action_focus_results(self) -> None:
        self._results_table().focus()

    def action_toggle_json_rendering(self) -> None:
        self._json_rendering_enabled = not self._json_rendering_enabled
        state = "on" if self._json_rendering_enabled else "off"
        self._log(f"JSON formatting/highlighting {state}.", "info")
        self._redraw_results_table()

    def action_copy_selected_cell(self) -> None:
        selected = self._selected_cell_context()
        if selected is None:
            self._log("No cell selected to copy.", "error")
            return

        row_index, column_index, value, column_name, type_name = selected
        formatted = self._format_full_cell_value(value, type_name)
        self.copy_to_clipboard(formatted)
        self._update_results_preview(row_index, column_index, column_name, type_name, value)
        self._log(f"Copied full cell value ({column_name}, row {row_index + 1}) to clipboard.", "ok")

    def action_toggle_sidebar(self) -> None:
        sidebar = self.query_one("#sidebar", Vertical)
        sidebar.display = not sidebar.display
        state = "shown" if sidebar.display else "hidden"
        self._log(f"Data Explorer {state}.", "info")

    def action_show_help(self) -> None:
        self._log(self.engine.help_text(), "info")

    def action_open_preview_link(self, href: str) -> None:
        if href:
            self.open_url(href)

    @staticmethod
    def _rows_to_tsv(columns: list[str], rows: list[tuple[CellValue, ...]]) -> str:
        output = StringIO(newline="")
        writer = csv.writer(output, delimiter="\t", quoting=csv.QUOTE_MINIMAL, lineterminator="\r\n")
        writer.writerow(columns)
        writer.writerows(rows)
        return output.getvalue()

    def action_copy_results_tsv(self) -> None:
        sql = self.engine.last_result_sql
        if sql is None:
            self._log("No query result available to copy.", "error")
            return

        try:
            relation = self.engine.conn.execute(sql)
            rows = relation.fetchall()
            columns = result_columns(relation.description)
        except Exception as exc:  # noqa: BLE001
            self._log(f"Copy failed: {exc}", "error")
            return

        if not columns:
            self._log("No tabular query result available to copy.", "error")
            return

        tsv_text = self._rows_to_tsv(columns, rows)
        self._last_results_tsv = tsv_text
        self.copy_to_clipboard(tsv_text)

        message = f"Copied {len(rows):,} rows x {len(columns)} cols as TSV (full query result). Paste into Excel."
        if self._active_result is not None and self._active_result.truncated:
            out_of_rows = self._results_out_of_rows(self._active_result)
            message += f" Results pane shows {len(self._active_result.rows):,}/{out_of_rows:,} rows."
        self._log(message, "ok")

    def _apply_response(self, response: EngineResponse) -> None:
        sql_log = response.activity_sql_log()
        if sql_log is not None:
            title, sql_text = sql_log
            self._log(f"{title}: {sql_text}", "sql")

        if response.result is not None:
            self._render_table(response.result)

        if response.message:
            self._log(response.message, response.status)

        if response.load_query is not None:
            self._set_editor_text(response.load_query)

        if response.clear_editor:
            self.action_clear_editor()

        if response.should_exit:
            self.exit()

        sidebar = self.query_one("#sidebar_text", Static)
        sidebar.update(self.engine.schema_preview())

    def _render_table(self, result: QueryResult) -> None:
        self._active_result = result
        self._base_rows = list(result.rows)
        self._sort_column_index = None
        self._sort_reverse = False
        self._active_data_total_rows = self.engine.row_count() if result.sql else None

        self._redraw_results_table()

    def _results_out_of_rows(self, result: QueryResult) -> int:
        if not result.sql:
            return result.total_rows
        data_total_rows = self._active_data_total_rows
        if data_total_rows is None:
            data_total_rows = self.engine.row_count()
            self._active_data_total_rows = data_total_rows
        return max(result.total_rows, data_total_rows)

    def _detect_json_columns(self, result: QueryResult) -> set[int]:
        if not self._json_rendering_enabled:
            return set()
        if result.total_rows > JSON_HIGHLIGHT_MAX_TOTAL_ROWS:
            return set()
        if not result.rows:
            return set()
        if len(result.column_types) != len(result.columns):
            return set()

        detected: set[int] = set()
        for index, type_name in enumerate(result.column_types):
            if is_struct_type_name(type_name):
                detected.add(index)
                continue
            if not is_varchar_type(type_name):
                continue
            if _column_looks_like_json(result.rows, index):
                detected.add(index)
        return detected

    def _render_json_cell(self, value: CellValue) -> RenderedCell:
        if not self._json_rendering_enabled:
            return self._render_scalar_cell(value)
        image = summarize_image_cell(value)
        if image is not None:
            return Text(format_image_cell_token(image), end="", no_wrap=True)
        compact = _compact_json_cell(value)
        if compact is None:
            return self._render_scalar_cell(value)
        highlighted = Text(compact, end="", no_wrap=True)
        self._json_highlighter.highlight(highlighted)
        truncated = _truncate_highlighted_text(highlighted, self.engine.max_value_chars)
        _stylize_links(truncated, clickable=False)
        return truncated

    def _render_scalar_cell(self, value: CellValue) -> RenderedCell:
        if value is None:
            return Text("NULL", style=NULL_VALUE_STYLE, end="", no_wrap=True)
        if not self._json_rendering_enabled:
            text = format_scalar(value, self.engine.max_value_chars)
            rendered = Text(text, end="", no_wrap=True)
            if not _stylize_links(rendered, clickable=False):
                return text
            return rendered
        image = summarize_image_cell(value)
        if image is not None:
            return Text(format_image_cell_token(image), end="", no_wrap=True)
        text = format_scalar(value, self.engine.max_value_chars)
        rendered = Text(text, end="", no_wrap=True)
        if not _stylize_links(rendered, clickable=False):
            return text
        return rendered

    def _json_rendering_status_suffix(self) -> str:
        state = "on" if self._json_rendering_enabled else "off"
        return f" [json:{state}]"

    def _redraw_results_table(self) -> None:
        result = self._active_result
        table = self._results_table()
        table.clear(columns=True)
        if result is None or not result.columns:
            self.query_one("#results_header", Static).update(f"Results{self._json_rendering_status_suffix()}")
            self._set_results_preview_text("Move in Results to preview full cell value. F2 copies selected full value.")
            return

        table.add_columns(*result.columns)
        json_columns = self._detect_json_columns(result)
        for row in result.rows:
            rendered_row: list[RenderedCell] = []
            for index, value in enumerate(row):
                if index in json_columns:
                    rendered_row.append(self._render_json_cell(value))
                else:
                    rendered_row.append(self._render_scalar_cell(value))
            table.add_row(*rendered_row)

        out_of_rows = self._results_out_of_rows(result)
        header = f"Results ({len(result.rows):,}/{out_of_rows:,} rows, {result.elapsed_ms:.1f} ms)"
        if result.truncated:
            header += " [truncated]"
        if self._sort_column_index is not None and self._sort_column_index < len(result.columns):
            direction = "desc" if self._sort_reverse else "asc"
            header += f" [sorted: {result.columns[self._sort_column_index]} {direction}]"
        header += self._json_rendering_status_suffix()
        self.query_one("#results_header", Static).update(header)
        self._refresh_results_preview_from_cursor()

    def _set_results_preview_text(self, text: str) -> None:
        preview = self._results_preview()
        self._results_preview_plain_text = text
        preview.update(text)
        preview.scroll_home(animate=False, immediate=True)

    def _selected_cell_context_at(
        self,
        row_index: int,
        column_index: int,
    ) -> tuple[int, int, CellValue, str, str] | None:
        result = self._active_result
        if result is None or not result.rows:
            return None
        if row_index < 0 or row_index >= len(result.rows):
            return None
        row = result.rows[row_index]
        if column_index < 0 or column_index >= len(row):
            return None
        column_name = result.columns[column_index] if column_index < len(result.columns) else f"col_{column_index + 1}"
        type_name = result.column_types[column_index] if column_index < len(result.column_types) else ""
        return row_index, column_index, row[column_index], column_name, type_name

    def _selected_cell_context(self) -> tuple[int, int, CellValue, str, str] | None:
        table = self._results_table()
        return self._selected_cell_context_at(table.cursor_row, table.cursor_column)

    def _format_full_cell_value(self, value: CellValue, type_name: str) -> str:
        formatted_value, _ = self._format_preview_value(value, type_name)
        return formatted_value

    def _format_preview_value(self, value: CellValue, type_name: str) -> tuple[str, bool]:
        value_any = cast(Any, value)
        if value is None:
            return "NULL", False
        if not self._json_rendering_enabled:
            return str(value_any), False
        image = summarize_image_cell(value_any)
        if image is not None:
            metadata = format_image_preview_metadata(image)
            return f"{metadata}\nraw:\n{value_any}", False
        should_try_json = is_struct_type_name(type_name) or is_varchar_type(type_name) or isinstance(value, dict | list)
        if should_try_json:
            pretty = _pretty_json_cell(value_any)
            if pretty is not None:
                return pretty, True
        return str(value_any), False

    def _render_preview_value(self, value: CellValue, type_name: str) -> Text:
        if value is None:
            return Text("NULL", style=NULL_VALUE_STYLE, end="")
        formatted_value, is_json = self._format_preview_value(value, type_name)
        rendered = Text(formatted_value, end="")
        if is_json:
            self._json_highlighter.highlight(rendered)
        return rendered

    def _safe_len(self, value: CellValue) -> int | None:
        if not value:
            return None
        if isinstance(value, dict):
            return len(cast(JsonObject, value))
        if isinstance(value, list):
            return len(cast(JsonArray, value))
        if isinstance(value, str):
            return len(value)
        return len(str(value))

    def _update_results_preview(
        self,
        row_index: int,
        column_index: int,
        column_name: str,
        type_name: str,
        value: CellValue,
    ) -> None:
        type_label = _preview_type_label(type_name)
        value_len = self._safe_len(value)
        header = f"{column_name}, {type_label}, row {row_index + 1}, col {column_index + 1}, len {value_len}"
        preview_text = Text(f"{header}\n", end="")
        preview_text.append_text(self._render_preview_value(value, type_name))
        preview = self._results_preview()
        self._results_preview_plain_text = preview_text.plain
        preview.update(preview_text)
        preview.scroll_home(animate=False, immediate=True)

    def _refresh_results_preview_from_cursor(self) -> None:
        selected = self._selected_cell_context()
        if selected is None:
            self._set_results_preview_text("Move in Results to preview full cell value. F2 copies selected full value.")
            return
        row_index, column_index, value, column_name, type_name = selected
        self._update_results_preview(row_index, column_index, column_name, type_name, value)

    def _refresh_results_preview_at(self, row_index: int, column_index: int) -> None:
        selected = self._selected_cell_context_at(row_index, column_index)
        if selected is None:
            return
        selected_row, selected_column, value, column_name, type_name = selected
        self._update_results_preview(selected_row, selected_column, column_name, type_name, value)

    def on_data_table_header_selected(self, event: DataTable.HeaderSelected) -> None:
        result = self._active_result
        if result is None or not result.rows:
            return

        if self._sort_column_index == event.column_index:
            self._sort_reverse = not self._sort_reverse
        else:
            self._sort_column_index = event.column_index
            self._sort_reverse = False

        column_index = event.column_index
        result.rows = sorted(
            self._base_rows,
            key=lambda row: sort_cell_key(row[column_index]) if column_index < len(row) else (2, 0, ""),
            reverse=self._sort_reverse,
        )
        self._redraw_results_table()
        direction = "DESC" if self._sort_reverse else "ASC"
        self._log(f"Sorted results by {event.label.plain} {direction}", "info")

    def on_data_table_cell_highlighted(self, event: DataTable.CellHighlighted) -> None:
        self._refresh_results_preview_at(event.coordinate.row, event.coordinate.column)

    def on_data_table_cell_selected(self, event: DataTable.CellSelected) -> None:
        self._refresh_results_preview_at(event.coordinate.row, event.coordinate.column)

    def on_key(self, event: Key) -> None:
        if event.key != "ctrl+c":
            return
        focused = self.focused
        if focused is None:
            return
        if focused is self._results_preview():
            selected = self._selected_text(focused)
            event.stop()
            event.prevent_default()
            self.copy_to_clipboard(selected if selected is not None else self._results_preview_plain_text)
            return
        if focused is self._activity_log():
            selected = self._selected_text(focused)
            if selected is None:
                return
            event.stop()
            event.prevent_default()
            self.copy_to_clipboard(selected)

    @staticmethod
    def _selected_text(widget: Widget) -> str | None:
        if isinstance(widget, TextArea):
            if widget.selected_text:
                return widget.selected_text
        selection = widget.text_selection
        if selection is None:
            return None
        extracted = widget.get_selection(selection)
        if extracted is None:
            return None
        text, _ = extracted
        if not text:
            return None
        return text

    def _log(self, message: str, status: ResultStatus) -> None:
        logger = self._activity_log()
        self._activity_lines.append(f"[{status.upper()}] {message}")
        logger.load_text("\n".join(self._activity_lines))
        if logger.document.line_count:
            last_line = logger.document.line_count - 1
            logger.move_cursor((last_line, len(logger.document[last_line])))
