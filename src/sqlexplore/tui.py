from __future__ import annotations

import csv
import json
import math
from dataclasses import dataclass
from enum import Enum
from io import StringIO
from typing import Any, Callable, cast

from rich.highlighter import JSONHighlighter
from rich.markup import escape as rich_escape
from rich.syntax import Syntax
from rich.text import Text
from textual.app import App, ComposeResult
from textual.binding import Binding, BindingType
from textual.containers import Horizontal, Vertical
from textual.events import Blur, Focus, Key
from textual.widgets import DataTable, Footer, Header, OptionList, RichLog, Static, TextArea

from sqlexplore.engine import (
    DEFAULT_HELPER_COMMANDS,
    HELPER_PREFIX_RE,
    IDENT_PREFIX_RE,
    QUOTED_PREFIX_RE,
    STATUS_STYLE_BY_RESULT,
    CompletionItem,
    CompletionResult,
    EngineResponse,
    QueryResult,
    ResultStatus,
    SqlExplorerEngine,
    app_version,
    format_scalar,
    is_varchar_type,
    result_columns,
    sort_cell_key,
)

CellValue = object
RenderedCell = str | Text


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
        text = str(value).strip()
        if not text:
            continue

        sample_count += 1
        if len(text) <= JSON_CELL_MAX_PARSE_CHARS and _looks_like_json_container(text):
            try:
                parsed = json.loads(text)
            except (TypeError, ValueError):
                parsed = None
            if isinstance(parsed, dict | list):
                valid_count += 1

        if sample_count >= JSON_DETECTION_SAMPLE_SIZE:
            break

    if sample_count == 0:
        return False
    if valid_count < JSON_DETECTION_MIN_VALID:
        return False
    required = max(JSON_DETECTION_MIN_VALID, math.ceil(sample_count * JSON_DETECTION_MIN_RATIO))
    return valid_count >= required


def _compact_json_cell(value: CellValue) -> str | None:
    text = str(value).strip()
    if not text or len(text) > JSON_CELL_MAX_PARSE_CHARS:
        return None
    if not _looks_like_json_container(text):
        return None
    try:
        parsed = json.loads(text)
    except (TypeError, ValueError):
        return None
    if not isinstance(parsed, dict | list):
        return None
    return json.dumps(parsed, separators=(",", ":"))


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

        parts = command_line.split(maxsplit=1)
        command = parts[0]
        args = parts[1] if len(parts) > 1 else ""
        helper_commands = {item.casefold() for item in self._helper_command_provider()}
        command_style = "bold cyan" if command.casefold() in helper_commands else "bold red"
        rendered.append(command, style=command_style)
        if args:
            rendered.append(" ")
            rendered.append(args, style="bright_white")
        return rendered


class SqlExplorerTui(App[None]):
    TITLE = "sqlexplorer"
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
        height: 10;
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

    #activity_log {
        height: 11;
        border: round #b18b3d;
    }
    """

    BINDINGS = _build_shortcuts(for_editor=False)

    def __init__(self, engine: SqlExplorerEngine, startup_activity_messages: list[str] | None = None) -> None:
        super().__init__()
        self.engine = engine
        self._startup_activity_messages = tuple(startup_activity_messages or [])
        self._history_cursor: int | None = None
        self._completion_window_start = 0
        self._active_result: QueryResult | None = None
        self._base_rows: list[tuple[CellValue, ...]] = []
        self._sort_column_index: int | None = None
        self._sort_reverse = False
        self._last_results_tsv = ""
        self._json_highlighter = JSONHighlighter()

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

    @staticmethod
    def _completion_option_prompt(item: CompletionItem) -> str:
        kind = item.kind.replace("_", " ")
        if item.detail:
            return f"{item.insert_text}  [{kind}] {item.detail}"
        return f"{item.insert_text}  [{kind}]"

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
        prompts = [self._completion_option_prompt(item) for item in visible_items]
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
                    self.engine.default_query,
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
                yield DataTable(id="results_table")
                yield Static("Activity", classes="section-title")
                yield RichLog(id="activity_log", markup=True, highlight=True, wrap=True)
        yield Footer()

    def on_mount(self) -> None:
        table = self._results_table()
        table.zebra_stripes = True
        self._completion_menu().display = False
        self._completion_hint().display = False
        self._log(f"sqlexplore {app_version()}", "info")
        for message in self._startup_activity_messages:
            self._log(message, "info")
        self._log("Ready. Press Ctrl+Enter/F5 to run SQL. F1 opens help, F10 quits.", "info")
        boot = self.engine.run_sql(self.engine.default_query, remember=False)
        self._apply_response(boot)
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

    def action_toggle_sidebar(self) -> None:
        sidebar = self.query_one("#sidebar", Vertical)
        sidebar.display = not sidebar.display
        state = "shown" if sidebar.display else "hidden"
        self._log(f"Data Explorer {state}.", "info")

    def action_show_help(self) -> None:
        self._log(self.engine.help_text(), "info")

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
            message += f" Results pane shows {len(self._active_result.rows):,}/{self._active_result.total_rows:,} rows."
        self._log(message, "ok")

    def _apply_response(self, response: EngineResponse) -> None:
        if response.generated_sql:
            self._log(f"Generated SQL:\n{response.generated_sql}", "info")

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

        self._redraw_results_table()

    def _detect_json_columns(self, result: QueryResult) -> set[int]:
        if result.total_rows > JSON_HIGHLIGHT_MAX_TOTAL_ROWS:
            return set()
        if not result.rows:
            return set()
        if len(result.column_types) != len(result.columns):
            return set()

        detected: set[int] = set()
        for index, type_name in enumerate(result.column_types):
            if not is_varchar_type(type_name):
                continue
            if _column_looks_like_json(result.rows, index):
                detected.add(index)
        return detected

    def _render_json_cell(self, value: CellValue) -> RenderedCell:
        compact = _compact_json_cell(value)
        if compact is None:
            return format_scalar(value, self.engine.max_value_chars)
        if len(compact) > self.engine.max_value_chars:
            return format_scalar(compact, self.engine.max_value_chars)
        text = Text(compact, end="", no_wrap=True)
        self._json_highlighter.highlight(text)
        return text

    def _redraw_results_table(self) -> None:
        result = self._active_result
        table = self._results_table()
        table.clear(columns=True)
        if result is None or not result.columns:
            self.query_one("#results_header", Static).update("Results")
            return

        table.add_columns(*result.columns)
        json_columns = self._detect_json_columns(result)
        for row in result.rows:
            rendered_row: list[RenderedCell] = []
            for index, value in enumerate(row):
                if index in json_columns:
                    rendered_row.append(self._render_json_cell(value))
                else:
                    rendered_row.append(format_scalar(value, self.engine.max_value_chars))
            table.add_row(*rendered_row)

        header = f"Results ({len(result.rows):,}/{result.total_rows:,} rows, {result.elapsed_ms:.1f} ms)"
        if result.truncated:
            header += " [truncated]"
        if self._sort_column_index is not None and self._sort_column_index < len(result.columns):
            direction = "desc" if self._sort_reverse else "asc"
            header += f" [sorted: {result.columns[self._sort_column_index]} {direction}]"
        self.query_one("#results_header", Static).update(header)

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

    def _log(self, message: str, status: ResultStatus) -> None:
        logger = self.query_one("#activity_log", RichLog)
        style_prefix = f"[{STATUS_STYLE_BY_RESULT[status]}]"
        logger.write(f"{style_prefix}{rich_escape(message)}[/]")
