from typing import Any, Callable

from rich.syntax import Syntax
from rich.text import Text
from textual.events import Blur, Focus, Key
from textual.widgets import TextArea

from sqlexplore.completion.models import (
    DEFAULT_HELPER_COMMANDS,
    HELPER_PREFIX_RE,
    IDENT_PREFIX_RE,
    QUOTED_PREFIX_RE,
    CompletionItem,
    CompletionResult,
)
from sqlexplore.ui.tui_shared import CompletionMode
from sqlexplore.ui.tui_shared import build_shortcuts as _build_shortcuts


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
