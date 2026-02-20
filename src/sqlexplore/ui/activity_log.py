import re
from typing import Any, cast

from rich.syntax import Syntax
from rich.text import Text
from textual.widgets import TextArea

from sqlexplore.core.engine_models import ResultStatus
from sqlexplore.ui.tui_shared import stylize_links

_STATUS_LINE_RE = re.compile(r"^\[(?P<status>OK|INFO|SQL|ERROR)\]\s?(?P<message>.*)$")
_KEY_VALUE_RE = re.compile(r"\b(?P<key>[A-Za-z_][A-Za-z0-9_]*)=(?P<value>[^\s]+)")
_LLM_TAG_RE = re.compile(r"\[llm\]", re.IGNORECASE)

_STATUS_PREFIX_STYLE: dict[ResultStatus, str] = {
    "ok": "bold #6EC27A",
    "info": "bold #7AB6E8",
    "sql": "bold #D8B46A",
    "error": "bold #E57373",
}

_STATUS_MESSAGE_STYLE: dict[ResultStatus, str] = {
    "ok": "#D9F5DF",
    "info": "#D8E3EC",
    "sql": "#EEDDB3",
    "error": "#F4B2B2",
}

_KV_KEY_STYLE = "bold #9ABED8"
_LLM_TAG_STYLE = "bold #B0A8E0"


class ActivityLog(TextArea):
    def __init__(self, text: str = "", **kwargs: Any) -> None:
        super().__init__(
            text,
            read_only=True,
            soft_wrap=True,
            show_cursor=False,
            highlight_cursor_line=False,
            **kwargs,
        )
        self._sql_syntax = Syntax(
            "",
            "sql",
            theme="monokai",
            word_wrap=True,
            line_numbers=False,
            indent_guides=False,
            background_color="default",
        )

    def get_line(self, line_index: int) -> Text:
        line = self.document.get_line(line_index)
        rendered = self._render_activity_line(line)
        stylize_links(rendered, clickable=True)
        return rendered

    def _render_activity_line(self, line: str) -> Text:
        if not line:
            return Text("", end="")

        match = _STATUS_LINE_RE.match(line)
        if match is None:
            return Text(line, end="")

        status = cast(ResultStatus, match.group("status").lower())
        message = match.group("message")
        rendered = Text(end="")
        rendered.append(f"[{status.upper()}] ", style=_STATUS_PREFIX_STYLE[status])
        message_start = len(rendered.plain)
        if status == "sql":
            self._append_sql_message(rendered, message)
        else:
            rendered.append(message, style=_STATUS_MESSAGE_STYLE[status])
        self._stylize_message_tokens(rendered, start=message_start)
        return rendered

    def _append_sql_message(self, rendered: Text, message: str) -> None:
        title, separator, sql_text = message.partition(": ")
        if separator and title in {"Generated", "Executed"} and sql_text:
            rendered.append(f"{title}:", style="bold #E6C17A")
            rendered.append(" ")
            highlighted = self._sql_syntax.highlight(sql_text)
            if highlighted.plain.endswith("\n"):
                highlighted = highlighted[:-1]
            highlighted.end = ""
            rendered.append_text(highlighted)
            return
        rendered.append(message, style=_STATUS_MESSAGE_STYLE["sql"])

    @staticmethod
    def _stylize_message_tokens(rendered: Text, *, start: int) -> None:
        plain = rendered.plain
        for match in _LLM_TAG_RE.finditer(plain, start):
            rendered.stylize(_LLM_TAG_STYLE, match.start(), match.end())
        for match in _KEY_VALUE_RE.finditer(plain, start):
            key_start, key_end = match.span("key")
            rendered.stylize(_KV_KEY_STYLE, key_start, key_end)
