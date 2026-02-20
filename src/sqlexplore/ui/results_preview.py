from typing import Any

from rich.text import Text
from textual.widgets import TextArea

from sqlexplore.ui.tui_shared import PreviewContent, stylize_links


class ResultsPreview(TextArea):
    def __init__(self, content: PreviewContent = "", **kwargs: Any) -> None:
        super().__init__("", read_only=True, soft_wrap=True, show_cursor=False, highlight_cursor_line=False, **kwargs)
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
        # With read_only=True and show_cursor=False, TextArea.move_cursor is a no-op.
        # Reset selection explicitly so stale selection does not survive new preview content.
        self.selection = type(self.selection).cursor((0, 0))

    def get_line(self, line_index: int) -> Text:
        if self._rendered_lines is not None and line_index < len(self._rendered_lines):
            rendered = self._rendered_lines[line_index].copy()
            rendered.end = ""
        else:
            line_text = self.document.get_line(line_index)
            rendered = Text(line_text, end="")
        stylize_links(rendered, clickable=True)
        return rendered
