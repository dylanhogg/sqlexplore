import re
from typing import Any

from sqlexplore.completion.helpers import is_simple_ident

QUALIFIED_FUNCTION_LABEL_RE = re.compile(
    r'^(?P<prefix>(?:"(?:""|[^"]+)"|[A-Za-z_][A-Za-z0-9_$]*)\.)+(?P<func>"(?:""|[^"]+)"|[A-Za-z_][A-Za-z0-9_$]*)\('
)
QUOTED_FUNCTION_LABEL_RE = re.compile(r'^(?P<func>"(?:""|[^"]+)")\(')


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _normalize_function_ident(token: str) -> str:
    if not (token.startswith('"') and token.endswith('"') and len(token) > 1):
        return token
    unquoted = token[1:-1].replace('""', '"')
    return unquoted if is_simple_ident(unquoted) else token


def _normalize_result_column_label(name: str) -> str:
    qualified_match = QUALIFIED_FUNCTION_LABEL_RE.match(name)
    if qualified_match is not None:
        func_token = _normalize_function_ident(qualified_match.group("func"))
        return f"{func_token}({name[qualified_match.end() :]}"

    quoted_match = QUOTED_FUNCTION_LABEL_RE.match(name)
    if quoted_match is not None:
        func_token = _normalize_function_ident(quoted_match.group("func"))
        return f"{func_token}({name[quoted_match.end() :]}"

    return name


def result_columns(description: list[tuple[Any, ...]] | None) -> list[str]:
    if not description:
        return []
    return [_normalize_result_column_label(str(item[0])) for item in description]


def result_column_types(description: list[tuple[Any, ...]] | None) -> list[str]:
    if not description:
        return []
    return [str(item[1]) for item in description]


def format_scalar(value: Any, max_chars: int | None = None) -> str:
    if value is None:
        return "NULL"
    text = str(value)
    if max_chars is None or len(text) <= max_chars:
        return text
    if max_chars <= 3:
        return text[:max_chars]
    return f"{text[: max_chars - 3]}..."
