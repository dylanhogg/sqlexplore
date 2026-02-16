from .models import SIMPLE_IDENT_RE


def parse_optional_positive_int(raw: str) -> int | None:
    lowered = raw.strip().lower()
    if lowered in {"off", "none"}:
        return None
    try:
        value = int(lowered)
    except ValueError:
        return None
    if value <= 0:
        return None
    return value


def parse_single_positive_int_arg(raw: str) -> int | None:
    parts = raw.strip().split()
    if len(parts) != 1:
        return None
    return parse_optional_positive_int(parts[0])


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def is_simple_ident(name: str) -> bool:
    return bool(SIMPLE_IDENT_RE.fullmatch(name))


def is_numeric_type(type_name: str) -> bool:
    upper = type_name.upper()
    return any(marker in upper for marker in ("INT", "DOUBLE", "FLOAT", "DECIMAL", "REAL", "NUMERIC"))
