from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import typer

SchemaSignature = tuple[tuple[str, str], ...]


@dataclass(frozen=True, slots=True)
class StartupTableInfo:
    role: Literal["source", "union"]
    table_name: str
    source: str
    row_count: int


def normalize_data_paths(data_path: Path, data_paths: tuple[Path, ...] | None) -> tuple[Path, ...]:
    if data_paths is None:
        return (data_path.expanduser().resolve(),)
    normalized = tuple(path.expanduser().resolve() for path in data_paths)
    if not normalized:
        raise typer.BadParameter("At least one data file is required.")
    return normalized


def source_view_name(table_name: str, source_index: int) -> str:
    return f"{table_name}_src_{source_index + 1}"


def schema_signature_from_rows(rows: list[tuple[Any, ...]]) -> SchemaSignature:
    return tuple((str(row[0]), str(row[1])) for row in rows)


def format_schema_mismatch(
    expected: SchemaSignature,
    actual: SchemaSignature,
    *,
    source_index: int,
    source_label: str,
) -> str:
    prefix = f"Schema mismatch in source {source_index}: {source_label}"
    if len(expected) != len(actual):
        return f"{prefix}. Expected {len(expected)} columns, got {len(actual)}."
    for column_index, (expected_col, actual_col) in enumerate(zip(expected, actual), start=1):
        expected_name, expected_type = expected_col
        actual_name, actual_type = actual_col
        if expected_name != actual_name:
            return f"{prefix}. Column {column_index} name mismatch: expected {expected_name}, got {actual_name}."
        if expected_type != actual_type:
            return (
                f"{prefix}. Column {column_index} type mismatch for {expected_name}: "
                f"expected {expected_type}, got {actual_type}."
            )
    return f"{prefix}. Source schema does not match expected schema."


def union_sql(source_views: tuple[str, ...]) -> str:
    selects = [f'SELECT * FROM "{source_view}"' for source_view in source_views]
    return " UNION ALL ".join(selects)


def count_table_rows(conn: Any, table_name: str) -> int:
    out = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()
    if out is None:
        return 0
    return int(out[0])
