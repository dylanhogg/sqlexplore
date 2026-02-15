from sqlexplore.core.result_utils import (
    format_scalar,
    result_column_types,
    result_columns,
)


def test_result_columns_and_types_return_empty_for_none_or_empty_description() -> None:
    assert result_columns(None) == []
    assert result_columns([]) == []
    assert result_column_types(None) == []
    assert result_column_types([]) == []


def test_result_columns_normalize_qualified_and_quoted_function_labels() -> None:
    description = [
        ('duckdb.main."sum"(x)', "BIGINT"),
        ('"count"(*)', "BIGINT"),
        ('"my func"(x)', "BIGINT"),
    ]
    assert result_columns(description) == ["sum(x)", "count(*)", '"my func"(x)']


def test_result_column_types_are_stringified() -> None:
    description = [("x", "INTEGER"), ("y", "VARCHAR")]
    assert result_column_types(description) == ["INTEGER", "VARCHAR"]


def test_format_scalar_handles_none_and_truncation_edges() -> None:
    assert format_scalar(None) == "NULL"
    assert format_scalar("abcdef", 10) == "abcdef"
    assert format_scalar("abcdef", 3) == "abc"
    assert format_scalar("abcdef", 2) == "ab"
    assert format_scalar("abcdef", 5) == "ab..."
