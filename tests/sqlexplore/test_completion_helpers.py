import pytest

from sqlexplore.completion.helpers import (
    parse_optional_positive_int,
    parse_single_positive_int_arg,
)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("off", None),
        ("none", None),
        (" 12 ", 12),
        ("0", None),
        ("-2", None),
        ("bad", None),
    ],
)
def test_parse_optional_positive_int(raw: str, expected: int | None) -> None:
    assert parse_optional_positive_int(raw) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("10", 10),
        ("off", None),
        ("", None),
        ("1 2", None),
        ("none 1", None),
    ],
)
def test_parse_single_positive_int_arg(raw: str, expected: int | None) -> None:
    assert parse_single_positive_int_arg(raw) == expected
