from typing import TYPE_CHECKING

import pytest

from streaming.utils import make_event, parse_bool

if TYPE_CHECKING:
    from streaming.types import JSON


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("yes", True),
        ("true", True),
        ("1", True),
        ("y", True),
        ("t", True),
        ("no", False),
        ("false", False),
        ("0", False),
        ("n", False),
        ("f", False),
        (1, True),
        (True, True),
        (0, False),
        (False, False),
        (None, False),
        ("random", False),
    ],
)
def test_parse_bool(value: str, expected: bool) -> None:
    assert parse_bool(value) == expected


def test_make_event_with_string_message() -> None:
    event = make_event("hello", event="test_event", domain="test_domain")
    assert event == {"event": "test_event", "domain": "test_domain", "payload": {"message": "hello"}}


def test_make_event_with_json_message() -> None:
    json_message: JSON = {"key": "value", "number": 123}
    event = make_event(json_message, event="json_event", domain="json_domain")
    assert event == {"event": "json_event", "domain": "json_domain", "payload": {"key": "value", "number": 123}}


def test_make_event_default_values() -> None:
    event = make_event("simple")
    assert event == {"event": "", "domain": "", "payload": {"message": "simple"}}
