import datetime
from uuid import uuid4

import pytest

from streaming.utils import json_dumps, json_loads, make_event, parse_bool


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


def test_make_event_with_string_message():
    event = make_event("hello", event="test_event")
    assert isinstance(event["timestamp"], datetime.datetime)
    del event["timestamp"]
    assert event == {"event": "test_event", "type": "absolute", "payload": {"message": "hello"}}


def test_make_event_with_json_message():
    event = make_event({"key": "value", "number": 123}, event="json_event")
    assert isinstance(event["timestamp"], datetime.datetime)
    del event["timestamp"]
    assert event == {"event": "json_event", "type": "absolute", "payload": {"key": "value", "number": 123}}


def test_make_event_default_values():
    event = make_event("simple")
    assert isinstance(event["timestamp"], datetime.datetime)
    del event["timestamp"]
    assert event == {"event": "", "type": "absolute", "payload": {"message": "simple"}}


def test_encoding(admin_user):
    uid = uuid4()
    evt = make_event({"user": admin_user, "uuid": uid, "str": "test"}, event="user.save")
    dump = json_dumps(evt)
    restored = json_loads(dump)
    assert restored["event"] == evt["event"]
    assert restored["payload"]["uuid"] == str(uid)
