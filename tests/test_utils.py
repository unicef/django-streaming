import datetime
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from pika.adapters.blocking_connection import BlockingChannel

from streaming.utils import exchange_exists, make_event, parse_bool


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


def test_exchange_exists():
    channel = MagicMock(specs=BlockingChannel)
    channel.exchange_declare.side_effect = AttributeError
    assert not exchange_exists(channel, exchange_name="test")

    channel.exchange_declare.side_effect = lambda *a, **kw: True
    assert exchange_exists(channel, exchange_name="test")


def test_make_event_with_string_message():
    event = make_event("hello", key="test_event")
    assert isinstance(event.timestamp, datetime.datetime)
    assert sorted(event.as_dict().keys()) == ["id", "key", "payload", "timestamp", "value_type"]


def test_make_event_with_json_message():
    event = make_event({"key": "value", "number": 123}, key="json_event")
    assert isinstance(event.timestamp, datetime.datetime)
    assert sorted(event.as_dict().keys()) == ["id", "key", "payload", "timestamp", "value_type"]


def test_encoding(admin_user):
    uid = uuid4()
    evt = make_event({"user": admin_user, "uuid": uid, "str": "test"}, key="user.save")
    dump = evt.marshall()
    restored = evt.unmarshal(dump)
    assert restored.key == evt.key
    assert restored.payload["uuid"] == str(uid)
