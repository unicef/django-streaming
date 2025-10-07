import pytest

from streaming.event import Event


@pytest.mark.parametrize(
    "args",
    [
        {"message_id": "123", "payload": "message"},
        {"message_id": "456", "payload": {"key": "value"}},
    ],
)
def test_event_object(args):
    Event(**args)
