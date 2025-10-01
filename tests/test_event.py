import pytest

from streaming.event import Event


@pytest.mark.parametrize(
    "args",
    [
        {"key": "routing.key", "payload": "message"},
        {"key": "routing.key", "payload": {"key": "value"}},
    ],
)
def test_event_object(args):
    Event(**args)
