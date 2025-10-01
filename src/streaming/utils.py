import socket
from typing import TYPE_CHECKING, Any

from django.core.serializers.json import DjangoJSONEncoder
from django.db import models
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import ChannelClosedByBroker

if TYPE_CHECKING:
    from streaming.event import Event
    from streaming.types import JSON


MINUTE = 60
HOUR = MINUTE * 60
DAY = HOUR * 24


class StreamingJSONEncoder(DjangoJSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, models.Model):
            return str(o)
        return super().default(o)


def parse_bool(value: Any) -> bool:
    if isinstance(value, str):
        return value.lower() in ("yes", "true", "1", "y", "t")
    return value in [1, True]


def make_event(message: "str | JSON", *, key: str = "") -> "Event":
    from streaming.event import Event

    return Event.build(key=key, data=message, value_type="absolute")


def get_local_ip() -> str:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    s.connect(("<broadcast>", 12345))  # 12345 is random port. 0 fails on Mac.
    return str(s.getsockname()[0])


def exchange_exists(channel: BlockingChannel | None, exchange_name: str) -> bool:
    try:
        channel.exchange_declare(exchange=exchange_name, passive=True)  # type: ignore[union-attr]
        return True
    except (ChannelClosedByBroker, AttributeError):
        return False
