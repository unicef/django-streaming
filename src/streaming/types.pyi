from collections.abc import Callable
from typing import TypedDict

from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties

JsonT = None | bool | int | float | str | list["JsonT"] | dict[str, "JsonT"]
JSON = dict[str, JsonT]

PikaCallback = Callable[[BlockingChannel, Basic.Deliver, BasicProperties, bytes], None]

class EventType(TypedDict):
    event: str
    domain: str
    payload: JSON
