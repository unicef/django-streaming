from collections.abc import Callable
from datetime import datetime
from typing import Literal, TypedDict

from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties

JsonT = None | bool | int | float | str | list["JsonT"] | dict[str, "JsonT"]
JSON = dict[str, JsonT]

PikaCallback = Callable[[BlockingChannel, Basic.Deliver, BasicProperties, bytes], None]
UserCallback = Callable[[str, BlockingChannel, Basic.Deliver, BasicProperties, bytes], None]

class EventType(TypedDict):
    timestamp: str | datetime
    event: str
    payload: JSON
    type: Literal["absolute", "delta", "event"]
