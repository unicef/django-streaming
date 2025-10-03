from collections.abc import Callable
from typing import Literal
from uuid import UUID

from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties

JsonT = None | bool | int | float | str | list["JsonT"] | dict[str, "JsonT"] | UUID
JSON = dict[str, JsonT]

EventType = Literal["absolute", "delta", "event"]
PikaCallback = Callable[[BlockingChannel, Basic.Deliver, BasicProperties, bytes], None]
UserCallback = Callable[[str, BlockingChannel, Basic.Deliver, BasicProperties, bytes], None]
