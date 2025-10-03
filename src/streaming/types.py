import inspect
from collections.abc import Callable
from typing import Literal, get_type_hints
from uuid import UUID

from pika import BasicProperties
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic

JsonT = None | bool | int | float | str | list["JsonT"] | dict[str, "JsonT"] | UUID
JSON = dict[str, JsonT]

EventType = Literal["absolute", "delta", "event"]
PikaCallback = Callable[[BlockingChannel, Basic.Deliver, BasicProperties, bytes], None]
UserCallback = Callable[[str, BlockingChannel, Basic.Deliver, BasicProperties, bytes], None]


def check_callback(func: Callable, expected: PikaCallback | UserCallback) -> bool:
    try:
        sig = inspect.signature(func)
        params = list(sig.parameters.values())
        hints = get_type_hints(func)
        expected_args = expected.__args__[0]
        expected_return = expected.__args__[1]

        if len(params) != len(expected_args):
            return False

        for param, exp_type in zip(params, expected_args, strict=True):
            ann = hints.get(param.name, None)
            if ann != exp_type:
                return False

        return hints.get("return", None) == expected_return
    except (TypeError, NameError, AttributeError):
        # TypeError -> not a callable or invalid annotations
        # NameError -> forward refs not resolvable
        # AttributeError -> in case expected is malformed
        return False
