import logging
import socket
from collections.abc import Callable
from inspect import signature
from typing import TYPE_CHECKING, Any, get_type_hints

from colorama import Fore
from django.core.serializers.json import DjangoJSONEncoder
from django.db import models
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import ChannelClosedByBroker

from streaming.types import JSON, EventType, UserCallback

if TYPE_CHECKING:
    from streaming.event import Event

MINUTE = 60
HOUR = MINUTE * 60
DAY = HOUR * 24


class LevelFormatter(logging.Formatter):
    template = f"$color$%(levelname)s{Fore.RESET}: {Fore.LIGHTWHITE_EX}%(name)s{Fore.RESET} - %(message)s"
    default = Fore.LIGHTWHITE_EX
    colors = {
        logging.DEBUG: Fore.MAGENTA,
        logging.CRITICAL: Fore.RED,
        logging.ERROR: Fore.RED,
        logging.INFO: Fore.BLUE,
        logging.WARN: Fore.YELLOW,
    }

    def format(self, record: logging.LogRecord) -> str:
        color = self.colors.get(record.levelno, self.default)
        log_fmt = self.template.replace("$color$", color)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


class StreamingJSONEncoder(DjangoJSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, models.Model):
            return str(o)
        return super().default(o)


def parse_bool(value: Any) -> bool:
    if isinstance(value, str):
        return value.lower() in ("yes", "true", "1", "y", "t")
    return value in [1, True]


def make_event(message: "str|JSON", *, value_type: "EventType" = "absolute", message_id: str | None = None) -> "Event":
    from streaming.event import Event

    return Event.build(
        data=message,
        value_type=value_type,
        message_id=message_id,
    )


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


def check_callback(
    func: Callable[
        [
            Any,
        ],
        Any,
    ],
) -> bool:
    expected = UserCallback
    try:
        sig = signature(func)
        params = list(sig.parameters.values())
        hints = get_type_hints(func)

        # For collections.abc.Callable, __args__ is ([arg1, arg2, ...], return_type)
        expected_arg_types = expected.__args__[:-1]  # type: ignore[attr-defined]
        expected_return_type = expected.__args__[-1]  # type: ignore[attr-defined]

        if len(params) != len(expected_arg_types):
            return False
        return_hint = hints.get("return", type(None))
        if expected_return_type is None:
            expected_return_type = type(None)

        for i, param in enumerate(params):
            if hints.get(param.name) != expected_arg_types[i]:
                return False

        return bool(return_hint == expected_return_type)

    except (TypeError, NameError, AttributeError, IndexError):
        # TypeError -> not a callable or invalid annotations
        # NameError -> forward refs not resolvable
        # AttributeError -> in case expected is malformed
        # IndexError -> if __args__ is not what we expect
        return False
