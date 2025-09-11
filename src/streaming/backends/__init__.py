from typing import TYPE_CHECKING
from urllib.parse import urlparse

if TYPE_CHECKING:
    from ._base import BaseBackend

from streaming.exceptions import StreamingConfigError


def get_backend() -> "BaseBackend":
    from streaming.config import CONFIG

    parsed_url = urlparse(CONFIG.BROKER_URL)
    if parsed_url.scheme == "console":
        from .console import Backend as ConsoleBackend

        return ConsoleBackend(CONFIG.BROKER_URL)
    if parsed_url.scheme == "redis":
        from .redis import Backend as RedisBackend

        return RedisBackend(CONFIG.BROKER_URL)
    if parsed_url.scheme == "rabbit":
        from .rabbitmq import Backend as RabbitBackend

        return RabbitBackend(CONFIG.BROKER_URL)
    raise StreamingConfigError(f"Broker not supported: {parsed_url.scheme}")


def initialize_engine() -> None:
    global backend  # noqa: PLW0603
    backend = get_backend()
    backend.initialize()


backend: "BaseBackend | None" = None
