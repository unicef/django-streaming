import logging
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from streaming.exceptions import StreamingConfigError

if TYPE_CHECKING:
    from ._base import BaseBackend

logger = logging.getLogger(__name__)


def get_backend() -> "BaseBackend":
    from streaming.config import CONFIG

    parsed_url = urlparse(CONFIG.BROKER_URL)
    if parsed_url.scheme == "console":
        from .console import ConsoleBackend

        return ConsoleBackend(CONFIG.BROKER_URL)
    if parsed_url.scheme == "redis":
        from .redis import RedisBackend

        return RedisBackend(CONFIG.BROKER_URL)
    if parsed_url.scheme == "rabbit":
        from .rabbitmq import RabbitMQBackend

        return RabbitMQBackend(CONFIG.BROKER_URL)
    if parsed_url.scheme == "debug":
        from .debug import DebugBackend

        return DebugBackend(CONFIG.BROKER_URL)
    raise StreamingConfigError(f"Broker not supported: {parsed_url.scheme}")
