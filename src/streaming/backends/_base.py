import atexit
import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any
from urllib.parse import ParseResult, parse_qs, urlparse

from streaming.config import CONFIG
from streaming.utils import get_local_ip

from ..exceptions import (
    StreamingError,
)

if TYPE_CHECKING:
    from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection

    from ..event import Event
    from ..types import UserCallback


logger = logging.getLogger(__name__)

MAX_RETRIES = 5


class BaseBackend(ABC):
    def __init__(self, url: str) -> None:
        self.connection_url: str = url
        self._parsed_url: ParseResult = urlparse(self.connection_url)
        self._options = {k: v[0] for k, v in parse_qs(self._parsed_url.query).items()}
        self.exchange = self.get_option("exchange", "django-streaming-broadcast")

        self.host = str(self._parsed_url.hostname)
        self.port = int(self._parsed_url.port) if self._parsed_url.port else 5672
        self._connection: BlockingConnection | None = None
        self._username = self._parsed_url.username or "guest"
        self._password = self._parsed_url.password or "guest"

        self.channel: BlockingChannel | None = None
        self.exchange = self.get_option("exchange", "django-streaming-broadcast")
        self.retry_exchange = f"retry_{self.exchange}"
        self.timeout = float(self.get_option("timeout", 0.5))
        self.virtual_host = self.get_option("vhost", "/")
        # listener
        self.client_name = CONFIG.CLIENT_NAME or self.get_option("client_name", get_local_ip())
        atexit.register(self.disconnect)

    def get_option(self, name: str, default: Any = "") -> Any:
        return self._options.get(name, default)

    @property
    def client_name(self) -> str:
        return self.__client_name

    @client_name.setter
    def client_name(self, name: str) -> None:
        self.__client_name = name

    @abstractmethod
    def configure_exchanges(self) -> None:  # noqa: B027
        ...

    @abstractmethod
    def configure_queue_routing(self) -> dict[str, list[str]]:  # noqa: B027
        ...

    def get_real_queue_name(self, name: str) -> str:
        return f"{self.client_name}:{name}"

    def set_credential(self, username: str, password: str) -> None:
        if self._connection:
            raise StreamingError("Disconnect first.")
        self.__username = username
        self.__password = password

    @abstractmethod
    def connect(self, raise_if_error: bool = False) -> None:
        pass

    @abstractmethod
    def disconnect(self) -> None:
        pass

    # Publisher
    @abstractmethod
    def publish(self, routing_key: str, message: "Event") -> bool: ...

    @abstractmethod
    def listen(self, callback: "UserCallback", queues: list[str] | None = None, ack: bool = True) -> None:
        pass
