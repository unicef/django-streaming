import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any
from urllib.parse import ParseResult, parse_qs, urlparse

if TYPE_CHECKING:
    from streaming.types import EventType


logger = logging.getLogger(__name__)


class BaseBackend(ABC):
    def __init__(self, url: str) -> None:
        self.connection_url: str = url
        self._parsed_url: ParseResult = urlparse(self.connection_url)
        self._options = {k: v[0] for k, v in parse_qs(self._parsed_url.query).items()}

    def get_option(self, name: str, default: Any = "") -> Any:
        return self._options.get(name, default)

    @abstractmethod
    def connect(self, raise_if_error: bool = False) -> None: ...

    @abstractmethod
    def publish(self, routing_key: str, message: "EventType") -> bool: ...
