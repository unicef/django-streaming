import logging
from typing import TYPE_CHECKING, Any
from urllib.parse import ParseResult, parse_qs, urlparse

from streaming.config import DEFAULT_QUEUE_NAME

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from streaming.types import EventType

not_provided = object()


class BaseBackend:
    def __init__(self, url: str) -> None:
        self.connection_url: str = url
        self._parsed_url: ParseResult = urlparse(self.connection_url)
        self._options = {k: v[0] for k, v in parse_qs(self._parsed_url.query).items()}
        self.queue_name: str = self._options.pop("queue", DEFAULT_QUEUE_NAME)

    def initialize(self) -> None:
        pass

    def publish(self, message: "EventType", **kwargs: Any) -> None:
        raise NotImplementedError()
