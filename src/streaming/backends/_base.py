from typing import TYPE_CHECKING
from urllib.parse import ParseResult, parse_qs, urlparse

from streaming.config import DEFAULT_QUEUE_NAME

if TYPE_CHECKING:
    from streaming.types import JSON

not_provided = object()


class BaseBackend:
    def __init__(self, url: str) -> None:
        self._parsed_url: ParseResult = urlparse(url)
        self._options = {k: v[0] for k, v in parse_qs(self._parsed_url.query).items()}
        self.queue_name: str = self._options.pop("queue", DEFAULT_QUEUE_NAME)

    def initialize(self) -> None:
        pass

    def publish(self, message: "JSON") -> None:
        raise NotImplementedError()
