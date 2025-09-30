import logging
import sys
from typing import TYPE_CHECKING, Any

from ._base import BaseBackend

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from ..types import EventType


class ConsoleBackend(BaseBackend):
    def __init__(self, url: str) -> None:
        super().__init__(url)
        self.stream = self.get_option("stream", "stdout")

    def publish(self, routing_key: str, message: "EventType") -> bool:
        stream = getattr(sys, self.stream)
        stream.write(f"routing_key:{routing_key} message:{message}\n")
        return True

    def listen(self, **kwargs: Any) -> None:
        pass

    def connect(self, raise_if_error: bool = False) -> None:
        pass
