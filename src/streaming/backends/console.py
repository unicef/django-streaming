import logging
import sys
from typing import TYPE_CHECKING

from ._base import BaseBackend

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from ..event import Event
    from ..types import UserCallback


class ConsoleBackend(BaseBackend):
    def __init__(self, url: str) -> None:
        super().__init__(url)
        self.stream = self.get_option("stream", "stdout")

    def configure_queue_routing(self) -> dict[str, list[str]]:  # pragma: no cover
        return {}

    def configure_exchanges(self) -> None:  # pragma: no cover
        pass

    def publish(self, routing_key: str, message: "Event") -> bool:
        stream = getattr(sys, self.stream)
        stream.write(f"routing_key:{routing_key} message:{message}\n")
        return True

    def listen(self, callback: "UserCallback", queues: list[str] | None = None, ack: bool = True) -> None:
        pass

    def connect(self, raise_if_error: bool = False) -> None:
        pass

    def disconnect(self) -> None:
        pass
