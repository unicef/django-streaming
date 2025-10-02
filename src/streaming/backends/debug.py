import logging
from typing import TYPE_CHECKING

from ._base import BaseBackend

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from ..event import Event
    from ..types import UserCallback


class DebugBackend(BaseBackend):
    def configure_queue_routing(self) -> dict[str, list[str]]:  # pragma: no cover
        return {}

    def configure_exchanges(self) -> None:  # pragma: no cover
        pass

    def __init__(self, url: str) -> None:
        super().__init__(url)
        self.messages: list[tuple[str, Event]] = []

    def listen(self, callback: "UserCallback", queues: list[str] | None = None, ack: bool = True) -> None:
        pass

    def connect(self, raise_if_error: bool = False) -> None:
        pass

    def disconnect(self) -> None:
        pass

    def publish(self, routing_key: str, message: "Event") -> bool:
        self.messages.append((routing_key, message))
        return True
