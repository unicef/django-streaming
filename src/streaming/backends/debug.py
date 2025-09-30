import logging
from typing import TYPE_CHECKING

from ._base import BaseBackend

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from ..types import EventType, UserCallback


class DebugBackend(BaseBackend):
    def __init__(self, url: str) -> None:
        super().__init__(url)
        self.messages: list[tuple[str, EventType]] = []

    def listen(self, callback: "UserCallback", queues: list[str] | None = None, ack: bool = True) -> None:
        pass

    def connect(self, raise_if_error: bool = False) -> None:
        pass

    def disconnect(self) -> None:
        pass

    def publish(self, routing_key: str, message: "EventType") -> bool:
        self.messages.append((routing_key, message))
        return True
