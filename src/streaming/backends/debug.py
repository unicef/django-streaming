import logging
from typing import TYPE_CHECKING

from ._base import BaseBackend

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from ..types import EventType


class DebugBackend(BaseBackend):
    def __init__(self, url: str) -> None:
        super().__init__(url)
        self.messages: list[EventType] = []

    def publish(self, message: "EventType") -> None:
        self.messages.append(message)
