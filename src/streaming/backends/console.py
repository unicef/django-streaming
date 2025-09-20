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
        self.stream = self._options.get("streams", "stdout")

    def publish(self, message: "EventType", **kwargs: Any) -> None:
        stream = getattr(sys, self.stream)
        stream.write(f"{message}\n")

    def listen(self, **kwargs: Any) -> None:
        pass
