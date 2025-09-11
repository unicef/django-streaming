import sys
from typing import TYPE_CHECKING

from ._base import BaseBackend

if TYPE_CHECKING:
    from ..types import JSON


class Backend(BaseBackend):
    def __init__(self, url: str) -> None:
        super().__init__(url)
        self.stream = self._options.get("streams", "stdout")

    def publish(self, message: "JSON") -> None:
        stream = getattr(sys, self.stream)
        stream.write(f"{message}\n")
