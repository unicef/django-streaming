from typing import TYPE_CHECKING

from ._base import BaseBackend

if TYPE_CHECKING:
    from ..types import JSON


class Backend(BaseBackend):
    def __init__(self, url: str) -> None:
        super().__init__(url)
        self.messages: list[JSON] = []

    def publish(self, message: "JSON") -> None:
        self.messages.append(message)
