import json
from datetime import datetime
from typing import TYPE_CHECKING, Any

from .utils import StreamingJSONEncoder

if TYPE_CHECKING:
    from .types import JSON, EventType

"""
# class EventType(TypedDict):
#     timestamp: str | datetime
#     event: str
#     payload: JSON
#     type: Literal["absolute", "delta", "event"]
"""


class Event:
    def __init__(
        self, *, key: str, payload: "JSON", value_type: "EventType" = "absolute", timestamp: datetime | None = None
    ) -> None:
        self.timestamp = timestamp or datetime.now()
        self.key = key
        self.payload = payload
        self.value_type = value_type

    def marshall(self) -> bytes:
        return json.dumps(
            {
                "timestamp": self.timestamp.isoformat(),
                "key": self.key,
                "payload": self.payload,
            },
            cls=StreamingJSONEncoder,
        ).encode()

    @classmethod
    def unmarshal(cls, body: bytes) -> "Event":
        return cls(**json.loads(body.decode()))

    @classmethod
    def build(cls, key: str, data: Any, value_type: "EventType") -> "Event":
        if isinstance(data, str):
            payload: JSON = {"message": data}
        else:
            payload = data
        return cls(key=key, payload=payload, value_type=value_type)

    def as_dict(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp.isoformat(),
            "key": self.key,
            "payload": self.payload,
            "value_type": self.value_type,
        }
