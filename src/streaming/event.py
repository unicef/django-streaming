import json
import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any

from .utils import StreamingJSONEncoder

if TYPE_CHECKING:
    from .types import JSON, EventType


class Event:
    def __init__(
        self,
        *,
        key: str,
        payload: "JSON",
        value_type: "EventType" = "absolute",
        timestamp: datetime | None = None,
        message_id: str | None = None,
    ) -> None:
        self.timestamp = timestamp or datetime.now()
        self.key = key
        self.payload = payload
        self.value_type = value_type
        self.id = message_id or uuid.uuid4()

    def marshall(self) -> bytes:
        return json.dumps(
            {
                "id": self.id,
                "timestamp": self.timestamp.isoformat(),
                "key": self.key,
                "payload": self.payload,
            },
            cls=StreamingJSONEncoder,
        ).encode()

    @classmethod
    def unmarshal(cls, body: bytes) -> "Event":
        data = json.loads(body.decode())
        return cls(
            message_id=data["id"],
            payload=data["payload"],
            key=data["key"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
        )

    @classmethod
    def build(cls, key: str, data: Any, value_type: "EventType") -> "Event":
        if isinstance(data, str):
            payload: JSON = {"message": data}
        else:
            payload = data
        return cls(key=key, payload=payload, value_type=value_type)

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "timestamp": self.timestamp.isoformat(),
            "key": self.key,
            "payload": self.payload,
            "value_type": self.value_type,
        }
