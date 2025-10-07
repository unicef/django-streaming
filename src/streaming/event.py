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
        payload: "JSON",
        value_type: "EventType" = "absolute",
        timestamp: datetime | None = None,
        message_id: str | None = None,
    ) -> None:
        self.timestamp = timestamp or datetime.now()
        self.payload = payload
        self.value_type = value_type
        self.id = message_id or uuid.uuid4()

    def marshall(self) -> bytes:
        return json.dumps(
            {
                "id": self.id,
                "timestamp": self.timestamp.isoformat(),
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
            timestamp=datetime.fromisoformat(data["timestamp"]),
        )

    @classmethod
    def build(
        cls, data: Any, value_type: "EventType" = "absolute", key: str = "N/A", message_id: str | None = None
    ) -> "Event":
        if isinstance(data, str):
            payload: JSON = {"message": data}
        else:
            payload = data
        return cls(payload=payload, value_type=value_type, message_id=message_id)

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "timestamp": self.timestamp.isoformat(),
            "payload": self.payload,
            "value_type": self.value_type,
        }
