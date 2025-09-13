from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from streaming.types import JSON, EventType

MINUTE = 60
HOUR = MINUTE * 60
DAY = HOUR * 24


def parse_bool(value: Any) -> bool:
    if isinstance(value, str):
        return value.lower() in ("yes", "true", "1", "y", "t")
    return value in [1, True]


def make_event(message: "str | JSON", *, event: str = "", domain: str = "") -> "EventType":
    if isinstance(message, str):
        payload: JSON = {"message": message}
    else:
        payload = message
    return {"event": event, "domain": domain, "payload": payload}
