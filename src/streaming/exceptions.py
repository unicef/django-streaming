import logging

logger = logging.getLogger(__name__)


class StreamingError(Exception):
    pass


class StreamingException(Exception):  # noqa N818
    pass


class StreamingConfigError(StreamingError):
    pass


class AuthorizationError(StreamingConfigError):
    pass


class StreamingBackendError(StreamingError):
    pass


class CallbackSkipAck(StreamingException):  # noqa: N818
    """Consume exception but do not acknowledge the message."""


class CallbackRetry(StreamingException):  # noqa: N818
    """Message will be re-queued."""


class CallbackError(StreamingError):  # noqa: N818
    """Message will be discarded."""
