import logging

logger = logging.getLogger(__name__)


class StreamingError(Exception):
    pass


class StreamingConfigError(StreamingError):
    pass


class AuthorizationError(StreamingConfigError):
    pass


class StreamingBackendError(StreamingError):
    pass


class StreamingCallbackRetryError(StreamingError):
    """Generic exception raised when a callback fails. Message will be re-queued."""


class StreamingCallbackFailure(StreamingError):  # noqa: N818
    """Generic exception raised when a callback fails. Message will be discarded."""
