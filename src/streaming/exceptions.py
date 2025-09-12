import logging

logger = logging.getLogger(__name__)


class StreamingError(Exception):
    pass


class StreamingConfigError(StreamingError):
    pass


class StreamingBackendError(StreamingError):
    pass
