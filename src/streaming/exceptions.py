class StreamingError(Exception):
    pass


class StreamingConfigError(StreamingError):
    pass


class StreamingBackendError(StreamingError):
    pass
