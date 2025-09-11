import pytest

from streaming.backends.rabbitmq import Backend as RabbitBackend
from streaming.exceptions import StreamingBackendError


@pytest.fixture
def backend(settings) -> RabbitBackend:
    settings.STREAMING = {"BROKER_URL": "rabbit://localhost:5672?queue=test"}
    from streaming.config import CONFIG

    return RabbitBackend(CONFIG.BROKER_URL)


def test_publish(backend: RabbitBackend) -> None:
    backend.publish({"message": "Hello World"})


def test_error(settings) -> None:
    settings.STREAMING = {"BROKER_URL": "rabbit://localhost:1111?queue=test",
                          "RETRY_COUNT": 1,
                          "RETRY_DELAY": 0.1}
    from streaming.config import CONFIG
    with pytest.raises(StreamingBackendError):
        RabbitBackend(CONFIG.BROKER_URL)
