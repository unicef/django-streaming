import logging

import pytest

from streaming.backends.rabbitmq import RabbitMQBackend
from streaming.exceptions import StreamingBackendError
from streaming.utils import make_event

logger = logging.getLogger(__name__)


@pytest.fixture
def backend(settings) -> RabbitMQBackend:
    settings.STREAMING = {"BROKER_URL": "rabbit://localhost:5672?queue=test"}
    from streaming.config import CONFIG

    return RabbitMQBackend(CONFIG.BROKER_URL)


def test_publish(backend: RabbitMQBackend) -> None:
    backend.publish(make_event("Hello World"))


def test_error(settings) -> None:
    settings.STREAMING = {
        "BROKER_URL": "rabbit://localhost:1111?queue=test&timeout=0.01",
        "RETRY_COUNT": 1,
        "RETRY_DELAY": 0.1,
    }
    from streaming.config import CONFIG

    b = RabbitMQBackend(CONFIG.BROKER_URL)

    with pytest.raises(StreamingBackendError):
        b.connect()
