import logging
from unittest import mock

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from streaming.backends.redis import RedisBackend
from streaming.exceptions import StreamingBackendError

logger = logging.getLogger(__name__)


@pytest.fixture
def backend(settings) -> RedisBackend:
    settings.STREAMING = {"BROKER_URL": "redis://localhost:6379/0?queue=test"}
    from streaming.config import CONFIG

    return RedisBackend(CONFIG.BROKER_URL)


def test_publish(backend: RedisBackend) -> None:
    backend.publish({})


def test_error(settings) -> None:
    settings.STREAMING = {
        "BROKER_URL": "redis://localhost:1111?queue=test&timeout=0.01",
        "RETRY_COUNT": 1,
        "RETRY_DELAY": 0.1,
    }
    from streaming.config import CONFIG

    with pytest.raises(StreamingBackendError):
        RedisBackend(CONFIG.BROKER_URL)


def test_connection_error(backend) -> None:
    with mock.patch.object(backend.redis_client, "publish", side_effect=RedisConnectionError):
        backend.publish({})
