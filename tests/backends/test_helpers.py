import logging

import pytest

from streaming.backends import get_backend
from streaming.exceptions import StreamingConfigError

logger = logging.getLogger(__name__)


@pytest.mark.parametrize("broker", ["console://", "rabbit://localhost:5672", "debug://"])
def test_get_backend(stream_config, broker):
    stream_config.BROKER_URL = broker
    assert get_backend()


def test_error(stream_config):
    stream_config.BROKER_URL = "mysql://"
    with pytest.raises(StreamingConfigError, match=r"Broker not supported: .*"):
        assert get_backend()
    stream_config.BROKER_URL = ""
    with pytest.raises(StreamingConfigError, match=r"Empty BROKER_URL"):
        assert get_backend()
