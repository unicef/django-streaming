import logging

import pytest

from streaming.backends import get_backend
from streaming.exceptions import StreamingConfigError

logger = logging.getLogger(__name__)


@pytest.fixture(params=["console://", "rabbit://localhost:5672"])
def url(request):
    return request.param


@pytest.fixture
def config(settings, url) -> None:
    settings.STREAMING = {
        "BROKER_URL": url,
    }


def test_get_backend(config: str):
    assert get_backend()


def test_error(settings):
    settings.STREAMING = {
        "BROKER_URL": "mysql://",
    }
    with pytest.raises(StreamingConfigError, match=r"Broker not supported: .*"):
        assert get_backend()
