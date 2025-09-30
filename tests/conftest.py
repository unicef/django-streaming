import contextlib
import logging
import os
from pathlib import Path

import pytest

logger = logging.getLogger(__name__)

TEST_DIR = Path(__file__).parent

# these two lines must match tox.ini config
VHOST = "pytest"
EXCHANGE = "test_stream"


def pytest_configure():
    pass


def pytest_addoption(parser):
    parser.addoption("--reuse-vhost", action="store_true", default=False, help="Reuse RabbitMQ Virtual Host if exists")
    parser.addoption("--create-vhost", action="store_true", default=False, help="Re/Create RabbitMQ Virtual Host")


@pytest.fixture
def configure_server(request):
    import responses
    from pyrabbit2.api import Client
    from pyrabbit2.http import HTTPError

    responses.stop()
    client = Client("localhost:10001", "guest", "guest")
    if request.config.getoption("--create-vhost"):
        with contextlib.suppress(HTTPError):
            client.delete_vhost(VHOST)
    client.create_vhost(VHOST)
    client.set_vhost_permissions(VHOST, "guest", ".*", ".*", ".*")
    from streaming.backends import get_backend

    backend = get_backend()
    backend.connect()
    backend.configure_exchanges()
    backend.configure_queue_routing()
    yield
    if request.session.testsfailed == 0 and not request.config.getoption("--reuse-vhost"):
        client.delete_vhost(VHOST)


@pytest.fixture
def rabbit_server(configure_server):
    # stream_config.BROKER_URL = "rabbit://localhost:10000/vhost=pytest"
    os.environ["BROKER_URL"] = "rabbit://localhost:10000/vhost=pytest"
    return os.environ.get("RABBIT_SERVER", "localhost:10000")


class ConfigWrapper:
    def __init__(self) -> None:
        from streaming.config import CONFIG

        object.__setattr__(self, "_to_restore", {})
        object.__setattr__(self, "_archived", CONFIG._overrides)
        object.__setattr__(self, "_config", CONFIG)

    def __setattr__(self, attr: str, value) -> None:
        if attr not in self._to_restore:
            self._to_restore[attr] = getattr(self._config, attr)
        self._config._cached[attr] = value

    def finalize(self):
        for k, v in self._to_restore.items():
            self._config._cached[k] = v


@pytest.fixture
def stream_config():
    wrapper = ConfigWrapper()
    yield wrapper
    wrapper.finalize()
