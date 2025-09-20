import logging
from pathlib import Path

import pytest
import requests
from requests import exceptions

logger = logging.getLogger(__name__)


def pytest_configure(config):
    pass


@pytest.fixture(scope="session")
def docker_compose_command() -> str:
    return "docker compose"


@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    return [
        str(Path(__file__).parent / "docker-compose.yml"),
    ]


@pytest.mark.withoutresponses
def is_rabbit_running(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return True
    except exceptions.ConnectionError:
        return False


@pytest.fixture(scope="session")
def rabbit_server(docker_ip, docker_services):
    """Ensure that HTTP service is up and responsive."""

    port = docker_services.port_for("rabbitmq", 15672)
    url = "http://{}:{}".format(docker_ip, port)
    docker_services.wait_until_responsive(timeout=30.0, pause=0.1, check=lambda: is_rabbit_running(url))
    return docker_ip
