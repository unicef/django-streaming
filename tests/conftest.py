import logging
import os

import pytest

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def rabbit_server():
    return os.environ.get("RABBIT_SERVER", "localhost:10000")
