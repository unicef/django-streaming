import logging

import pytest

from streaming.backends.console import ConsoleBackend
from streaming.config import CONFIG

logger = logging.getLogger(__name__)


@pytest.fixture
def backend(settings) -> ConsoleBackend:
    settings.STREAMING = {"BROKER_URL": "console://queue=test"}
    return ConsoleBackend(CONFIG.BROKER_URL)


def test_publish(backend: ConsoleBackend) -> None:
    import io
    from contextlib import redirect_stdout

    f = io.StringIO()
    with redirect_stdout(f):
        backend.publish("test")
    s = f.getvalue()
    assert s == "test\n"
