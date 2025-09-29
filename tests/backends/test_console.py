import logging

import pytest

from streaming.backends.console import ConsoleBackend
from streaming.config import CONFIG
from streaming.utils import make_event

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
        backend.publish("a.b", make_event("test"))
    assert "routing_key:a.b" in f.getvalue()
