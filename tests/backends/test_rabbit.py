import logging
from typing import TYPE_CHECKING
from unittest import mock
from unittest.mock import MagicMock

import pika
import pytest
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties

from streaming.backends.rabbitmq import MAX_RETRIES, Callback, RabbitMQBackend
from streaming.exceptions import StreamingCallbackError, StreamingCallbackFailure
from streaming.utils import make_event

if TYPE_CHECKING:
    from streaming.types import EventType

logger = logging.getLogger(__name__)


@pytest.fixture
def backend(settings) -> RabbitMQBackend:
    settings.STREAMING = {"BROKER_URL": "rabbit://localhost:5672?queue=test"}
    from streaming.config import CONFIG

    return RabbitMQBackend(CONFIG.BROKER_URL)


def test_publish_error(backend: RabbitMQBackend, caplog) -> None:
    backend.connect()
    with mock.patch.object(backend.channel, "basic_publish") as m:
        m.side_effect = Exception
        with caplog.at_level(logging.CRITICAL):
            backend.publish(make_event("Hello World"))
            assert "Unhandled error sending to RabbitMQ. Message not published." in caplog.text


def test_publish_no_connection(backend: RabbitMQBackend, caplog) -> None:
    with mock.patch.object(backend, "connect") as m:
        m.return_value = None
        backend.channel = None
        with caplog.at_level(logging.WARNING):
            backend.publish(make_event("Hello World"))
            assert "RabbitMQ connection not available after reconnect" in caplog.text


def test_close(backend: RabbitMQBackend) -> None:
    backend.connect()
    backend.close()
    backend.close()


def test_error(settings, caplog) -> None:
    settings.STREAMING = {
        "BROKER_URL": "rabbit://localhost:1111?queue=test&timeout=0.01",
        "RETRY_COUNT": 1,
        "RETRY_DELAY": 0.1,
    }
    from streaming.config import CONFIG

    b = RabbitMQBackend(CONFIG.BROKER_URL)

    with caplog.at_level(logging.WARNING):
        b.connect()
        assert "Could not connect to RabbitMQ after multiple retries." in caplog.text


def test_connect(backend) -> None:
    backend.connect()
    backend.connect()


def test_listen(backend) -> None:
    with mock.patch("pika.adapters.blocking_connection.BlockingChannel.start_consuming"):
        backend.listen(["abc"], MagicMock)

    backend.connect()
    assert backend.channel
    with mock.patch("pika.adapters.blocking_connection.BlockingChannel.start_consuming"):
        backend.listen(["abc"], MagicMock)


@pytest.mark.parametrize("ack", [True, False])
def test_callback(backend, ack) -> None:
    user_callback = MagicMock()
    ch = mock.Mock(spec=BlockingChannel)
    ch.basic_ack = MagicMock()
    method = mock.Mock(spec=Basic.Deliver)
    method.delivery_tag = 123  # must exist!

    cb = Callback(backend, user_callback, ack=ack)
    cb(ch, method, mock.Mock(spec=BasicProperties), b"")
    assert user_callback.called
    assert ch.basic_ack.called is ack


def test_callback_error(backend) -> None:
    backend.connect()
    backend._handle_retry = MagicMock()
    user_callback = MagicMock()
    user_callback.side_effect = StreamingCallbackError
    ch = mock.Mock(spec=BlockingChannel)
    ch.basic_ack = MagicMock()
    method = mock.Mock(spec=Basic.Deliver)
    method.delivery_tag = 123  # must exist!
    mock.Mock(spec=BasicProperties)

    properties = pika.BasicProperties(
        delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE,
        expiration="1000",  # milliseconds
        headers={"x-retries": 1},
    )

    cb = Callback(backend, user_callback, ack=True)
    cb(ch, method, properties, b"{}")
    assert user_callback.called


def test_callback_failure(backend) -> None:
    backend.connect()
    backend._handle_retry = MagicMock()
    user_callback = MagicMock()
    user_callback.side_effect = StreamingCallbackFailure
    ch = mock.Mock(spec=BlockingChannel)
    ch.basic_ack = MagicMock()
    method = mock.Mock(spec=Basic.Deliver)
    method.delivery_tag = 123  # must exist!
    mock.Mock(spec=BasicProperties)

    properties = pika.BasicProperties(
        delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE,
        expiration="1000",  # milliseconds
        headers={"x-retries": 1},
    )

    cb = Callback(backend, user_callback, ack=True)
    cb(ch, method, properties, b"{}")
    assert user_callback.called
    assert not ch.basic_ack.called


def test_callback_exception(backend) -> None:
    backend.connect()
    backend._handle_retry = MagicMock()
    user_callback = MagicMock()
    user_callback.side_effect = Exception
    ch = mock.Mock(spec=BlockingChannel)
    ch.basic_ack = MagicMock()
    method = mock.Mock(spec=Basic.Deliver)
    method.delivery_tag = 123  # must exist!
    mock.Mock(spec=BasicProperties)

    properties = pika.BasicProperties(
        delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE,
        expiration="1000",  # milliseconds
        headers={"x-retries": 1},
    )

    cb = Callback(backend, user_callback, ack=True)
    cb(ch, method, properties, b"{}")
    assert user_callback.called
    assert not ch.basic_ack.called


def test_backend__handle_retry(backend) -> None:
    backend.connect()
    message: "EventType" = {"event": "event", "domain": "domain", "payload": {}}
    backend._basic_publish = MagicMock()
    ch = mock.Mock(spec=BlockingChannel)
    ch.basic_ack = MagicMock()
    ch.queue_bind = MagicMock()
    method = mock.Mock(spec=Basic.Deliver)
    method.delivery_tag = 123  # must exist!

    backend._handle_retry(message, ch, method, 1)
    assert backend._basic_publish.called
    assert ch.basic_ack.called


def test_backend__max_retry(backend, caplog) -> None:
    backend.connect()
    message: "EventType" = {"event": "event", "domain": "domain", "payload": {}}
    backend._basic_publish = MagicMock()
    ch = mock.Mock(spec=BlockingChannel)
    ch.basic_ack = MagicMock()
    ch.queue_bind = MagicMock()
    method = mock.Mock(spec=Basic.Deliver)
    method.delivery_tag = 123  # must exist!

    with caplog.at_level(logging.ERROR):
        backend._handle_retry(message, ch, method, MAX_RETRIES + 1)
        assert f"Dropping after {MAX_RETRIES} retries" in caplog.text

    assert not backend._basic_publish.called
