import logging
import socket
from typing import TYPE_CHECKING
from unittest import mock
from unittest.mock import MagicMock

import pytest
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import PERSISTENT_DELIVERY_MODE, Basic, BasicProperties

from streaming.backends import get_backend
from streaming.backends.rabbitmq import MAX_RETRIES, Callback, RabbitMQBackend
from streaming.exceptions import StreamingCallbackError, StreamingCallbackFailure, StreamingConfigError, StreamingError
from streaming.utils import make_event

if TYPE_CHECKING:
    from streaming.types import EventType

logger = logging.getLogger(__name__)


@pytest.fixture
def backend(stream_config, rabbit_server) -> RabbitMQBackend:
    return get_backend()


@pytest.mark.withoutresponses
def test_init_logic(backend: RabbitMQBackend, caplog) -> None:
    # new instance should not have connection
    assert not backend._connection

    # client_name is available
    assert backend.client_name
    backend.connect(True)
    assert backend._connection


@pytest.mark.withoutresponses
def test_manager(backend: RabbitMQBackend, caplog) -> None:
    from streaming.manager import initialize_engine

    manager = initialize_engine(True)
    assert not manager.backend._connection
    assert manager.backend.client_name
    backend.connect(True)
    assert backend._connection


@pytest.mark.withoutresponses
def test_publish_error(backend: RabbitMQBackend, caplog) -> None:
    backend.connect(True)
    with mock.patch.object(backend.channel, "basic_publish") as m:
        m.side_effect = Exception
        with caplog.at_level(logging.CRITICAL):
            backend.publish("a.b", make_event("Hello World"))
            assert "Unhandled error sending to RabbitMQ. Message not published." in caplog.text


@pytest.mark.withoutresponses
def test_configure_exchanges(backend: RabbitMQBackend, caplog) -> None:
    with pytest.raises(StreamingConfigError):
        backend.configure_exchanges()

    backend.connect(True)
    backend.configure_exchanges()


@pytest.mark.withoutresponses
def test_get_real_queue_name(stream_config) -> None:
    stream_config.QUEUES = {"q1": {}}
    backend: RabbitMQBackend = get_backend()
    with mock.patch.object(backend, "channel"):
        backend.configure_client_queues()
        assert ":q1" in backend.get_real_queue_name("q1")
        with pytest.raises(StreamingError, match="Unknown queue .*"):
            backend.get_real_queue_name("wrong-queue")


@pytest.mark.withoutresponses
def test_configure_client_queues(backend: RabbitMQBackend, caplog) -> None:
    backend.connect(True)
    backend.configure_client_queues()
    with mock.patch.object(backend, "channel", None):
        with pytest.raises(StreamingConfigError, match="No active channel"):
            backend.configure_client_queues()


def test_publish_no_connection(stream_config, caplog) -> None:
    stream_config.BROKER_URL = "rabbit://localhost:10000"
    backend = get_backend()
    with mock.patch.object(backend, "configure_client_queues") as mocked_configure_client_queues:
        mocked_configure_client_queues.return_value = None
        with mock.patch("pika.BlockingConnection") as m:
            m.side_effect = socket.gaierror
            backend.channel = None
            with caplog.at_level(logging.WARNING):
                backend.connect()
                assert "Could not connect to RabbitMQ after multiple retries" in caplog.text
            with caplog.at_level(logging.WARNING):
                backend.publish("a.b", make_event("Hello World"))
                assert "Error connecting rabbit://localhost:10000" in caplog.text


def test_close(caplog) -> None:
    backend = get_backend()
    with mock.patch.object(backend, "_connection"):
        backend.disconnect()
        backend.disconnect()


def test_error(settings, caplog) -> None:
    settings.STREAMING = {
        "BROKER_URL": "rabbit://localhost:1111?queue=test&timeout=0.01",
        "RETRY_COUNT": 1,
        "RETRY_DELAY": 0.1,
    }
    from streaming.config import CONFIG

    backend = RabbitMQBackend(CONFIG.BROKER_URL)
    with mock.patch.object(backend, "configure_client_queues"):
        with caplog.at_level(logging.WARNING):
            backend.connect()
            assert "Could not connect to RabbitMQ after multiple retries." in caplog.text


@pytest.mark.withoutresponses
def test_connect(backend, caplog) -> None:
    backend.connect()
    backend.connect()


@pytest.mark.withoutresponses
def test_listen_no_queues_configured(settings, rabbit_server, backend, caplog):
    settings.STREAMING = {"BROKER_URL": f"rabbit://{rabbit_server}", "QUEUES": {}}
    with mock.patch("pika.adapters.blocking_connection.BlockingChannel.start_consuming"):
        with caplog.at_level(logging.WARNING):
            backend.listen(MagicMock())
            assert "No queues configured" in caplog.text


def test_listen_all_queues(stream_config, caplog):
    stream_config.QUEUES = {
        "q1": {"routing": ["#"]},
        "q2": {"routing": ["test.*"]},
    }
    backend: RabbitMQBackend = get_backend()

    with mock.patch.object(backend, "channel", spec=BlockingChannel) as mock_channel:
        backend.configure_client_queues()

        real_q1_name = backend.get_real_queue_name("q1")
        real_q2_name = backend.get_real_queue_name("q2")

        assert mock_channel.queue_declare.call_count == 2
        mock_channel.queue_declare.assert_any_call(queue=real_q1_name, durable=True, arguments=None)
        mock_channel.queue_declare.assert_any_call(queue=real_q2_name, durable=True, arguments=None)

        backend.listen(callback=MagicMock())
        assert mock_channel.queue_bind.call_count == 2
        mock_channel.queue_bind.assert_any_call(exchange=backend.exchange, queue=real_q1_name, routing_key="#")
        mock_channel.queue_bind.assert_any_call(exchange=backend.exchange, queue=real_q2_name, routing_key="test.*")


def test_listen_queues_subset(stream_config, caplog):
    stream_config.QUEUES = {
        "q1": {"routing": ["#"]},
        "q2": {"routing": ["test.*"]},
    }
    backend: RabbitMQBackend = get_backend()

    with mock.patch.object(backend, "channel", spec=BlockingChannel) as mocked_channel:
        mocked_channel.basic_consume.side_effect = None

        backend.configure_client_queues()
        assert mocked_channel.queue_declare.call_count == 2

        backend.listen(callback=MagicMock(), queues=["q1"])
        assert mocked_channel.queue_bind.call_count == 1
        mocked_channel.queue_bind.assert_called_with(
            exchange=backend.exchange, queue=backend.get_real_queue_name("q1"), routing_key="#"
        )


def test_listen_non_existent_queue(stream_config, caplog):
    stream_config.QUEUES = {
        "q1": {"routing": ["#"]},
    }
    backend: RabbitMQBackend = get_backend()
    with mock.patch("pika.adapters.blocking_connection.BlockingChannel.start_consuming"):
        with caplog.at_level(logging.WARNING):
            backend.listen(MagicMock(), queues=["non-existent"])
            assert "not found in configured listening queues" in caplog.text


@pytest.mark.parametrize("ack", [True, False])
def test_callback(backend, ack) -> None:
    with mock.patch.object(backend, "channel", spec=BlockingChannel):
        user_callback = MagicMock()
        ch = mock.Mock(spec=BlockingChannel)
        ch.basic_ack = MagicMock()
        method = mock.Mock(spec=Basic.Deliver)
        method.delivery_tag = 123  # must exist!

        cb = Callback("q1", backend, user_callback, ack=ack)
        cb(ch, method, mock.Mock(spec=BasicProperties), b"")
        assert user_callback.called
        assert ch.basic_ack.called is ack


def test_callback_error(caplog) -> None:
    backend: RabbitMQBackend = get_backend()

    with mock.patch.object(backend, "channel", spec=BlockingChannel):
        user_callback = MagicMock()
        user_callback.side_effect = StreamingCallbackError
        ch = mock.Mock(spec=BlockingChannel)
        ch.basic_ack = MagicMock()
        method = mock.Mock(spec=Basic.Deliver)
        method.delivery_tag = 123  # must exist!
        method.routing_key = 123  # must exist!

        properties = BasicProperties(
            delivery_mode=PERSISTENT_DELIVERY_MODE,
            expiration="1000",  # milliseconds
            headers={"x-retries": 1},
        )

        cb = Callback("q1", backend, user_callback, ack=True)
        cb(ch, method, properties, b"{}")
        assert user_callback.called
        assert "StreamingCallbackError" in caplog.text


def test_callback_failure(caplog) -> None:
    backend: RabbitMQBackend = get_backend()

    with mock.patch.object(backend, "channel", spec=BlockingChannel):
        user_callback = MagicMock()
        user_callback.side_effect = StreamingCallbackFailure
        ch = mock.Mock(spec=BlockingChannel)
        ch.basic_ack = MagicMock()
        method = mock.Mock(spec=Basic.Deliver)
        method.delivery_tag = 123  # must exist!

        properties = BasicProperties(
            delivery_mode=PERSISTENT_DELIVERY_MODE,
            expiration="1000",  # milliseconds
            headers={"x-retries": 1},
        )

        cb = Callback("q1", backend, user_callback, ack=True)
        cb(ch, method, properties, b"{}")
        assert user_callback.called
        assert not ch.basic_ack.called
        assert "Callback failure" in caplog.text


def test_callback_exception(caplog) -> None:
    backend: RabbitMQBackend = get_backend()

    with mock.patch.object(backend, "channel", spec=BlockingChannel):
        user_callback = MagicMock()
        user_callback.side_effect = Exception
        ch = mock.Mock(spec=BlockingChannel)
        ch.basic_ack = MagicMock()
        method = mock.Mock(spec=Basic.Deliver)
        method.delivery_tag = 123  # must exist!
        mock.Mock(spec=BasicProperties)

        properties = BasicProperties(
            delivery_mode=PERSISTENT_DELIVERY_MODE,
            expiration="1000",  # milliseconds
            headers={"x-retries": 1},
        )

        cb = Callback("q1", backend, user_callback, ack=True)
        cb(ch, method, properties, b"{}")
        assert user_callback.called
        assert not ch.basic_ack.called
        assert "Unexpected exception occurred" in caplog.text


def test_backend__handle_retry() -> None:
    backend: RabbitMQBackend = get_backend()

    with mock.patch.object(backend, "channel", spec=BlockingChannel):
        backend.connect()
        message: "EventType" = {"event": "event", "timestamp": "", "type": "absolute", "payload": {}}
        backend._basic_publish = MagicMock()
        ch = mock.Mock(spec=BlockingChannel)
        ch.basic_ack = MagicMock()
        ch.queue_bind = MagicMock()
        method = mock.Mock(spec=Basic.Deliver)
        method.delivery_tag = 123  # must exist!
        method.routing_key = "123"  # must exist!

        backend._handle_retry(message, ch, method, 1)
        assert backend._basic_publish.called
        assert ch.basic_ack.called


def test_backend__max_retry(backend, caplog) -> None:
    backend.connect()
    message: "EventType" = {"event": "event", "timestamp": "", "type": "absolute", "payload": {}}
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
