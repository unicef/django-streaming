import logging
import socket
from unittest import mock
from unittest.mock import MagicMock

import pytest
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import PERSISTENT_DELIVERY_MODE, Basic, BasicProperties

from streaming.backends import get_backend
from streaming.backends.rabbitmq import MAX_RETRIES, Callback, RabbitMQBackend
from streaming.event import Event
from streaming.exceptions import CallbackError, CallbackRetry, CallbackSkipAck, StreamingConfigError
from streaming.utils import make_event

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
    from streaming.manager import initialize_engine  # noqa

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
def test_publish_success(backend: RabbitMQBackend, caplog) -> None:
    backend.connect(True)
    with mock.patch.object(backend, "channel"):
        with caplog.at_level(logging.DEBUG):
            assert backend.publish("a.b", make_event("Hello World"))
            assert "Publishing to exchange" in caplog.text


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
    assert backend.get_real_queue_name("wrong-queue")


@pytest.mark.withoutresponses
def test_configure_queue_routing(backend: RabbitMQBackend, caplog) -> None:
    backend.connect(True)
    backend.configure_queue_routing()
    with mock.patch.object(backend, "channel", None):
        with pytest.raises(StreamingConfigError, match="No active channel"):
            backend.configure_queue_routing()

    with mock.patch("streaming.backends.rabbitmq.exchange_exists") as mocked_exchange_exists:
        mocked_exchange_exists.return_value = False
        with pytest.raises(StreamingConfigError, match="Exchange not found"):
            backend.configure_queue_routing()


def test_publish_no_connection(stream_config, caplog) -> None:
    stream_config.BROKER_URL = "rabbit://localhost:10000"
    backend = get_backend()
    with mock.patch.object(backend, "configure_queue_routing") as mocked_configure_queue_routing:
        mocked_configure_queue_routing.return_value = None
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
    from streaming.config import CONFIG  # noqa

    backend = RabbitMQBackend(CONFIG.BROKER_URL)
    with mock.patch.object(backend, "configure_queue_routing"):
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
        "q1": {"binding_keys": ["#"]},
        "q2": {"binding_keys": ["test.*"]},
    }
    backend: RabbitMQBackend = get_backend()

    with mock.patch.object(backend, "channel", spec=BlockingChannel) as mock_channel:
        backend.listen(callback=MagicMock())
        assert mock_channel.queue_bind.call_count == 0


def test_listen_queues_subset(stream_config, caplog):
    stream_config.QUEUES = {
        "q1": {"binding_keys": ["#"]},
        "q2": {"binding_keys": ["test.*"]},
    }
    backend: RabbitMQBackend = get_backend()

    with mock.patch.object(backend, "channel", spec=BlockingChannel) as mocked_channel:
        mocked_channel.basic_consume.side_effect = None

        backend.configure_queue_routing()
        assert mocked_channel.queue_declare.call_count == 2
        assert mocked_channel.queue_bind.call_count == 2

        backend.listen(callback=MagicMock(), queues=["q1"])


def test_callback(backend) -> None:
    with mock.patch.object(backend, "channel", spec=BlockingChannel):
        user_callback = MagicMock()

        ch = mock.Mock(spec=BlockingChannel)
        ch.basic_ack = MagicMock()

        method = mock.Mock(spec=Basic.Deliver)
        method.delivery_tag = 123  # must exist!

        cb = Callback("q1", backend, user_callback)
        cb(ch, method, mock.Mock(spec=BasicProperties), b"")
        assert user_callback.called
        assert ch.basic_ack.called


def test_callback_error(caplog) -> None:
    backend: RabbitMQBackend = get_backend()

    with mock.patch.object(backend, "channel", spec=BlockingChannel):
        # Mock a user callback that always fails with a retryable error
        user_callback = MagicMock()
        user_callback.side_effect = CallbackRetry

        # Mock the channel and message details
        ch = mock.Mock(spec=BlockingChannel)
        ch.basic_ack = MagicMock()
        method = mock.Mock(spec=Basic.Deliver)
        method.delivery_tag = 123
        method.routing_key = "some.routing.key"

        properties = BasicProperties(headers={"x-retries": 1})

        # Create and execute the callback
        cb = Callback("q1", backend, user_callback)
        cb(
            ch,
            method,
            properties,
            b'{"id":1,"payload":{},"value_type":"absolute","timestamp":"2025-10-02T15:45:30+00:00"}',
        )

        # Assert that the callback was called and the error was logged
        assert user_callback.called
        assert "Retrying message" in caplog.text


def test_callback_retry(caplog) -> None:
    backend: RabbitMQBackend = get_backend()

    with mock.patch.object(backend, "channel", spec=BlockingChannel):
        user_callback = MagicMock()
        user_callback.side_effect = CallbackError
        ch = mock.Mock(spec=BlockingChannel)
        ch.basic_ack = MagicMock()
        method = mock.Mock(spec=Basic.Deliver)
        method.delivery_tag = 123  # must exist!

        properties = BasicProperties(
            delivery_mode=PERSISTENT_DELIVERY_MODE,
            expiration="1000",  # milliseconds
            headers={"x-retries": 1},
        )

        cb = Callback("q1", backend, user_callback)
        cb(ch, method, properties, b"{}")
        assert user_callback.called
        assert not ch.basic_ack.called
        assert "Callback failure" in caplog.text


def test_callback_noack(caplog) -> None:
    backend: RabbitMQBackend = get_backend()

    with mock.patch.object(backend, "channel", spec=BlockingChannel):
        user_callback = MagicMock()
        user_callback.side_effect = CallbackSkipAck
        ch = mock.Mock(spec=BlockingChannel)
        ch.basic_ack = MagicMock()
        method = mock.Mock(spec=Basic.Deliver)
        method.delivery_tag = 123  # must exist!

        properties = BasicProperties(
            delivery_mode=PERSISTENT_DELIVERY_MODE,
            expiration="1000",  # milliseconds
            headers={"x-retries": 1},
        )

        cb = Callback("q1", backend, user_callback)
        cb(ch, method, properties, b"{}")
        assert user_callback.called
        assert not ch.basic_ack.called


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

        cb = Callback("q1", backend, user_callback)
        cb(ch, method, properties, b"{}")
        assert user_callback.called
        assert not ch.basic_ack.called
        assert "Unexpected exception occurred" in caplog.text


def test_backend__handle_retry(configure_server) -> None:
    """
    This test verifies that the `_handle_retry` method correctly publishes a new
    message to the retry infrastructure.
    """
    backend: RabbitMQBackend = get_backend()

    with mock.patch.object(backend, "channel", spec=BlockingChannel):
        backend.connect()
        message = Event(value_type="absolute", payload={})

        # Mock the method that publishes messages
        backend._basic_publish = MagicMock()

        # Mock the channel and message details
        ch = mock.Mock(spec=BlockingChannel)
        ch.basic_ack = MagicMock()
        ch.queue_bind = MagicMock()
        method = mock.Mock(spec=Basic.Deliver)
        method.delivery_tag = 123
        method.routing_key = "123"

        # Call the retry handler
        backend._handle_retry(message, ch, method, 1)

        # Assert that the original message was acknowledged and a new one was published
        assert backend._basic_publish.called
        assert ch.basic_ack.called


def test_backend_max_retry(caplog, configure_server) -> None:
    backend = get_backend()
    backend.connect()
    message = Event(value_type="absolute", payload={})
    backend._basic_publish = MagicMock()
    ch = mock.Mock(spec=BlockingChannel)
    ch.basic_ack = MagicMock()
    ch.queue_bind = MagicMock()
    method = mock.Mock(spec=Basic.Deliver)
    method.delivery_tag = 123  # must exist!

    with caplog.at_level(logging.ERROR):
        backend._handle_retry(message, ch, method, MAX_RETRIES + 1)
        assert "Dropping message" in caplog.text

    assert not backend._basic_publish.called
