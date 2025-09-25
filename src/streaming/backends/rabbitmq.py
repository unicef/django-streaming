import atexit
import json
import logging
import socket
import time
from typing import TYPE_CHECKING, Any

import pika.channel
import pika.exceptions
from pika import PlainCredentials
from pika.exceptions import ConnectionClosedByBroker, ConnectionWrongStateError
from pika.exchange_type import ExchangeType

from streaming.config import CONFIG

from ..exceptions import StreamingCallbackError, StreamingCallbackFailure, StreamingConfigError
from ..utils import DAY, get_local_ip
from ._base import BaseBackend

if TYPE_CHECKING:
    from pika.adapters.blocking_connection import BlockingChannel
    from pika.spec import Basic, BasicProperties

    from streaming.types import EventType, PikaCallback

logger = logging.getLogger(__name__)

MAX_RETRIES = 5


class Callback:
    def __init__(self, backend: "RabbitMQBackend", user_callback: "PikaCallback", ack: bool = True) -> None:
        self.backend = backend
        self.user_callback = user_callback
        self.ack = ack

    def __call__(
        self, ch: "BlockingChannel", method: "Basic.Deliver", properties: "BasicProperties", body: bytes
    ) -> None:
        try:
            self.user_callback(ch, method, properties, body)
            if self.ack:
                ch.basic_ack(delivery_tag=method.delivery_tag)  # type: ignore[arg-type]
        except StreamingCallbackError as e:
            evt: EventType = json.loads(body.decode())
            retries = int(properties.headers.get("x-retries", 0))  # type: ignore[union-attr]
            ch.basic_ack(method.delivery_tag)  # type: ignore[arg-type]
            self.backend._handle_retry(evt, ch, method, retries)
            logger.debug("StreamingCallbackError", exc_info=e)
        except StreamingCallbackFailure as e:
            logger.error(f"Callback failure: {e}", exc_info=e)
        except Exception as e:
            logger.exception(f"Unexpected exception occurred: {e}", exc_info=e)


class RabbitMQBackend(BaseBackend):
    def __init__(self, url: str) -> None:
        super().__init__(url)
        self.host = str(self._parsed_url.hostname)
        self.port = int(self._parsed_url.port) if self._parsed_url.port else 5672

        self.exchange = self._options.get("exchange", "django-streaming-broadcast")
        self.retry_exchange = f"retry_{self.exchange}"

        self.connection_name = self._options.get("connection_name", get_local_ip())
        self.timeout = float(self._options.get("timeout", 0.5))
        self.routing_key = self._options.get("routing_key", "")
        self.virtual_host = self._options.get("virtual_host", "/")

        self.connection: pika.BlockingConnection | None = None
        self.channel: BlockingChannel | None = None

        atexit.register(self.close)

    def _configure(self) -> None:
        if not self.connection:
            raise StreamingConfigError("No active connection")
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.exchange, exchange_type=ExchangeType.topic, durable=True)
        self.channel.exchange_declare(self.retry_exchange, exchange_type=ExchangeType.direct, durable=True)

        self.channel.queue_declare(
            queue=f"{self.exchange}_global_queue",
            durable=True,
            arguments={
                "x-dead-letter-exchange": self.retry_exchange  # failed → retry
            },
        )
        self.channel.queue_bind(exchange=self.exchange, queue=f"{self.exchange}_global_queue", routing_key="#")

    def _connect(self, raise_if_error: bool = False) -> None:
        logger.debug("Connecting to %s:%s", self.host, self.port)
        if self.connection and self.connection.is_open:
            self.close()

        for __ in range(CONFIG.RETRY_COUNT):
            try:
                if self._parsed_url.username:
                    auth = PlainCredentials(self._parsed_url.username, self._parsed_url.password or "")
                else:
                    auth = PlainCredentials("guest", "guest")
                self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host=self.host,
                        port=self.port,
                        virtual_host=self.virtual_host,
                        credentials=auth,
                        socket_timeout=self.timeout,
                        blocked_connection_timeout=self.timeout,
                        stack_timeout=self.timeout,
                        client_properties={
                            "connection_name": self.connection_name,
                            "product": "django-streaming",
                            "information": "",
                            "version": "1.0",
                        },
                    )
                )
            except (socket.gaierror, pika.exceptions.AMQPConnectionError) as e:
                logger.warning(
                    f"Could not connect to RabbitMQ. Retrying in {CONFIG.RETRY_DELAY} seconds...",
                )
                time.sleep(CONFIG.RETRY_DELAY)
                if raise_if_error:
                    raise StreamingConfigError(f"Error connecting {self.connection_url}") from e
        logger.critical("Could not connect to RabbitMQ after multiple retries.")

    def connect(self, raise_if_error: bool = False) -> None:
        self._connect(raise_if_error)
        if self.connection:
            self._configure()
        elif raise_if_error:
            raise StreamingConfigError("No active connection")

    def _basic_publish(self, message: "EventType", retry_count: int = 0) -> None:
        if not self.channel:
            raise StreamingConfigError("No active channel")
        self.channel.basic_publish(
            exchange=self.exchange,
            routing_key=message.get("domain", self.routing_key) or self.routing_key,
            body=json.dumps(message).encode(),
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE,
                expiration=str(DAY * 2 * 1000),  # milliseconds
                headers={"x-retries": retry_count},
            ),
        )

    def _handle_retry(self, message: "EventType", ch: "BlockingChannel", method: "Basic.Deliver", retries: int) -> None:
        ch.basic_ack(method.delivery_tag)  # type: ignore[arg-type]
        if retries < MAX_RETRIES:
            delay = 2000 * (2**retries)  # ms (exponential backoff)
            delay_queue = f"{self.exchange}_retry_{delay}ms"

            # Declare a delay queue with TTL
            ch.queue_declare(
                delay_queue,
                durable=True,
                arguments={
                    "x-dead-letter-exchange": self.exchange,  # after delay → back to main
                    "x-message-ttl": delay,
                },
            )
            ch.queue_bind(delay_queue, self.retry_exchange, routing_key="task")
            self._basic_publish(message, retries + 1)
        else:
            logger.error(f"Dropping after {MAX_RETRIES} retries")

    def listen(self, domains: list[str], callback: "PikaCallback", ack: bool = True) -> None:
        if self.channel is None:
            self.connect()
        _callback = Callback(self, callback, ack=ack)
        for domain in domains:
            queue_name = f"{self.connection_name.lower()}_sub_to_{domain}"
            self.channel.queue_declare(queue=queue_name, durable=True)  # type: ignore[union-attr]
            self.channel.queue_bind(exchange=self.exchange, queue=queue_name, routing_key=domain)  # type: ignore[union-attr]
            self.channel.basic_consume(queue=queue_name, on_message_callback=_callback, auto_ack=False)  # type: ignore[union-attr]
        self.channel.start_consuming()  # type: ignore[union-attr]

    def publish(self, message: "EventType", retry_count: int = 0, **kwargs: Any) -> None:
        if not self.channel or self.channel.is_closed:
            self.connect()
        if self.channel:
            logger.debug("publish to %s %s", self.exchange, message.get("domain", ""))
            try:
                self._basic_publish(message, 0)
            except Exception as e:  # noqa: BLE001
                logger.critical("Unhandled error sending to RabbitMQ. Message not published.", exc_info=e)
        else:
            logger.critical("RabbitMQ connection not available after reconnect. Message not published.")

    def close(self) -> None:
        logger.debug("Closing RabbitMQ connection.")
        try:
            self.connection.close()  # type: ignore[union-attr]
        except (ConnectionClosedByBroker, AttributeError, ConnectionWrongStateError):
            pass
        finally:
            self.connection = None
            self.channel = None
