import logging
import socket
import time
from collections import defaultdict
from contextlib import suppress
from typing import TYPE_CHECKING

import pika.channel
import pika.exceptions
from pika import PlainCredentials
from pika.exceptions import ConnectionClosedByBroker, ConnectionWrongStateError
from pika.exchange_type import ExchangeType

from streaming.config import CONFIG
from streaming.utils import exchange_exists

from ..event import Event
from ..exceptions import (
    AuthorizationError,
    StreamingCallbackFailure,
    StreamingCallbackRetryError,
    StreamingConfigError,
)
from ._base import BaseBackend

if TYPE_CHECKING:
    from pika.adapters.blocking_connection import BlockingChannel
    from pika.spec import Basic, BasicProperties

    from streaming.types import UserCallback

logger = logging.getLogger(__name__)

MAX_RETRIES = 5


class Callback:
    def __init__(
        self, queue_name: str, backend: "RabbitMQBackend", user_callback: "UserCallback", ack: bool = True
    ) -> None:
        self.backend = backend
        self.user_callback: UserCallback = user_callback
        self.ack = ack
        self.queue_name = queue_name

    def __call__(
        self, ch: "BlockingChannel", method: "Basic.Deliver", properties: "BasicProperties", body: bytes
    ) -> None:
        try:
            self.user_callback(self.queue_name, ch, method, properties, body)
            if self.ack:
                ch.basic_ack(delivery_tag=method.delivery_tag)  # type: ignore[arg-type]
        except StreamingCallbackRetryError as e:
            evt: Event = Event.unmarshal(body)
            logger.debug(f"StreamingCallbackError {evt.id}", exc_info=e)
            retries = int(properties.headers.get("x-retries", 0))  # type: ignore[union-attr]
            self.backend._handle_retry(evt, ch, method, retries)
        except StreamingCallbackFailure as e:
            logger.error(f"Callback failure: {e}", exc_info=e)
        except Exception as e:
            logger.exception(f"Unexpected exception occurred: {e}", exc_info=e)


class RabbitMQBackend(BaseBackend):
    def __init__(self, url: str) -> None:
        super().__init__(url)

    def check_channel(self) -> None:
        if not self.channel:
            raise StreamingConfigError("No active channel")

    def configure_exchanges(self) -> None:
        logger.debug("Configuring exchanges")
        if not self.channel:
            raise StreamingConfigError("No active channel")
        unrouted_exchange = f"{self.exchange}_unrouted"
        dead_letter_queue = f"{self.exchange}_unrouted_queue"
        logger.debug(f"Declaring exchange '{unrouted_exchange}'")
        self.channel.exchange_declare(exchange=unrouted_exchange, exchange_type=ExchangeType.fanout, durable=True)
        self.channel.queue_declare(queue=dead_letter_queue, durable=True)
        self.channel.queue_bind(exchange=unrouted_exchange, queue=dead_letter_queue)

        logger.debug(f"Declaring exchange '{self.exchange}'")
        self.channel.exchange_declare(
            exchange=self.exchange,
            exchange_type=ExchangeType.topic,
            durable=True,
            arguments={"alternate-exchange": unrouted_exchange},
        )
        self.channel.exchange_declare(self.retry_exchange, exchange_type=ExchangeType.direct, durable=True)

    def configure_queue_routing(self) -> dict[str, list[str]]:
        self.check_channel()
        if not exchange_exists(self.channel, self.exchange):
            raise StreamingConfigError("Exchange not found")
        ret = defaultdict(list)
        for alias, config in CONFIG.QUEUES.items():
            real_name = self.get_real_queue_name(alias)
            logger.debug(f"Declaring queue '{real_name}'")
            self.channel.queue_declare(queue=real_name, durable=True, arguments=None)  # type: ignore[union-attr]
            if binding_keys := config.get("routing", []):
                for binding_key in binding_keys:
                    logger.debug("Listening on queue '%s' routed by '%s'", alias, binding_key)
                    self.channel.queue_bind(exchange=self.exchange, queue=real_name, routing_key=binding_key)  # type: ignore[union-attr]
                    ret[alias].append(binding_key)
            else:
                ret[alias].append("warning no routing defined")
        return ret

    def connect(self, raise_if_error: bool = False) -> None:
        logger.debug("Connecting to %s:%s", self.host, self.port)
        if self._connection and self._connection.is_open:
            self.disconnect()

        for __ in range(CONFIG.RETRY_COUNT):
            try:
                auth = PlainCredentials(self._username, self._password)
                self._connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host=self.host,
                        port=self.port,
                        virtual_host=self.virtual_host,
                        credentials=auth,
                        socket_timeout=self.timeout,
                        blocked_connection_timeout=self.timeout,
                        stack_timeout=self.timeout,
                        client_properties={
                            "connection_name": self.client_name,
                            "product": "django-streaming",
                            "information": "",
                            "version": "1.0",
                        },
                    )
                )
                self.channel = self._connection.channel()
                return
            except (
                pika.exceptions.AuthenticationError,
                pika.exceptions.ProbableAuthenticationError,
                pika.exceptions.ProbableAccessDeniedError,
            ) as e:
                raise AuthorizationError(str(e)) from e
            except (socket.gaierror, pika.exceptions.AMQPError) as e:
                logger.warning(
                    f"Could not connect to RabbitMQ. Retrying in {CONFIG.RETRY_DELAY} seconds...",
                )
                time.sleep(CONFIG.RETRY_DELAY)
                if raise_if_error:
                    raise StreamingConfigError(f"Error connecting {self.connection_url}") from e
        logger.critical("Could not connect to RabbitMQ after multiple retries.")

    def disconnect(self) -> None:
        try:
            if self._connection:
                with suppress(ConnectionClosedByBroker, AttributeError, ConnectionWrongStateError):
                    logger.debug("Closing RabbitMQ connection.")
                    self._connection.close()
        finally:
            self._connection = None
            self.channel = None

    # Publisher
    def publish(self, routing_key: str, message: "Event") -> bool:
        try:
            if not self.channel or self.channel.is_closed:
                self.connect(True)
            logger.debug(f"Publishing to exchange '{self.exchange}' using routing key '{routing_key}'")
            self._basic_publish(message, routing_key, 0)
            return True
        except Exception as e:  # noqa: BLE001
            logger.critical("Unhandled error sending to RabbitMQ. Message not published.", exc_info=e)
        return False

    # Listener

    def _basic_publish(
        self, message: "Event", routing_key: str, retry_count: int = 0, exchange: str | None = None
    ) -> None:
        self.check_channel()
        self.channel.basic_publish(  # type: ignore[union-attr]
            exchange=exchange or self.exchange,
            routing_key=routing_key,
            body=message.marshall(),
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE,
                expiration=str(CONFIG.MESSAGE_TTL * 1000),  # milliseconds
                headers={"x-retries": retry_count},
            ),
        )

    def _handle_retry(self, message: "Event", ch: "BlockingChannel", method: "Basic.Deliver", retries: int) -> None:
        ch.basic_ack(method.delivery_tag)  # type: ignore[arg-type]
        if retries < MAX_RETRIES:
            delay = 2000 * (2**retries)  # ms (exponential backoff)
            delay_queue = f"{self.exchange}_retry_{method.routing_key}_{delay}ms"
            logger.debug(
                f"Retrying message {message.id} ({retries + 1}/{MAX_RETRIES}) "
                f"with routing key '{method.routing_key}' in {delay / 1000}s"
            )

            # Declare a delay queue with TTL
            ch.queue_declare(
                delay_queue,
                durable=True,
                arguments={
                    "x-dead-letter-exchange": self.exchange,  # after delay → back to main
                    "x-message-ttl": delay,
                    "x-dead-letter-routing-key": method.routing_key,
                },
            )
            ch.queue_bind(delay_queue, self.retry_exchange, routing_key=delay_queue)
            self._basic_publish(message, routing_key=delay_queue, retry_count=retries + 1, exchange=self.retry_exchange)
        else:
            logger.error(f"Dropping message {message.id} after {MAX_RETRIES} retries")

    def listen(self, callback: "UserCallback", queues: list[str] | None = None, ack: bool = True) -> None:
        if self.channel is None:
            self.connect()
        self.check_channel()

        configured_queues = CONFIG.QUEUES
        if not configured_queues:
            logger.warning("No queues configured in settings.STREAMING['QUEUES']")
            return
        queues_to_listen = queues or configured_queues.keys()
        for queue_name in queues_to_listen:
            real_queue_name = self.get_real_queue_name(queue_name)
            _callback = Callback(queue_name, self, callback, ack=ack)
            self.channel.basic_consume(queue=real_queue_name, on_message_callback=_callback, auto_ack=False)  # type: ignore[union-attr]

        logger.info("Waiting for messages. To exit press CTRL+C")
        self.channel.start_consuming()  # type: ignore[union-attr]
