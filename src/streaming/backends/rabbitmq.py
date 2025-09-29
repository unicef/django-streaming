import atexit
import json
import logging
import socket
import time
from typing import TYPE_CHECKING

import pika.channel
import pika.exceptions
from pika import PlainCredentials
from pika.exceptions import ConnectionClosedByBroker, ConnectionWrongStateError
from pika.exchange_type import ExchangeType

from streaming.config import CONFIG
from streaming.utils import get_local_ip

from ..exceptions import (
    AuthorizationError,
    StreamingCallbackError,
    StreamingCallbackFailure,
    StreamingConfigError,
    StreamingError,
)
from ..utils import json_dumps
from ._base import BaseBackend

if TYPE_CHECKING:
    from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection
    from pika.spec import Basic, BasicProperties

    from streaming.types import EventType, UserCallback

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
        self._connection: BlockingConnection | None = None
        self._username = self._parsed_url.username or "guest"
        self._password = self._parsed_url.password or "guest"

        self.channel: BlockingChannel | None = None
        self.exchange = self.get_option("exchange", "django-streaming-broadcast")
        self.retry_exchange = f"retry_{self.exchange}"
        self.timeout = float(self.get_option("timeout", 0.5))
        self.virtual_host = self.get_option("vhost", "/")
        # listener
        self.client_name = CONFIG.CLIENT_NAME or get_local_ip()
        self._queue_mapping: dict[str, str] = {}
        atexit.register(self.disconnect)

    @property
    def client_name(self) -> str:
        return self.__client_name

    @client_name.setter
    def client_name(self, name: str) -> None:
        self.__client_name = name

    def initialize(self) -> None:
        pass

    @property
    def queues(self) -> list[str]:
        return list(self._queue_mapping.keys())

    def configure_exchanges(self) -> None:
        if not self.channel:
            raise StreamingConfigError("No active channel")
        unrouted_exchange = f"{self.exchange}_unrouted"
        dead_letter_queue = f"{self.exchange}_unrouted_queue"

        self.channel.exchange_declare(exchange=unrouted_exchange, exchange_type=ExchangeType.fanout, durable=True)
        self.channel.queue_declare(queue=dead_letter_queue, durable=True)
        self.channel.queue_bind(exchange=unrouted_exchange, queue=dead_letter_queue)

        self.channel.exchange_declare(
            exchange=self.exchange,
            exchange_type=ExchangeType.topic,
            durable=True,
            arguments={"alternate-exchange": unrouted_exchange},
        )
        self.channel.exchange_declare(self.retry_exchange, exchange_type=ExchangeType.direct, durable=True)

    def configure_client_queues(self) -> None:
        if not self.channel:
            raise StreamingConfigError("No active channel")
        for alias, config in CONFIG.QUEUES.items():
            real_name = f"{self.client_name}:{config.get('name', alias)}"
            logger.debug(f"Declaring queue '{real_name}'")
            self.channel.queue_declare(queue=real_name, durable=True, arguments=None)
            self._queue_mapping[alias] = real_name

    def get_real_queue_name(self, name: str) -> str:
        try:
            return self._queue_mapping[name]
        except KeyError as e:
            raise StreamingError(f"Unknown queue '{name}'. Valid values are: {self.queues}") from e

    def set_credential(self, username: str, password: str) -> None:
        if self._connection:
            raise StreamingError("Disconnect first.")
        self.__username = username
        self.__password = password

    def _connect(self, raise_if_error: bool = False) -> None:
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

    def connect(self, raise_if_error: bool = False) -> None:
        self._connect(raise_if_error)
        self.configure_client_queues()

    def disconnect(self) -> None:
        try:
            if self._connection:
                logger.debug("Closing RabbitMQ connection.")
                self._connection.close()
        except (ConnectionClosedByBroker, AttributeError, ConnectionWrongStateError):
            pass
        finally:
            self._connection = None
            self.channel = None

    # Publisher
    def publish(self, routing_key: str, message: "EventType") -> bool:
        try:
            if not self.channel:
                self.connect(True)
            logger.debug(f"Publishing to exchange '{self.exchange}' using routing key '{routing_key}'")
            self._basic_publish(message, routing_key, 0)
            return True
        except Exception as e:  # noqa: BLE001
            logger.critical("Unhandled error sending to RabbitMQ. Message not published.", exc_info=e)
        return False

    # Listener

    def _basic_publish(self, message: "EventType", routing_key: str, retry_count: int = 0) -> None:
        if not self.channel:
            raise StreamingConfigError("No active channel")
        self.channel.basic_publish(
            exchange=self.exchange,
            routing_key=routing_key,
            body=json_dumps(message).encode(),
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE,
                expiration=str(CONFIG.MESSAGE_TTL * 1000),  # milliseconds
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
            self._basic_publish(message, routing_key=method.routing_key or "*", retry_count=retries + 1)
        else:
            logger.error(f"Dropping after {MAX_RETRIES} retries")

    def listen(
        self,
        callback: "UserCallback",
        queues: list[str] | None = None,
        ack: bool = True,
    ) -> None:
        if self.channel is None:
            self.connect()
        if not self.channel:
            raise StreamingConfigError("No active channel")
        self.configure_client_queues()
        configured_queues = CONFIG.QUEUES
        if not configured_queues:
            logger.warning("No queues configured in settings.STREAMING['QUEUES']")
            return

        queues_to_listen = queues or configured_queues.keys()
        logger.debug(f"Configured  queues: {', '.join(queues_to_listen)}")
        for queue_name in queues_to_listen:
            if queue_name not in configured_queues:
                logger.error(f"Queue '{queue_name}' not found in configured listening queues. Ignored.")
                continue
            real_queue_name = self.get_real_queue_name(queue_name)
            _callback = Callback(queue_name, self, callback, ack=ack)
            queue_config = configured_queues[queue_name]
            binding_keys = queue_config.get("routing", [])
            for binding_key in binding_keys:
                logger.debug("Listening on queue '%s' routed by '%s'", queue_name, binding_key)
                self.channel.queue_bind(exchange=self.exchange, queue=real_queue_name, routing_key=binding_key)

            self.channel.basic_consume(queue=queue_name, on_message_callback=_callback, auto_ack=False)

        logger.info("Waiting for messages. To exit press CTRL+C")
        self.channel.start_consuming()
