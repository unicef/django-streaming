import atexit
import json
import logging
import socket
import time
from typing import TYPE_CHECKING, Any

import pika.channel
import pika.exceptions
from pika.exchange_type import ExchangeType

from streaming.config import CONFIG
from streaming.exceptions import StreamingBackendError

from ._base import BaseBackend

if TYPE_CHECKING:
    from pika.adapters.blocking_connection import BlockingChannel

    from streaming.types import EventType, PikaCallback

logger = logging.getLogger(__name__)


class RabbitMQBackend(BaseBackend):
    def __init__(self, url: str) -> None:
        super().__init__(url)
        self.host = str(self._parsed_url.hostname)
        self.port = int(self._parsed_url.port) if self._parsed_url.port else 5672

        self.exchange = self._options.get("exchange", "django-streaming-broadcast")
        self.connection_name = self._options.get("connection_name", "")
        self.timeout = float(self._options.get("timeout", 0.5))
        self.routing_key = self._options.get("routing_key", "")

        self.connection: pika.BlockingConnection | None = None
        self.channel: BlockingChannel | None = None

        atexit.register(self.close)

    def connect(self) -> None:
        if self.connection:
            self.connection.close()
            self.connection = None
            self.channel = None

        for __ in range(CONFIG.RETRY_COUNT):
            try:
                self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host=self.host,
                        port=self.port,
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
                self.channel = self.connection.channel()
                self.channel.exchange_declare(exchange=self.exchange, exchange_type=ExchangeType.direct, durable=True)

                return
            except (socket.gaierror, pika.exceptions.AMQPConnectionError):
                logger.warning(
                    f"Could not connect to RabbitMQ. Retrying in {CONFIG.RETRY_DELAY} seconds...",
                )
                time.sleep(CONFIG.RETRY_DELAY)
        raise StreamingBackendError("Could not connect to RabbitMQ after multiple retries.")

    def listen(self, domains: list[str], callback: "PikaCallback") -> None:
        def _callback(ch: Any, method: Any, properties: Any, body: bytes) -> None:
            callback(ch, method, properties, body)
            ch.basic_ack(delivery_tag=method.delivery_tag)

        if self.channel is None:
            self.connect()

        if self.channel:
            for domain in domains:
                queue_name = f"{self.connection_name}_sub_to_{domain}"
                self.channel.queue_declare(queue=queue_name, durable=True)
                self.channel.queue_bind(exchange=self.exchange, queue=queue_name, routing_key=domain)
                self.channel.basic_consume(queue=queue_name, on_message_callback=_callback, auto_ack=False)
            self.channel.start_consuming()

    def publish(self, message: "EventType") -> None:
        if not self.channel or self.channel.is_closed:
            self.connect()

        if self.channel:
            try:
                self.channel.basic_publish(
                    exchange=self.exchange,
                    routing_key=message["domain"],
                    body=json.dumps(message).encode(),
                    properties=pika.BasicProperties(
                        delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE,
                        expiration="86400000",  # Scadenza in millisecondi (24 * 60 * 60 * 1000)
                    ),
                )
            except Exception as e:
                raise StreamingBackendError(
                    "RabbitMQ connection not available after reconnect. Message not published."
                ) from e

    def close(self) -> None:
        if self.connection and self.connection.is_open:
            logger.info("Closing RabbitMQ connection.")
            self.connection.close()
