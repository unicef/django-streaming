import atexit
import json
import logging
import socket
import time
from typing import TYPE_CHECKING

import pika.channel
import pika.exceptions
from pika import PlainCredentials
from pika.exchange_type import ExchangeType

from streaming.config import CONFIG

from ..utils import DAY
from ._base import BaseBackend

if TYPE_CHECKING:
    from pika.adapters.blocking_connection import BlockingChannel
    from pika.spec import Basic, BasicProperties

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
        self.virtual_host = self._options.get("virtual_host", "/")

        self.connection: pika.BlockingConnection | None = None
        self.channel: BlockingChannel | None = None

        atexit.register(self.close)

    def connect(self) -> None:
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
                self.channel = self.connection.channel()
                self.channel.exchange_declare(exchange=self.exchange, exchange_type=ExchangeType.topic, durable=True)
                self.channel.queue_declare(queue="global_queue", durable=True)
                self.channel.queue_bind(exchange=self.exchange, queue="global_queue", routing_key="#")
                return
            except (socket.gaierror, pika.exceptions.AMQPConnectionError):
                logger.warning(
                    f"Could not connect to RabbitMQ. Retrying in {CONFIG.RETRY_DELAY} seconds...",
                )
                time.sleep(CONFIG.RETRY_DELAY)
        logger.critical("Could not connect to RabbitMQ after multiple retries.")

    def listen(self, domains: list[str], callback: "PikaCallback") -> None:
        def _callback(
            ch: "BlockingChannel", method: "Basic.Deliver", properties: "BasicProperties", body: bytes
        ) -> None:
            callback(ch, method, properties, body)
            ch.basic_ack(delivery_tag=method.delivery_tag)  # type: ignore[arg-type]

        if self.channel is None:
            self.connect()

        for domain in domains:
            queue_name = f"{self.connection_name.lower()}_sub_to_{domain}"
            self.channel.queue_declare(queue=queue_name, durable=True)  # type: ignore[union-attr]
            self.channel.queue_bind(exchange=self.exchange, queue=queue_name, routing_key=domain)  # type: ignore[union-attr]
            self.channel.basic_consume(queue=queue_name, on_message_callback=_callback, auto_ack=False)  # type: ignore[union-attr]
        self.channel.start_consuming()  # type: ignore[union-attr]

    def publish(self, message: "EventType") -> None:
        if not self.channel or self.channel.is_closed:
            self.connect()
        if self.channel:
            logger.debug("publish to %s %s", self.exchange, message.get("domain", ""))
            try:
                self.channel.basic_publish(
                    exchange=self.exchange,
                    routing_key=message.get("domain", self.routing_key) or self.routing_key,
                    body=json.dumps(message).encode(),
                    properties=pika.BasicProperties(
                        delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE,
                        expiration=str(DAY * 2 * 1000),  # milliseconds
                    ),
                )
            except Exception as e:  # noqa: BLE001
                logger.critical("Unhandled error sending to RabbitMQ. Message not published.", exc_info=e)
        else:
            logger.critical("RabbitMQ connection not available after reconnect. Message not published.")

    def close(self) -> None:
        if self.connection and self.connection.is_open:
            logger.debug("Closing RabbitMQ connection.")
            self.connection.close()
            self.connection = None
            self.channel = None
