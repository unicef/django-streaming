import json
import logging
import socket
import time
from typing import TYPE_CHECKING

import pika.channel
import pika.exceptions

from streaming.config import CONFIG
from streaming.exceptions import StreamingBackendError

from ._base import BaseBackend

if TYPE_CHECKING:
    from pika.adapters.blocking_connection import BlockingChannel

    from streaming.types import JSON

logger = logging.getLogger(__name__)


class Backend(BaseBackend):
    def __init__(self, url: str) -> None:
        super().__init__(url)
        self.channel: BlockingChannel | None = None
        self.host = str(self._parsed_url.hostname) or "----"
        self.port = int(self._parsed_url.port) if self._parsed_url.port else 5672
        self.exchange = self._options.get("exchange", "")
        self.channel = self._get_channel()

    def _get_channel(self) -> "BlockingChannel | None":
        for __ in range(CONFIG.RETRY_COUNT):
            try:
                connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host, port=self.port))
                channel = connection.channel()
                channel.queue_declare(queue=self.queue_name)
                return channel
            except (socket.gaierror, pika.exceptions.AMQPConnectionError):
                logger.warning("Could not connect to RabbitMQ. Retrying in %s seconds...", CONFIG.RETRY_DELAY)
                time.sleep(CONFIG.RETRY_DELAY)
        raise StreamingBackendError("Could not connect to RabbitMQ after multiple retries.")

    def publish(self, message: "JSON") -> None:
        if not self.channel or self.channel.is_closed:
            self.channel = self._get_channel()

        if self.channel:
            try:
                self.channel.basic_publish(
                    exchange=self.exchange,
                    routing_key=self.queue_name,
                    body=json.dumps(message).encode(),
                )
            except Exception as e:
                raise StreamingBackendError(
                    "RabbitMQ connection not available after reconnect. Message not published."
                ) from e
