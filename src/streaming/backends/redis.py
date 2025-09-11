import json
import logging
import time
from typing import TYPE_CHECKING

import redis
from redis.exceptions import ConnectionError as RedisConnectionError

from streaming.config import CONFIG
from streaming.exceptions import StreamingBackendError

from ._base import BaseBackend

if TYPE_CHECKING:
    from redis import Redis

    from ..types import JSON

logger = logging.getLogger(__name__)


class Backend(BaseBackend):
    redis_client: "Redis[bytes]"

    def __init__(self, url: str) -> None:
        super().__init__(url)
        self.db = int(self._parsed_url.path.lstrip("/") or 0)
        self.host = str(self._parsed_url.hostname)
        self.port = int(self._parsed_url.port) if self._parsed_url.port else 6379
        self.redis_client = self._get_client()

    def _get_client(self) -> "Redis[bytes]":
        for __ in range(CONFIG.RETRY_COUNT):
            try:
                client = redis.Redis(host=self.host, port=self.port, db=self.db)
                client.ping()
                return client
            except RedisConnectionError:
                logger.warning("Could not connect to Redis. Retrying in %s seconds...", CONFIG.RETRY_DELAY)
                time.sleep(CONFIG.RETRY_DELAY)
        raise StreamingBackendError("Could not connect to Redis after multiple retries.")

    def publish(self, message: "JSON") -> None:
        try:
            self.redis_client.publish(self.queue_name, json.dumps(message).encode())
        except RedisConnectionError:
            self.redis_client = self._get_client()
            self.redis_client.publish(self.queue_name, json.dumps(message).encode())
