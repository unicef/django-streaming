import logging
from collections import ChainMap
from typing import Any

from django.conf import settings
from django.core.signals import setting_changed
from django.dispatch import receiver

logger = logging.getLogger(__name__)


class StreamingConfig:
    _DEFAULTS = {
        "BROKER_URL": "console://",
        "QUEUES": {},
        "CLIENT_NAME": "",
        "RETRY_COUNT": 3,
        "RETRY_DELAY": 1,
        "MESSAGE_TTL": 60 * 60 * 24 * 2,  # 2 days
        "MANAGER_CLASS": "streaming.manager.ChangeManager",
    }

    def __init__(self) -> None:
        self._overrides: dict[str, Any]
        self._parsed: ChainMap[str, Any]
        self._cached: dict[str, Any]
        self.load()

    def __getattr__(self, name: str) -> Any:
        if name not in self._parsed:
            raise AttributeError(f" 'StreamingConfig' object has no attribute '{name}'")
        if name not in self._cached:
            self._cached[name] = self._parsed[name]
        return self._cached[name]

    def load(self) -> None:
        self._overrides = getattr(settings, "STREAMING", {})
        self._parsed = ChainMap(self._overrides, self._DEFAULTS)
        self._cached = {}


CONFIG = StreamingConfig()


@receiver(setting_changed)
def reload_config(sender: Any, setting: str, **kwargs: Any) -> None:
    if setting == "STREAMING":
        CONFIG.load()
