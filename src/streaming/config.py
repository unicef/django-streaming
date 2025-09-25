import logging
from collections import ChainMap
from typing import Any

from django.conf import settings
from django.core.signals import setting_changed
from django.dispatch import receiver

logger = logging.getLogger(__name__)

DEFAULT_QUEUE_NAME = "django_model_changes"


class StreamingConfig:
    _DEFAULTS = {
        "BROKER_URL": "console://",
        "RETRY_COUNT": 3,
        "RETRY_DELAY": 1,
        "MANAGER_CLASS": "streaming.manager.ChangeManager",
    }

    def __init__(self) -> None:
        self._overrides = getattr(settings, "STREAMING", {})
        self._parsed = ChainMap(self._overrides, self._DEFAULTS)
        self._cached: dict[str, Any] = {}

    def __getattr__(self, name: str) -> Any:
        if name not in self._parsed:
            raise AttributeError(f" 'StreamingConfig' object has no attribute '{name}'")
        if name not in self._cached:
            self._cached[name] = self._parsed[name]
        return self._cached[name]


CONFIG = StreamingConfig()


@receiver(setting_changed)
def reload_config(sender: Any, setting: str, **kwargs: Any) -> None:
    global CONFIG  # noqa: PLW0603
    if setting == "STREAMING":
        CONFIG = StreamingConfig()
