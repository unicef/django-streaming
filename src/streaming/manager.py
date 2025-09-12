import logging
from typing import TYPE_CHECKING, Any

from django.db.models import Model
from django.db.models.signals import post_save
from django.utils.module_loading import import_string

from streaming.backends import get_backend
from streaming.config import CONFIG

if TYPE_CHECKING:
    from .types import EventType

logger = logging.getLogger(__name__)


class ChangeManager:
    def __init__(self) -> None:
        self._registry: set[type[Model]] = set()
        self.backend = get_backend()

    def register(self, model: type[Model]) -> None:
        self._registry.add(model)

    def _post_save_receiver(self, sender: type[Model], instance: Model, created: bool, **kwargs: Any) -> None:
        payload = {"model": sender.__name__, "pk": instance.pk, "created": created, "fields": {}}
        for field in sender._meta.fields:
            payload["fields"][field.name] = str(getattr(instance, field.name))
        message: EventType = {"event": "post_save", "domain": sender.__name__, "payload": payload}
        self.notify(message)

    def initialize(self) -> None:
        for model in self._registry:
            post_save.connect(self._post_save_receiver, sender=model)

    def notify(self, event: "EventType") -> None:
        self.backend.publish(event)


def get_manager() -> ChangeManager:
    return import_string(CONFIG.MANAGER_CLASS)()  # type: ignore[no-any-return]


def initialize_engine() -> ChangeManager:
    global manager  # noqa: PLW0603
    if manager is None:
        manager = get_manager()
        manager.initialize()
    return manager


manager: "ChangeManager | None" = None
