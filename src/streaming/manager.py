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

not_provided = object()


class ChangeManager:
    def __init__(self) -> None:
        self._registry: set[type[Model]] = set()
        self.backend = get_backend()

    def register(self, model: type[Model]) -> None:
        logger.debug("Registering %s", model)
        self._registry.add(model)
        post_save.connect(self._post_save_receiver, sender=model)

    def _post_save_receiver(self, sender: type[Model], instance: Model, created: bool, **kwargs: Any) -> None:
        logger.debug("post_save event for %s", sender)
        payload = {"model": sender.__name__, "pk": instance.pk, "created": created, "fields": {}}
        for field in sender._meta.fields:
            payload["fields"][field.name] = str(getattr(instance, field.name))
        message: EventType = {"event": "post_save", "domain": sender._meta.app_label, "payload": payload}
        self.notify(message)

    def notify(self, event: "EventType") -> None:
        logger.debug("notifying  %s", event)
        self.backend.publish(event)


def get_manager() -> ChangeManager:
    return import_string(CONFIG.MANAGER_CLASS)()  # type: ignore[no-any-return]


def initialize_engine(reset: bool = False) -> ChangeManager:
    global manager  # noqa: PLW0603
    if manager is None or reset:
        manager = get_manager()
    return manager


manager: "ChangeManager | None" = None
