import atexit
import logging
import queue
import threading
from typing import TYPE_CHECKING, Any

from django.db import models
from django.db.models import Model
from django.db.models.signals import post_save
from django.utils.module_loading import import_string

from streaming.backends import get_backend

from .exceptions import StreamingConfigError
from .utils import make_event

if TYPE_CHECKING:
    from .backends._base import BaseBackend
    from .event import Event

logger = logging.getLogger(__name__)

not_provided = object()

EXCLUDED_FIELDS = [
    models.BinaryField,
    models.FileField,
    models.ImageField,
]


def get_serializable_fields(model: type[Model]) -> list[str]:
    return [f.name for f in model._meta.fields if f.__class__ not in EXCLUDED_FIELDS]


class ChangeManager:
    def __init__(self) -> None:
        self._registry: dict[type[Model], list[str]] = {}
        self._registrations: list[str] = []
        self.backend: BaseBackend = get_backend()

    def register(self, model: type[Model], fields: list[str] | None = None, receiver: Any = None) -> None:
        logger.debug("Registering %s", model)
        dispatch_uid = f"{id(self)}_{model.__name__}"
        if dispatch_uid in self._registrations:
            post_save.disconnect(dispatch_uid=dispatch_uid, sender=model)
        else:
            self._registrations.append(dispatch_uid)
        self._registry[model] = fields or get_serializable_fields(model)
        post_save.connect(receiver or self._post_save_receiver, sender=model, weak=False, dispatch_uid=dispatch_uid)

    def _post_save_receiver(self, sender: type[Model], instance: Model, created: bool, **kwargs: Any) -> None:
        logger.debug("post_save event for %s", sender)
        payload = {"model": sender.__name__, "pk": instance.pk, "created": created, "fields": {}}
        for field_name in self._registry[sender]:
            payload["fields"][field_name] = str(getattr(instance, field_name))
        routing_key = f"{sender._meta.app_label}.{sender._meta.model_name}.save"
        message: Event = make_event(payload)
        self.notify(routing_key, message)

    def notify(self, routing_key: str, event: "Event") -> bool:
        logger.debug("notifying [%s] %s", routing_key, event)
        return self.backend.publish(routing_key, event)


class ThreadedChangeManager(ChangeManager):
    def __init__(self) -> None:
        super().__init__()
        self.queue: queue.Queue[Any] = queue.Queue()
        self.thread: threading.Thread | None = None
        self.shutdown_event = threading.Event()
        atexit.register(self.stop)

    def start(self) -> None:
        if self.thread is None:
            self.thread = threading.Thread(target=self._worker, daemon=True)
            self.thread.start()

    def stop(self) -> None:
        logger.info("Shutting down threaded change manager...")
        self.shutdown_event.set()
        if self.thread and self.thread.is_alive():
            self.queue.join()
            self.thread.join(timeout=5)

    def _worker(self) -> None:
        while not self.shutdown_event.is_set() or not self.queue.empty():
            try:
                routing_key, message = self.queue.get(timeout=1)
                self.backend.publish(routing_key, message)
                self.queue.task_done()
            except queue.Empty:
                continue

    def notify(self, routing_key: str, event: "Event") -> bool:
        self.queue.put((routing_key, event))
        self.start()
        return True


def get_manager() -> ChangeManager:
    from streaming.config import CONFIG

    try:
        return import_string(CONFIG.MANAGER_CLASS)()  # type: ignore[no-any-return]
    except (ImportError, AttributeError):
        raise StreamingConfigError("Invalid manager class. Check your django-streaming configuration.") from None


def initialize_engine(reset: bool = False) -> ChangeManager | ThreadedChangeManager:
    global manager  # noqa: PLW0603
    if manager is None or reset:
        manager = get_manager()
    return manager


manager: "ChangeManager | None" = None
