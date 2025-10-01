import atexit
import logging
import queue
import threading
from typing import TYPE_CHECKING, Any

from django.db.models import Model
from django.db.models.signals import post_save
from django.utils.module_loading import import_string

from streaming.backends import get_backend

from .utils import make_event

if TYPE_CHECKING:
    from .backends._base import BaseBackend
    from .event import Event

logger = logging.getLogger(__name__)

not_provided = object()


class ChangeManager:
    def __init__(self) -> None:
        self._registry: set[type[Model]] = set()
        self.backend: BaseBackend = get_backend()

    def register(self, model: type[Model], receiver: Any = None) -> None:
        logger.debug("Registering %s", model)
        self._registry.add(model)
        post_save.connect(receiver or self._post_save_receiver, sender=model, weak=False)

    def _post_save_receiver(self, sender: type[Model], instance: Model, created: bool, **kwargs: Any) -> None:
        logger.debug("post_save event for %s", sender)
        payload = {"model": sender.__name__, "pk": instance.pk, "created": created, "fields": {}}
        for field in sender._meta.fields:
            payload["fields"][field.name] = str(getattr(instance, field.name))
        routing_key = f"{sender._meta.app_label}.{sender._meta.model_name}.save"
        message: Event = make_event(payload, key=routing_key)
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

    return import_string(CONFIG.MANAGER_CLASS)()  # type: ignore[no-any-return]


def initialize_engine(reset: bool = False) -> ChangeManager | ThreadedChangeManager:
    global manager  # noqa: PLW0603
    if manager is None or reset:
        manager = get_manager()
    return manager


manager: "ChangeManager | None" = None
