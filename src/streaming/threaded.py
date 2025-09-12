import atexit
import logging
import queue
import threading
from typing import Any

from django.db.models import Model

from streaming.manager import ChangeManager as BaseChangeManager

logger = logging.getLogger(__name__)


class ThreadedChangeManager(BaseChangeManager):
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
                message = self.queue.get(timeout=1)
                self.backend.publish(message)
                self.queue.task_done()
            except queue.Empty:
                continue

    def _post_save_receiver(self, sender: type[Model], instance: Model, created: bool, **kwargs: Any) -> None:
        data = {"model": sender.__name__, "pk": instance.pk, "created": created, "fields": {}}
        for field in sender._meta.fields:
            data["fields"][field.name] = str(getattr(instance, field.name))
        self.queue.put(data)
        self.start()

    def notify(self, event: Any) -> None:
        self.queue.put(event)
        self.start()
