import json
from typing import Any

from django.db.models import Model
from django.db.models.signals import post_save

from streaming.backends import get_backend


class ChangeManager:
    def __init__(self) -> None:
        self._registry: set[type[Model]] = set()
        self.backend = get_backend()

    def register(self, model: type[Model]) -> None:
        self._registry.add(model)

    def _post_save_receiver(self, sender: type[Model], instance: Model, created: bool, **kwargs: Any) -> None:
        # Serialize the model instance
        data = {"model": sender.__name__, "pk": instance.pk, "created": created, "fields": {}}
        for field in sender._meta.fields:
            data["fields"][field.name] = str(getattr(instance, field.name))

        message = json.dumps(data)
        self.backend.publish(message)

    def initialize(self) -> None:
        for model in self._registry:
            post_save.connect(self._post_save_receiver, sender=model)
