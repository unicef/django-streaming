import logging

from django.apps import AppConfig

from streaming.manager import initialize_engine

logger = logging.getLogger(__name__)


class DemoConfig(AppConfig):
    name = "demo"

    def ready(self):
        from django.contrib.auth.models import User

        manager = initialize_engine()
        manager.register(User)
