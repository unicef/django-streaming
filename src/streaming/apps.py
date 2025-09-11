from pathlib import Path

from django.apps import AppConfig


class StreamingConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "streaming"
    path = str(Path(__file__).parent)

    def ready(self) -> None:
        from streaming.backends import initialize_engine

        initialize_engine()
