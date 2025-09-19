import logging
from pathlib import Path

from django.apps import AppConfig

logger = logging.getLogger(__name__)


class StreamingConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "streaming"
    path = str(Path(__file__).parent)
