import logging

import pytest

from streaming.config import StreamingConfig

logger = logging.getLogger(__name__)


def test_issues_config_overrides(settings):
    settings.ISSUES = {
        "BROKER_URL": "console://",
    }
    config = StreamingConfig()
    assert config.BROKER_URL


def test_issues_config_getattr():
    config = StreamingConfig()
    with pytest.raises(AttributeError):
        assert config.NON_EXISTENT_ATTRIBUTE
