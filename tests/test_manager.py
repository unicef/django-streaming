from unittest import mock

import pytest
from django.contrib.auth.models import User

from streaming.manager import ChangeManager
from streaming.utils import make_event

pytestmark = pytest.mark.django_db


@pytest.fixture
def manager(settings) -> ChangeManager:
    from streaming.manager import initialize_engine

    settings.STREAMING = {"BROKER_URL": "debug://queue=test"}
    return initialize_engine(True)


def test_register(manager: ChangeManager):
    manager.register(User)
    assert len(manager._registry) == 1
    manager.register(User)
    assert len(manager._registry) == 1


def test_notify(manager: ChangeManager):
    manager.notify(make_event("test"))
    assert manager.backend.messages


def test_lifecycle(manager: ChangeManager):
    manager.register(User)
    with mock.patch.object(manager, "notify") as m:
        User.objects.create(username="test")
        assert m.called
