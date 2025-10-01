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
    manager.notify("a.b", make_event("test"))
    assert manager.backend.messages


def test_lifecycle(manager: ChangeManager):
    manager.register(User)
    User.objects.create(username="test")
    assert len(manager.backend.messages) == 1
    rk, msg = manager.backend.messages[-1]
    assert rk == "auth.user.save"
    assert msg.key == "auth.user.save"
    assert msg.payload["created"]
