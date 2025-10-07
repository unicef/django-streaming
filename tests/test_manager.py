import pytest
from django.contrib.auth.models import User

from streaming.backends.debug import DebugBackend
from streaming.manager import ChangeManager
from streaming.utils import make_event

pytestmark = pytest.mark.django_db


@pytest.fixture
def manager(settings) -> ChangeManager:
    manager = ChangeManager()
    manager.backend = DebugBackend("debug://")
    return manager


def test_register(manager: ChangeManager):
    manager.register(User)
    assert len(manager._registry) == 1
    manager.register(User)
    assert len(manager._registry) == 1


def test_register_with_fields(manager: ChangeManager):
    manager.register(User, ["username"])
    User.objects.create(username="user-1")
    rk, msg = manager.backend.messages[-1]
    assert rk == "auth.user.save"
    assert msg.payload["fields"] == {"username": "user-1"}

    # reregister
    manager.register(User, ["username", "email"])
    User.objects.create(username="user-2")

    assert len(manager.backend.messages) == 2
    rk, msg = manager.backend.messages[-1]
    assert rk == "auth.user.save"
    assert msg.payload["fields"] == {"email": "", "username": "user-2"}


def test_notify(manager: ChangeManager):
    manager.notify("a.b", make_event("test"))
    assert manager.backend.messages


def test_lifecycle(manager: ChangeManager):
    manager.register(User)
    User.objects.create(username="test")
    assert len(manager.backend.messages) == 1
    rk, msg = manager.backend.messages[-1]
    assert rk == "auth.user.save"
    assert msg.payload["created"]
