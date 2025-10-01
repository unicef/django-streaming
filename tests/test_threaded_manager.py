import pytest
from django.contrib.auth.models import User

from streaming.manager import ThreadedChangeManager
from streaming.utils import make_event

pytestmark = pytest.mark.django_db


@pytest.fixture
def manager(settings) -> ThreadedChangeManager:
    from django.core.signals import setting_changed

    from streaming.manager import initialize_engine

    settings.STREAMING = {
        "BROKER_URL": "debug://",
        "MANAGER_CLASS": "streaming.manager.ThreadedChangeManager",
    }
    setting_changed.send(sender=settings._wrapped, setting="STREAMING", value=None, enter=True)
    return initialize_engine(True)


def test_register(manager: ThreadedChangeManager):
    manager.register(User)
    assert len(manager._registry) == 1
    manager.register(User)
    assert len(manager._registry) == 1


def test_notify(manager: ThreadedChangeManager):
    manager.notify(make_event("test"), "a.b")
    manager.stop()
    assert manager.backend.messages


def test_lifecycle(manager: ThreadedChangeManager):
    manager.register(User)
    User.objects.create(username="test")
    assert len(manager.backend.messages) == 1
    msg = manager.backend.messages[-1]
    assert msg[0] == "auth.user.save"
    assert msg[1].key == "auth.user.save"
    assert msg[1].payload["created"]


def test_stop(manager: ThreadedChangeManager):
    manager.register(User)
    manager.notify("a.b", make_event("test"))
    manager.stop()
    assert not manager.thread.is_alive()
