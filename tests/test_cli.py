from unittest import mock

import pytest
from click.testing import CliRunner


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def test_hello_command(runner: CliRunner) -> None:
    from streaming.__cli__ import cli

    result = runner.invoke(cli)  # type: ignore[no-untyped-call]
    assert result.exit_code == 2


def test_rabbit_send_command(settings, runner: CliRunner) -> None:
    settings.STREAMING = {"BROKER_URL": "rabbit://localhost:5672?queue=test&exchange=test", "RETRY_DELAY": 0.1}
    from streaming.__cli__ import cli

    result = runner.invoke(cli, ["rabbit", "send", "--message", "Test Message", "--domain", "test_domain"])  # type: ignore[no-untyped-call]
    assert "Server: localhost:5672" in result.output
    assert "Sent:" in result.output


def test_rabbit_listen_command(settings, runner: CliRunner) -> None:
    settings.STREAMING = {"BROKER_URL": "rabbit://localhost:5672?queue=test&exchange=test"}
    from streaming.__cli__ import cli
    from streaming.backends.rabbitmq import RabbitMQBackend
    from streaming.manager import initialize_engine

    with mock.patch.object(RabbitMQBackend, "listen"):
        result = runner.invoke(cli, ["rabbit", "listen"])  # type: ignore[no-untyped-call]
        assert "Server: localhost:5672" in result.output
        assert "Listener: " in result.output
        assert "Domains: " in result.output

        runner.invoke(cli, ["rabbit", "listen", "--name", "test_listener", "--domain", "test_domain"])  # type: ignore[no-untyped-call]
        result = runner.invoke(cli, ["rabbit", "listen", "--name", "test_listener", "--domain", "test_domain"])  # type: ignore[no-untyped-call]
        assert "Server: localhost:5672" in result.output
        assert "Listener: test_listener" in result.output
        assert "Domains: test_domain" in result.output

        current_manager = initialize_engine()
        current_manager.backend.close()
        result = runner.invoke(cli, ["rabbit", "listen", "--name", "test_listener2"])  # type: ignore[no-untyped-call]
        assert "Server: localhost:5672" in result.output
