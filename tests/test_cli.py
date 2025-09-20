from unittest import mock

import pytest
from click.testing import CliRunner

from streaming.backends.console import ConsoleBackend


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def test_cli_command(runner: CliRunner) -> None:
    from streaming.__cli__ import cli

    result = runner.invoke(cli)  # type: ignore[no-untyped-call]
    assert result.exit_code == 2


def test_ctrl_c(settings, runner: CliRunner) -> None:
    settings.STREAMING = {"BROKER_URL": "rabbit://localhost:5672?queue=test&exchange=test", "RETRY_DELAY": 0.1}
    from streaming.__cli__ import cli

    with mock.patch("streaming.backends.rabbitmq.RabbitMQBackend.listen") as m:
        m.side_effect = KeyboardInterrupt
        result = runner.invoke(cli, ["rabbit", "listen"])  # type: ignore[no-untyped-call]
    assert result.exit_code == 0
    assert "Stopping listener." in result.output


@pytest.mark.parametrize("cmd", ["send", "listen"])
def test_rabbit_wrong_backend(settings, runner: CliRunner, cmd) -> None:
    from streaming.manager import initialize_engine

    settings.STREAMING = {"BROKER_URL": "console://"}
    from streaming.__cli__ import cli

    manager = initialize_engine(True)
    backend = manager.backend
    assert isinstance(backend, ConsoleBackend)

    result = runner.invoke(cli, ["rabbit", cmd])  # type: ignore[no-untyped-call]
    assert result.exit_code == 1
    assert result.output == "Error: RabbitMQ backend is not configured. Please set BROKER_URL to a rabbit:// URL.\n"


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

    with mock.patch.object(RabbitMQBackend, "listen") as mock_listen:
        mock_listen.side_effect = lambda __, cb: cb(None, None, None, b"done")

        result = runner.invoke(cli, ["rabbit", "listen"])  # type: ignore[no-untyped-call]
        assert "Server: localhost:5672" in result.output
        assert "Listen on: " in result.output

        runner.invoke(cli, ["rabbit", "listen", "--name", "test_listener", "--domain", "test_domain"])  # type: ignore[no-untyped-call]
        result = runner.invoke(cli, ["rabbit", "listen", "--name", "test_listener", "--domain", "test_domain"])  # type: ignore[no-untyped-call]
        assert "Server: localhost:5672" in result.output
        assert "Listen on: test test_domain" in result.output

        current_manager = initialize_engine()
        current_manager.backend.close()
        result = runner.invoke(cli, ["rabbit", "listen", "--name", "test_listener2"])  # type: ignore[no-untyped-call]
        assert "Server: localhost:5672" in result.output
