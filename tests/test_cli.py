from unittest import mock

import pytest
from click.testing import CliRunner

from streaming.backends.console import ConsoleBackend
from streaming.__cli__ import cli


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def test_cli_command(runner: CliRunner) -> None:
    result = runner.invoke(cli)
    assert result.exit_code == 0


def test_listen_ctrl_c(settings, runner: CliRunner) -> None:
    settings.STREAMING = {"BROKER_URL": "rabbit://localhost:5672?queue=test&exchange=test", "RETRY_DELAY": 0.1}

    with mock.patch("streaming.backends.rabbitmq.RabbitMQBackend.listen") as m:
        m.side_effect = KeyboardInterrupt
        result = runner.invoke(cli, ["listen", "--queue", "test", "a.b"])
    assert result.exit_code == 0
    assert "Stopping listener." in result.output


@pytest.mark.parametrize("cmd", ["send", "listen", "purge"])
def test_rabbit_wrong_backend(settings, runner: CliRunner, cmd) -> None:
    from streaming.manager import initialize_engine

    settings.STREAMING = {"BROKER_URL": "console://"}

    manager = initialize_engine(True)
    backend = manager.backend
    assert isinstance(backend, ConsoleBackend)

    result = runner.invoke(cli, [cmd, "test"])
    assert result.exit_code == 1
    assert "RabbitMQ backend is not configured" in result.output


def test_send_command(settings, runner: CliRunner) -> None:
    settings.STREAMING = {"BROKER_URL": "rabbit://localhost:5672?queue=test&exchange=test", "RETRY_DELAY": 0.1}

    with mock.patch("streaming.backends.rabbitmq.RabbitMQBackend.publish") as mock_publish:
        result = runner.invoke(cli, ["send", "a.b", "--message", "Test Message"])
        assert result.exit_code == 0
        assert "Sent:" in result.output
        mock_publish.assert_called_once()


def test_listen_command(settings, runner: CliRunner) -> None:
    settings.STREAMING = {"BROKER_URL": "rabbit://localhost:5672?queue=test&exchange=test"}
    from streaming.backends.rabbitmq import RabbitMQBackend

    with mock.patch.object(RabbitMQBackend, "listen") as mock_listen:
        result = runner.invoke(cli, ["listen", "--queue", "test_queue", "a.b"])
        assert result.exit_code == 0
        mock_listen.assert_called_once_with(['test_queue'], ['a.b'], mock.ANY)


def test_purge_command(settings, runner: CliRunner) -> None:
    settings.STREAMING = {"BROKER_URL": "rabbit://localhost:5672?queue=test&exchange=test"}
    from streaming.backends.rabbitmq import RabbitMQBackend

    with mock.patch.object(RabbitMQBackend, "connect") as mock_connect:
        with mock.patch.object(RabbitMQBackend, "close") as mock_close:
            with mock.patch.object(RabbitMQBackend, "channel") as mock_channel:
                mock_channel.queue_purge.return_value = mock.Mock(method=mock.Mock(message_count=5))
                result = runner.invoke(cli, ["purge", "test_queue"])
                assert result.exit_code == 0
                assert "Purged 5 messages from queue 'test_queue'" in result.output


def test_check_command(settings, runner: CliRunner) -> None:
    settings.STREAMING = {"BROKER_URL": "rabbit://localhost:5672?queue=test&exchange=test"}
    from streaming.backends.rabbitmq import RabbitMQBackend

    with mock.patch.object(RabbitMQBackend, "connect") as mock_connect:
        with mock.patch.object(RabbitMQBackend, "close") as mock_close:
            result = runner.invoke(cli, ["check"])
            assert result.exit_code == 0
            assert "Streaming Configuration:" in result.output
            assert "BROKER_URL: rabbit://localhost:5672?queue=test&exchange=test" in result.output
            assert "Connection successful." in result.output