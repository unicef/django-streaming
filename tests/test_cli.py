from unittest import mock

import pytest
from click.testing import CliRunner

from streaming.__cli__ import cli
from streaming.backends import get_backend
from streaming.backends.rabbitmq import RabbitMQBackend


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def backend(stream_config):
    stream_config.BROKER_URL = "rabbit://localhost:10000"
    backend = get_backend()
    mocked_assert_backend = mock.patch("streaming.__cli__.assert_backend", spec=RabbitMQBackend)
    mocked_assert_backend.return_value = backend
    mocked_assert_backend.start()
    yield backend
    mocked_assert_backend.stop()


def test_cli_command(runner: CliRunner) -> None:
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0


def test_cli_configure(runner: CliRunner) -> None:
    result = runner.invoke(cli, ["configure"])
    assert result.exit_code == 0

    result = runner.invoke(cli, ["configure", "--client-name", "test"])
    assert result.exit_code == 0


def test_listen_ctrl_c(stream_config, runner: CliRunner) -> None:
    stream_config.BROKER_URL = "rabbit://localhost:10000"
    backend = get_backend()
    with mock.patch("streaming.__cli__.assert_backend") as mocked_assert_backend:
        mocked_assert_backend.return_value = backend
        with mock.patch.object(backend, "channel"):
            with mock.patch.object(backend, "listen") as m1:
                m1.side_effect = KeyboardInterrupt
                result = runner.invoke(cli, ["listen"], catch_exceptions=False)
            assert result.exit_code == 0
            assert "Stopping listener." in result.output


def test_listen_wrong_backend(settings, runner: CliRunner, caplog) -> None:
    settings.STREAMING = {"BROKER_URL": "console://"}
    result = runner.invoke(cli, ["listen"])
    assert result.exit_code == 1
    assert "RabbitMQ backend is not configured" in result.output


def test_purge_wrong_backend(settings, runner: CliRunner) -> None:
    settings.STREAMING = {"BROKER_URL": "console://"}
    result = runner.invoke(cli, ["purge"])
    assert result.exit_code == 1
    assert "RabbitMQ backend is not configured" in result.output


def test_send_wrong_backend(settings, runner: CliRunner) -> None:
    settings.STREAMING = {"BROKER_URL": "console://"}
    result = runner.invoke(cli, ["send", "a.b"])
    assert result.exit_code == 1
    assert "RabbitMQ backend is not configured" in result.output


def test_send_command(settings, runner: CliRunner) -> None:
    settings.STREAMING = {"BROKER_URL": "rabbit://localhost:10000"}

    mock_backend = mock.MagicMock(spec=RabbitMQBackend)

    with mock.patch("streaming.__cli__.assert_backend") as get_backend:
        get_backend.return_value = mock_backend
        # mock_manager = mock.MagicMock()
        # mock_manager.backend = mock_backend
        # mock_initialize_engine.return_value = mock_manager

        result = runner.invoke(cli, ["send", "a.b", "--message", "Test Message"])
        assert result.exit_code == 0
        assert "Sent:" in result.output
        mock_backend.publish.assert_called_once()


def test_listen_command(backend, runner: CliRunner) -> None:
    # settings.STREAMING = {"BROKER_URL": "rabbit://localhost:5672"}
    # from streaming.backends.rabbitmq import RabbitMQBackend
    backend = get_backend()
    with mock.patch("streaming.__cli__.assert_backend") as mocked_assert_backend:
        mocked_assert_backend.return_value = backend
        with mock.patch.object(backend, "listen") as mock_listen:
            result = runner.invoke(cli, ["listen", "--queues", "test_queue"], catch_exceptions=False)
            assert result.exit_code == 0
            mock_listen.assert_called()


# def test_listen_command_no_queues(runner: CliRunner) -> None:
#     with mock.patch.object(RabbitMQBackend, "connect"):
#         with mock.patch("streaming.backends.rabbitmq.RabbitMQBackend.listen"):
#             result = runner.invoke(cli, ["listen"], catch_exceptions=False)
#             assert result.stderr == ""
#             assert result.exit_code == 0
#             mocked.listen.assert_called_once_with(mock.ANY, queues=None)


def test_purge_command(stream_config, runner: CliRunner) -> None:
    stream_config.BROKER_URL = "rabbit://localhost:10000"
    stream_config.QUEUES = {"q1": {}, "q2": {}}

    mock_backend = mock.MagicMock(spec=RabbitMQBackend)
    mock_backend.channel = mock.MagicMock()
    mock_backend.channel.queue_purge.return_value = mock.Mock(method=mock.Mock(message_count=5))

    with mock.patch("streaming.manager.initialize_engine") as mock_initialize_engine:
        mock_manager = mock.MagicMock()
        mock_manager.backend = mock_backend
        mock_initialize_engine.return_value = mock_manager

        result = runner.invoke(cli, ["purge"], catch_exceptions=False)

        assert result.exit_code == 0
        assert "Purged 5 messages from queue 'q1'" in result.output
        assert "Purged 5 messages from queue 'q2'" in result.output
        mock_backend.connect.assert_called_once()
        assert mock_backend.channel.queue_purge.call_count == 2
        mock_backend.disconnect.assert_called_once()


def test_check_command(settings, runner: CliRunner) -> None:
    settings.STREAMING = {"BROKER_URL": "rabbit://localhost:5672?queue=test&exchange=test"}
    from streaming.backends.rabbitmq import RabbitMQBackend

    with mock.patch.object(RabbitMQBackend, "connect"):
        with mock.patch.object(RabbitMQBackend, "disconnect"):
            result = runner.invoke(cli, ["check"])
            assert result.exit_code == 0
            assert "System Configuration:" in result.output
            assert "BROKER_URL: rabbit://localhost:5672?queue=test&exchange=test" in result.output
            assert "Connection successful." in result.output
