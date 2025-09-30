from unittest import mock
from unittest.mock import MagicMock

import pytest
from click import ClickException
from click.testing import CliRunner
from django.core.exceptions import ImproperlyConfigured

from streaming.__cli__ import assert_backend, cli
from streaming.backends import get_backend
from streaming.backends.rabbitmq import RabbitMQBackend
from streaming.exceptions import AuthorizationError, StreamingConfigError
from streaming.utils import json_dumps, make_event


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


@pytest.mark.parametrize("exc", [ModuleNotFoundError, ImproperlyConfigured])
def test_django_invalid_setup(exc, runner: CliRunner) -> None:
    with mock.patch("django.setup") as m:
        m.side_effect = exc
        result = runner.invoke(cli, ["check"])
    assert "Error: Unable to setup Django." in result.stderr
    assert result.exit_code == 1


def test_assert_backend(stream_config) -> None:
    stream_config.BROKER_URL = "rabbit://localhost:10000"
    assert assert_backend()

    stream_config.BROKER_URL = "console://"
    with pytest.raises(ClickException, match="RabbitMQ backend is not configured"):
        assert_backend()


def test_cli_command(runner: CliRunner) -> None:
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0


def test_cli_configure(stream_config, runner: CliRunner, configure_server) -> None:
    stream_config.BROKER_URL = "rabbit://localhost:10000?vhost=pytest&exchange=stream"
    backend = get_backend()
    with mock.patch("streaming.__cli__.assert_backend") as mocked_assert_backend:
        mocked_assert_backend.return_value = backend
        result = runner.invoke(cli, ["configure"], catch_exceptions=False)
        assert result.stderr == ""
        assert result.exit_code == 0

        result = runner.invoke(cli, ["configure", "--client-name", "test"], catch_exceptions=False)
        assert result.exit_code == 0
        with mock.patch.object(backend, "connect") as mocked_connect:
            mocked_connect.side_effect = AuthorizationError
            result = runner.invoke(cli, ["configure", "--client-name", "test"], catch_exceptions=False)
            assert "Unable to connect using rabbit://localhost:10000" in result.stderr
            assert result.stdout == ""
            assert result.exit_code == 1
        with mock.patch.object(backend, "connect") as mocked_connect:
            mocked_connect.side_effect = StreamingConfigError
            result = runner.invoke(cli, ["configure", "--client-name", "test"], catch_exceptions=False)
            assert "Generic error" in result.stderr
            assert result.stdout == ""
            assert result.exit_code == 1


def test_listen_ctrl_c(stream_config, runner: CliRunner, configure_server) -> None:
    # stream_config.BROKER_URL = "rabbit://localhost:10000"
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


@pytest.mark.parametrize(
    "args",
    [
        (),
        ("--client-name", "name1"),
        ("--payload",),
        ("--pretty",),
        ("--payload", "--pretty"),
    ],
)
def test_listen_callback(stream_config, runner: CliRunner, caplog, args) -> None:
    stream_config.BROKER_URL = "rabbit://localhost:10000"
    backend = get_backend()
    evt = make_event("")
    with mock.patch("streaming.__cli__.assert_backend") as mocked_assert_backend:
        mocked_assert_backend.return_value = backend
        with mock.patch.object(backend, "listen") as mocked_listen:
            mocked_listen.side_effect = lambda cb, queues: cb(
                "queue_name", MagicMock(), MagicMock(), MagicMock(), json_dumps(evt).encode()
            )
            result = runner.invoke(cli, ["listen", *args], catch_exceptions=False)
            assert result.stderr == ""
            assert result.exit_code == 0


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


@pytest.mark.parametrize("args", [(), ("--debug",), ("--client-name", "name1"), ("--message", "Test Message")])
def test_send_command(settings, runner: CliRunner, args) -> None:
    settings.STREAMING = {"BROKER_URL": "rabbit://localhost:10000"}

    mock_backend = mock.MagicMock(spec=RabbitMQBackend)

    with mock.patch("streaming.__cli__.assert_backend") as get_backend:
        get_backend.return_value = mock_backend

        result = runner.invoke(cli, ["send", "a.b", *args])
        assert result.exit_code == 0
        assert "Sent:" in result.output
        mock_backend.publish.assert_called_once()


def test_listen_command(backend, runner: CliRunner, configure_server) -> None:
    backend = get_backend()
    with mock.patch("streaming.__cli__.assert_backend") as mocked_assert_backend:
        mocked_assert_backend.return_value = backend
        with mock.patch.object(backend, "listen") as mock_listen:
            result = runner.invoke(cli, ["listen", "--queues", "test_queue"], catch_exceptions=False)
            assert result.exit_code == 0
            mock_listen.assert_called()


def test_listen_reload(runner: CliRunner) -> None:
    with mock.patch("streaming.__cli__._listen"):
        with mock.patch("django.utils.autoreload.run_with_reloader") as mocked_run_with_reloader:
            mocked_run_with_reloader.side_effect = lambda func, *a, **kw: func()
            result = runner.invoke(cli, ["listen", "--autoreload"], catch_exceptions=False)
            assert result.stderr == ""
            assert result.exit_code == 0


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


def test_check_command(runner: CliRunner, configure_server) -> None:
    backend = get_backend()
    with mock.patch("streaming.__cli__.assert_backend") as mocked_assert_backend:
        mocked_assert_backend.return_value = backend
        result = runner.invoke(cli, ["check"], catch_exceptions=False)
        assert result.stderr == ""
        assert result.exit_code == 0
        assert "System Configuration:" in result.output
        assert "Connection successful." in result.output

        with mock.patch.object(backend, "connect") as mocked_connect:
            mocked_connect.side_effect = StreamingConfigError
            result = runner.invoke(cli, ["check"], catch_exceptions=False)
            assert "Error: Connection failed" in result.stderr
            assert result.exit_code == 1
