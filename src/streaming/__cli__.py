import json
import logging
from collections.abc import Iterable
from typing import TYPE_CHECKING

import click
from click import ClickException
from colorama import Fore, Style
from django.core.exceptions import ImproperlyConfigured
from django.utils.module_loading import import_string
from pika.exceptions import ChannelClosedByBroker

from .backends import RabbitMQBackend, get_backend
from .config import CONFIG
from .exceptions import AuthorizationError, CallbackRetry, StreamingConfigError
from .utils import LevelFormatter, check_callback, make_event

if TYPE_CHECKING:
    from .event import Event


logger = logging.getLogger(__name__)
INFO_LINE = f"{Fore.YELLOW}%-16s: {Style.RESET_ALL}%s"


def _dump_info(backend: "RabbitMQBackend") -> None:
    click.secho(INFO_LINE % ("Server", f"{backend.host}:{backend.port}"))
    click.secho(INFO_LINE % ("VirtualHost", f"{backend.virtual_host}"))
    click.secho(INFO_LINE % ("Exchange", f"{backend.exchange}"))
    click.secho(INFO_LINE % ("Timeout", f"{backend.timeout}"))
    click.secho(INFO_LINE % ("Client Name", f"{backend.client_name}"))


def assert_backend() -> "RabbitMQBackend":
    backend: RabbitMQBackend = get_backend()  # type: ignore[assignment]

    if not isinstance(backend, RabbitMQBackend):
        raise ClickException("RabbitMQ backend is not configured")
    return backend


def configure_logging(debug: bool, loggers: Iterable[str] = ("streaming",)) -> None:
    # This should be called only from a click command
    formatter = LevelFormatter()
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    for log_name in loggers:
        logr = logging.getLogger(log_name)
        logr.handlers = []
        if debug:
            logr.setLevel(logging.DEBUG)
            logr.addHandler(ch)


@click.group()
def cli() -> None:
    """Streaming CLI."""
    try:
        import django

        django.setup()
    except ModuleNotFoundError as e:
        raise ClickException(f"Unable to setup Django. {e}") from e
    except ImproperlyConfigured as e:
        raise ClickException("Unable to setup Django. Is DJANGO_SETTINGS_MODULE environment variable set?") from e


@cli.command()
@click.option("--queues/--no-queues", "queues", is_flag=True, default=False, help="Debug mode")
@click.option("--debug", is_flag=True, help="Debug mode")
def configure(queues: bool = False, debug: bool = False) -> None:
    from streaming.config import CONFIG

    routing = None
    configure_logging(debug)
    backend: RabbitMQBackend = assert_backend()
    try:
        backend.connect(True)
        backend.configure_exchanges()
        if queues:
            routing = backend.configure_queue_routing()
        check.callback()  # type: ignore[misc]
        if routing:
            click.secho(INFO_LINE % ("Queues", ""))
            for k, v in routing.items():
                click.secho(INFO_LINE % (f"  {k}", "; ".join(v)))
    except AuthorizationError as e:
        click.secho(f"Unable to connect using {CONFIG.BROKER_URL}", fg="red", err=True)
        raise ClickException(str(e)) from e
    except StreamingConfigError as e:
        click.secho(f"Generic error {e}", fg="red", err=True)
        raise ClickException(str(e)) from e


@cli.command()
@click.argument("routing_key")
@click.option("-c", "--client-name", default=None, help="Override client name")
@click.option("--message", default="Test Message", help="Message to send")
@click.option("--debug", is_flag=True, help="Debug mode")
def send(routing_key: str, message: str, client_name: str, debug: bool) -> None:
    backend = assert_backend()
    configure_logging(debug)

    if client_name:
        backend.client_name = client_name
    try:
        payload = json.loads(message)
    except json.decoder.JSONDecodeError:
        payload = {
            "message": message,
        }
    msg: Event = make_event(message=payload)
    backend.publish(routing_key, msg)
    click.secho(f"Sent: {msg}")
    backend.disconnect()


@cli.command()
@click.option("-q", "--queues", multiple=True, help="Queue name to listen to")
@click.option("-cb", "--callback", default=None, help="User callback")
@click.option("--debug", is_flag=True, help="Debug mode")
def listen(  # noqa PLR0913
    queues: list[str], callback: str | None = None, debug: bool = False, dry_run: bool = False
) -> None:
    """Listens for streaming events."""
    configure_logging(debug)

    if not callback:
        callback = CONFIG.LISTEN_CALLBACK

    try:
        cb = import_string(callback)
        if not check_callback(cb):
            raise ClickException(f"Callback {callback} is not a valid callback")
    except ImportError:
        raise ClickException(f"Callback {callback} is not a valid callback") from None

    backend = assert_backend()
    try:
        _dump_info(backend)
        backend.connect(True)
        backend.listen(cb, queues=queues, ack=not dry_run)
    except ChannelClosedByBroker as e:
        logger.debug(e)
        click.secho(str(e), fg="red")
        if "no queue" in str(e):
            click.secho("Did you run 'stream configure --queues'", fg="red")
    except CallbackRetry as e:
        logger.debug(e)
    except (StreamingConfigError, ImportError) as e:
        logger.debug(e)
        click.secho(str(e), fg="red")
        click.get_current_context().exit(2)
    except KeyboardInterrupt as e:
        logger.debug(e)
        click.secho("Stopping listener.", fg="yellow")
    finally:
        backend.disconnect()


@cli.command()
def purge() -> None:
    """Purges all messages from the configured queues."""
    from streaming.backends.rabbitmq import RabbitMQBackend
    from streaming.config import CONFIG
    from streaming.manager import initialize_engine

    manager = initialize_engine(True)
    backend = manager.backend
    if not isinstance(backend, RabbitMQBackend):
        raise click.ClickException("RabbitMQ backend is not configured. Please set BROKER_URL to a rabbit:// URL.")

    backend.connect(True)
    for queue_alias, queue_config in CONFIG.QUEUES.items():
        queue_name = queue_config.get("name", queue_alias)
        try:
            message_count = backend.channel.queue_purge(queue_name)  # type: ignore[union-attr]
            click.secho(f"Purged {message_count.method.message_count} messages from queue '{queue_name}'.", fg="green")
        except ChannelClosedByBroker:
            click.secho(f"Could not purge queue '{queue_name}'. Queue may not exist.", fg="red")
    backend.disconnect()


@cli.command()
def check() -> None:
    """Checks the streaming configuration and connection."""
    from streaming.config import CONFIG

    click.secho("System Configuration:")
    config_dict = dict(CONFIG._parsed)
    for key, value in config_dict.items():
        click.echo(f"  {key}: {value}")

    backend: RabbitMQBackend = assert_backend()
    try:
        backend.connect(True)
        click.secho("Connection successful.", fg="green")
        _dump_info(backend)
    except (StreamingConfigError, AuthorizationError) as e:
        raise ClickException(f"Connection failed: {e}") from e
