import json
import logging
from collections.abc import Iterable
import curses

import click
from click import ClickException
from colorama import Fore, Style
from django.core.exceptions import ImproperlyConfigured
from django.utils.module_loading import import_string
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import ChannelClosedByBroker
from pika.spec import Basic, BasicProperties

from .backends import RabbitMQBackend, get_backend
from .event import Event
from .exceptions import AuthorizationError, StreamingCallbackRetryError, StreamingConfigError
from .types import UserCallback, check_callback
from .utils import make_event

logger = logging.getLogger(__name__)
INFO_LINE = f"{Fore.YELLOW}%-16s: {Style.RESET_ALL}%s"


def _get_info(backend: "RabbitMQBackend") -> str:
    return (
        f"{INFO_LINE % ('Server', f'{backend.host}:{backend.port}')}\n"
        f"{INFO_LINE % ('VirtualHost', f'{backend.virtual_host}')}\n"
        f"{INFO_LINE % ('Exchange', f'{backend.exchange}')}\n"
        f"{INFO_LINE % ('Timeout', f'{backend.timeout}')}\n"
        f"{INFO_LINE % ('Client Name', f'{backend.client_name}')}"
    )


def _dump_info(backend: "RabbitMQBackend", stdscr) -> None:
    stdscr.addstr(0, 0, _get_info(backend))
    stdscr.refresh()


def assert_backend() -> "RabbitMQBackend":
    backend: RabbitMQBackend = get_backend()  # type: ignore[assignment]

    if not isinstance(backend, RabbitMQBackend):
        raise ClickException("RabbitMQ backend is not configured")
    return backend


def configure_logging(debug: bool, loggers: Iterable[str] = ("streaming",)) -> None:
    for log_name in loggers:
        logr = logging.getLogger(log_name)
        if debug:
            logr.setLevel(logging.DEBUG)
            logr.addHandler(logging.StreamHandler())
        else:
            logr.handlers = []


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
    msg: Event = make_event(payload, key="Test")
    backend.publish(routing_key, msg)
    click.secho(f"Sent: {msg}")
    backend.disconnect()


def _default_callback(
    stdscr,
    queue_name: str,
    ch: BlockingChannel,
    method: Basic.Deliver,
    properties: BasicProperties,
    body: bytes,
    dry_run: bool = False,
) -> None:
    message: Event = Event.unmarshal(body)
    stdscr.addstr(f"{message.timestamp} [{queue_name}] [{message.key}] {message.id} \n")
    stdscr.refresh()
    if not dry_run:
        ch.basic_ack(delivery_tag=method.delivery_tag)


def _listen(queues: list[str], callback: "UserCallback", ack: bool = True, stdscr=None) -> None:
    backend = assert_backend()

    try:
        backend.connect(True)
        _dump_info(backend, stdscr)
        backend.listen(callback, queues=queues, ack=ack)
    except ChannelClosedByBroker as e:
        click.secho(str(e), fg="red")
        if "no queue" in str(e):
            click.secho("Did you run 'stream configure --queues'", fg="red")
    except StreamingCallbackRetryError:
        pass
    except (StreamingConfigError, ImportError) as e:
        click.secho(str(e), fg="red")
        click.get_current_context().exit(2)
    except KeyboardInterrupt:
        click.secho("Stopping listener.", fg="yellow")
        raise
        raise
    finally:
        backend.disconnect()


def _curses_listen(stdscr, queues: list[str], callback: str | None = None, dry_run: bool = False) -> None:
    try:
        if callback:
            try:
                cb = import_string(callback)
                if not check_callback(cb, UserCallback):
                    raise ClickException(f"Callback {callback} is not a valid callback")
            except ImportError as e:
                raise StreamingConfigError(f"Unable to import {callback}: {e}") from None
        else:
            cb = _default_callback

        _listen(queues=queues, callback=cb, ack=not dry_run, stdscr=stdscr)
    except KeyboardInterrupt:
        raise


@cli.command()
@click.option("-q", "--queues", multiple=True, help="Queue name to listen to")
@click.option("-cb", "--callback", default=None, help="User callback")
@click.option("--debug", is_flag=True, help="Debug mode")
@click.option("--dry-run", default=False, is_flag=True)
def listen(  # noqa PLR0913
    queues: list[str],
    callback: str | None = None,
    debug: bool = False,
    dry_run: bool = False,
) -> None:
    """Listens for streaming events."""
    configure_logging(debug)
    assert_backend()
    curses.wrapper(_curses_listen, queues=queues, callback=callback, dry_run=dry_run)


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
        click.secho(_get_info(backend))
    except (StreamingConfigError, AuthorizationError) as e:
        raise ClickException(f"Connection failed: {e}") from e
