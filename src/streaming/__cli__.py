import json
import logging
from typing import TYPE_CHECKING

import click
from click import ClickException
from colorama import Fore, Style
from django.core.exceptions import ImproperlyConfigured
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import ChannelClosedByBroker
from pika.spec import Basic, BasicProperties

from .backends import RabbitMQBackend, get_backend
from .exceptions import AuthorizationError, StreamingConfigError
from .utils import make_event

if TYPE_CHECKING:
    from .types import JSON, EventType


logger = logging.getLogger(__name__)


def _dump_info(backend: "RabbitMQBackend") -> None:
    from streaming.config import CONFIG

    line = f"{Fore.YELLOW}%-16s: {Style.RESET_ALL}%s"
    click.secho(line % ("Server", f"{backend.host}:{backend.port}"))
    click.secho(line % ("VirtualHost", f"{backend.virtual_host}"))
    click.secho(line % ("Exchange", f"{backend.exchange}"))
    click.secho(line % ("Queues", ""))
    for alias, config in CONFIG.QUEUES.items():
        click.secho(line % (f"   {alias}", f"{config}"))
    click.secho(line % ("Timeout", f"{backend.timeout}"))
    click.secho(line % ("Client Name", f"{backend.client_name}"))


def assert_backend() -> "RabbitMQBackend":
    backend: RabbitMQBackend = get_backend()  # type: ignore[assignment]

    if not isinstance(backend, RabbitMQBackend):
        raise ClickException("RabbitMQ backend is not configured")
    return backend


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
@click.option("--client-name", default=None, help="Override client name")
def configure(client_name: str) -> None:
    from streaming.config import CONFIG

    backend: RabbitMQBackend = assert_backend()
    if client_name:
        backend.client_name = client_name
    try:
        backend.connect(True)
        backend.configure_exchanges()
        backend.configure_queue_routing()
        check.callback()  # type: ignore[misc]
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
    logger = logging.getLogger("streaming")
    if debug:
        logger.setLevel(logging.DEBUG)
        logger.addHandler(logging.StreamHandler())
    else:
        logger.handlers = []

    if client_name:
        backend.client_name = client_name
    try:
        payload = json.loads(message)
    except json.decoder.JSONDecodeError:
        payload = {
            "message": message,
        }
    msg: EventType = make_event(payload, event="Test")
    backend.publish(routing_key, msg)
    click.secho(f"Sent: {msg}")
    backend.disconnect()


def _listen(queues: list[str], payload: bool, pretty: bool, client_name: str) -> None:
    backend = assert_backend()

    if client_name:
        backend.client_name = client_name

    def callback(
        queue_name: str, ch: BlockingChannel, method: Basic.Deliver, properties: BasicProperties, body: bytes
    ) -> None:
        message: EventType = json.loads(body.decode())
        click.echo(
            f"{Fore.GREEN}{message['timestamp']} "
            f"[{queue_name}]"
            f"{Fore.LIGHTWHITE_EX} [{message['type']}]"
            f"{message['event']} "
        )
        extra: str | JSON
        if payload:
            if pretty:
                extra = json.dumps(message["payload"], indent=4)
            else:
                extra = message["payload"]
            click.echo(f"{Fore.YELLOW}{extra}{Fore.RESET}")

    try:
        backend.connect()
        _dump_info(backend)
        backend.listen(callback, queues=queues)
    except KeyboardInterrupt:
        click.secho("Stopping listener.", fg="yellow")
    finally:
        backend.disconnect()


@cli.command()
@click.option("-q", "--queues", multiple=True, help="Queue name to listen to")
@click.option("-c", "--client-name", default=None, help="Override client name")
@click.option("--payload", default=False, is_flag=True, help="Print payload")
@click.option("--autoreload", "reload", is_flag=True, help="Enable auto-reloading.")
@click.option("--pretty", is_flag=True, help="Pretty-print payload.")
def listen(queues: list[str], payload: bool, reload: bool, pretty: bool, client_name: str) -> None:
    """Listens for streaming events."""
    if reload:
        from django.utils import autoreload

        click.secho("Starting listener with autoreload...", fg="yellow")
        autoreload.run_with_reloader(_listen, queues=queues, payload=payload, pretty=pretty, client_name=client_name)
    else:
        _listen(queues=queues, payload=payload, pretty=pretty, client_name=client_name)


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
