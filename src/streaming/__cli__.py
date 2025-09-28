import datetime
import json
import logging
import random
from typing import TYPE_CHECKING

import click
from click import ClickException
from django.core.exceptions import ImproperlyConfigured
from django.db.models.functions import Trunc
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import ChannelClosedByBroker
from pika.spec import Basic, BasicProperties

from streaming.backends.rabbitmq import RabbitMQBackend
from streaming.exceptions import StreamingConfigError, AuthorizationError
from streaming.utils import make_event
from colorama import Fore, Style

if TYPE_CHECKING:
    from streaming.types import EventType

logger = logging.getLogger(__name__)

names = [
    "Spider-Man",
    "Peter Parker",
    "Iron Man",
    "Tony Stark",
    "Captain America",
    "Steve Rogers",
    "Thor",
    "Thor Odinson",
    "Hulk",
    "Bruce Banner",
    "Black Widow",
    "Natasha Romanoff",
    "Hawkeye",
    "Clint Barton",
    "Doctor Strange",
    "Stephen Strange",
    "Black Panther",
    "T’Challa",
    "Scarlet Witch",
    "Wanda Maximoff",
    "Vision",
    "Vision",
    "Ant-Man",
    "Scott Lang",
    "Wasp",
    "Hope van Dyne",
    "Falcon",
    "Sam Wilson",
    "Winter Soldier",
    "Bucky Barnes",
    "Captain Marvel",
    "Carol Danvers",
    "Mr. Fantastic",
    "Reed Richards",
    "Invisible Woman",
    "Sue Storm",
    "Human Torch",
    "Johnny Storm",
    "The Thing",
    "Ben Grimm",
    "Wolverine",
    "James Howlett",
    "Cyclops",
    "Scott Summers",
    "Jean Grey",
    "Jean Grey",
    "Storm",
    "Ororo Munroe",
    "Professor X",
    "Charles Xavier",
    "Rogue",
    "Anna Marie",
    "Gambit",
    "Remy LeBeau",
    "Beast",
    "Hank McCoy",
    "Colossus",
    "Piotr Rasputin",
    "Nightcrawler",
    "Kurt Wagner",
]




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
@click.argument("routing_key")
@click.option("--message", default="Test Message", help="Message to send")
def send(routing_key: str, message: str) -> None:
    from streaming.backends.rabbitmq import RabbitMQBackend
    from streaming.manager import initialize_engine

    manager = initialize_engine(True)

    backend = manager.backend
    if not isinstance(backend, RabbitMQBackend):
        raise click.ClickException("RabbitMQ backend is not configured. Please set BROKER_URL to a rabbit:// URL.")

    backend.connection_name = "sender"
    backend.connect()

    try:
        payload = json.loads(message)
    except json.decoder.JSONDecodeError:
        payload = {
            "message": message,
        }
    msg: EventType = make_event(payload, event="Test")
    backend.publish(msg, routing_key=routing_key)
    click.secho(f"Sent: {msg}")
    backend.connection.close()  # type: ignore[union-attr]


def _listen(binding_keys: tuple[str, ...], queues: tuple[str, ...], payload: bool, pretty: bool) -> None:
    from streaming.backends.rabbitmq import RabbitMQBackend
    from streaming.manager import initialize_engine

    manager = initialize_engine(True)
    backend: RabbitMQBackend = manager.backend  # type: ignore[assignment]

    if not isinstance(manager.backend, RabbitMQBackend):
        raise click.ClickException(
            "RabbitMQ backend is not configured. Please check your django-streaming configuration"
        )

    name = random.choice(names)  # noqa S311

    backend.connection_name = name
    backend.connect()

    def callback(ch: BlockingChannel, method: Basic.Deliver, properties: BasicProperties, body: bytes) -> None:
        message: EventType = json.loads(body.decode())
        click.echo(f"{Fore.GREEN}{message['timestamp']}{Fore.LIGHTWHITE_EX} [{message['type']}]{message['event']} {message['domain']}")
        if payload:
            if pretty:
                extra = json.dumps(message["payload"], indent=4)
            else:
                extra = message["payload"]
            click.echo(f"{Fore.YELLOW}{extra}{Fore.RESET}")

    try:
        backend.listen(list(queues), list(binding_keys), callback)
    except KeyboardInterrupt:
        click.secho("\nStopping listener.", fg="yellow")
    finally:
        backend.close()


@cli.command()
@click.argument("queues", nargs=-1, required=True)
def purge(queues: tuple[str, ...]) -> None:
    """Purges all messages from the specified queues."""
    from streaming.backends.rabbitmq import RabbitMQBackend
    from streaming.manager import initialize_engine

    manager = initialize_engine(True)
    backend = manager.backend
    if not isinstance(backend, RabbitMQBackend):
        raise click.ClickException("RabbitMQ backend is not configured. Please set BROKER_URL to a rabbit:// URL.")

    backend.connect()
    if backend.channel:
        for queue_name in queues:
            try:
                message_count = backend.channel.queue_purge(queue_name)
                click.secho(f"Purged {message_count.method.message_count} messages from queue '{queue_name}'.", fg="green")
            except ChannelClosedByBroker:
                click.secho(f"Could not purge queue '{queue_name}'. Queue may not exist.", fg="red")
    backend.close()


@cli.command()
def check() -> None:
    """Checks the streaming configuration and connection."""
    from streaming.config import CONFIG
    from streaming.manager import initialize_engine

    click.secho("Streaming Configuration:", bold=True)
    # Convert ChainMap to a dict for consistent iteration
    config_dict = dict(CONFIG._parsed)
    for key, value in config_dict.items():
        click.echo(f"  {key}: {value}")

    click.secho("\nChecking connection...", bold=True)
    try:
        manager = initialize_engine(True)
        backend = manager.backend
        backend.connect(raise_if_error=True)
        click.secho("Connection successful.", fg="green")
        backend.close()
    except (StreamingConfigError, AuthorizationError) as e:
        raise ClickException(f"Connection failed: {e}") from e


@cli.command()
@click.argument("binding_keys", nargs=-1, required=True)
@click.option("--queue", "queues", multiple=True, required=True, help="Queue name to listen to")
@click.option("--payload", default=False, is_flag=True, help="Print payload")
@click.option("--autoreload", is_flag=True, help="Enable auto-reloading.")
@click.option("--pretty", is_flag=True, help="Pretty-print payload.")
def listen(binding_keys: tuple[str, ...], queues: tuple[str, ...], payload: bool, autoreload: bool, pretty: bool) -> None:
    """Listens for streaming events."""
    if autoreload:
        from django.utils import autoreload
        click.secho("Starting listener with autoreload...", fg="yellow")
        autoreload.run_with_reloader(_listen, binding_keys=binding_keys, queues=queues, payload=payload, pretty=pretty)
    else:
        _listen(binding_keys, queues, payload, pretty)
