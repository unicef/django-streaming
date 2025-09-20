import datetime
import json
import logging
import random
from typing import TYPE_CHECKING

import click
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties

from streaming.utils import make_event

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
    import django

    django.setup()


@cli.group()
def rabbit() -> None:
    """RabbitMQ client."""


@rabbit.command()
@click.option("--message", default="Test Message", help="Message to send")
@click.option("--domain", default="", help="Consumer name")
def send(message: str, domain: str) -> None:
    from streaming.backends.rabbitmq import RabbitMQBackend
    from streaming.manager import initialize_engine

    manager = initialize_engine(True)

    backend = manager.backend
    if not isinstance(backend, RabbitMQBackend):
        raise click.ClickException("RabbitMQ backend is not configured. Please set BROKER_URL to a rabbit:// URL.")

    backend.connection_name = "sender"
    backend.connect()
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    try:
        payload = json.loads(message)
    except json.decoder.JSONDecodeError:
        payload = {
            "timestamp": timestamp,
            "message": message,
        }
    msg: EventType = make_event(payload, event="Test", domain=domain)
    backend.publish(msg)
    click.secho(f"Server: {backend.host}:{backend.port}")
    click.secho(f"Publish to: {backend.exchange} {domain}")
    click.secho(f"Sent: {msg}")
    backend.connection.close()  # type: ignore[union-attr]


@rabbit.command()
@click.option("--name", default=None, help="Consumer name")
@click.option("--domain", default="", help="Domain name")
def listen(name: str, domain: str) -> None:
    from streaming.backends.rabbitmq import RabbitMQBackend
    from streaming.manager import initialize_engine

    manager = initialize_engine(True)
    backend: RabbitMQBackend = manager.backend  # type: ignore[assignment]

    if not isinstance(manager.backend, RabbitMQBackend):
        raise click.ClickException("RabbitMQ backend is not configured. Please set BROKER_URL to a rabbit:// URL.")

    if not name:
        name = random.choice(names)  # noqa S311

    backend.connection_name = name
    backend.connect()

    click.secho(f"Server: {backend.host}:{backend.port}")
    click.secho(f"Consumer: {name}")
    click.secho(f"Listen on: {backend.exchange} {domain}")

    def callback(ch: BlockingChannel, method: Basic.Deliver, properties: BasicProperties, body: bytes) -> None:
        click.echo(f"Received {body.decode()}")

    try:
        backend.listen([domain], callback)
    except KeyboardInterrupt:
        click.secho("\nStopping listener.", fg="yellow")
    finally:
        backend.close()
