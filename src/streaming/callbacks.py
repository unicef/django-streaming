import logging
import sys

from colorama import Fore
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties

from streaming.event import Event

logger = logging.getLogger(__name__)


def default_callback(
    queue_name: str, ch: BlockingChannel, method: Basic.Deliver, properties: BasicProperties, body: bytes
) -> bool:
    logger.debug("Invoking default callback")
    message: Event = Event.unmarshal(body)
    routing_key = method.routing_key
    ack = f"{Fore.GREEN}ACK{Fore.RESET}"
    sys.stdout.write(
        f"{Fore.GREEN}{message.timestamp} [{queue_name}]{Fore.LIGHTWHITE_EX} [{routing_key}] {message.id} {ack}\n"
    )
    return True
