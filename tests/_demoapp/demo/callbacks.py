import logging
import sys

from colorama import Fore
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties

from streaming.backends.rabbitmq import MAX_RETRIES
from streaming.event import Event
from streaming.exceptions import CallbackRetry, CallbackSkipAck

logger = logging.getLogger(__name__)


def invalid(ch: BlockingChannel, method: Basic.Deliver, properties: BasicProperties, body: bytes) -> bool:
    return True


def nack(queue_name: str, ch: BlockingChannel, method: Basic.Deliver, properties: BasicProperties, body: bytes) -> bool:
    logger.debug("Invoking default callback")
    message: Event = Event.unmarshal(body)
    ack = f"{Fore.RED}NACK{Fore.RESET}"
    sys.stdout.write(
        f"{Fore.GREEN}{message.timestamp} [{queue_name}]{Fore.LIGHTWHITE_EX} [{message.key}] {message.id} {ack}\n"
    )
    return False


def retry_delay(
    queue_name: str, ch: BlockingChannel, method: Basic.Deliver, properties: BasicProperties, body: bytes
) -> bool:
    attempts = int(properties.headers.get("x-retries", 0))  # type: ignore[union-attr]
    if attempts < MAX_RETRIES:
        raise CallbackRetry(f"Attempt {attempts}")
    message: Event = Event.unmarshal(body)
    ack = f"{Fore.GREEN}ACK{Fore.RESET}"
    sys.stdout.write(
        f"{Fore.GREEN}{message.timestamp} [{queue_name}]{Fore.LIGHTWHITE_EX} [{message.key}] {message.id} {ack}\n"
    )
    return True


def retry_noack(
    queue_name: str, ch: BlockingChannel, method: Basic.Deliver, properties: BasicProperties, body: bytes
) -> bool:
    message: Event = Event.unmarshal(body)
    ack = f"{Fore.RED}NACK{Fore.RESET}"
    sys.stdout.write(
        f"{Fore.GREEN}{message.timestamp} [{queue_name}]{Fore.LIGHTWHITE_EX} [{message.key}] {message.id} {ack}\n"
    )
    raise CallbackSkipAck
