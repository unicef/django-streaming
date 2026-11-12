# Usage

`django-streaming` provides a flexible framework for streaming data from your Django applications to various backends.

## Basic Usage

To use `django-streaming`, you typically configure a backend in your Django settings and then use the provided `publish` function to send messages.

Example `settings.py` configuration for the console backend:

```python
STREAMING = {
    "BROKER_URL": "console://"
}
```

Then, in your Django application, you can publish messages:

```python
from streaming.manager import manager
from streaming.utils import make_event

manager.notify("invoice.create", make_event("Your message here!"))

```

OR

```python
from streaming.manager import manager
from streaming.utils import make_event

manager.notify("invoice.create", make_event("Your message here!", key="key", message_id="my_id"))

```

## Using the Threaded Change Manager

For applications where blocking the main thread for message publishing is undesirable (e.g., web applications), `django-streaming` provides a `ThreadedChangeManager`. This manager publishes messages asynchronously in a separate thread.

To enable the threaded manager, set the `MANAGER_CLASS` in your `settings.py`:

```python
STREAMING = {
    "MANAGER_CLASS": "streaming.manager.ThreadedChangeManager"
}
```

When using the `ThreadedChangeManager`, messages are queued and processed in a background thread. The manager handles graceful shutdown on process termination, attempting to empty the queue within a timeout.

## Available Backends

`django-streaming` supports various backends, including:

-   **Console Backend:** For printing messages to the console (useful for debugging).
-   **RabbitMQ Backend:** For publishing messages to a RabbitMQ message broker.

See [Configuration](configuration.md) for backend settings and the [CLI guide](cli.md) for listener commands.


## RabbitMQ callback retries

A RabbitMQ listener callback can raise `CallbackRetry` to request another attempt. Retries use exponential delays
(2, 4, 8, 16, and 32 seconds by default). The number of additional attempts is controlled by
[`CALLBACK_RETRY_COUNT`](configuration.md#rabbitmq-specific-settings) (default: `5`); `0` sends the first failed
attempt directly to the exhausted queue. See [`stream listen`](cli.md#listen) for running a listener.

```text
Callback
  |-- success ------------------> ACK original
  `-- CallbackRetry
        |-- retries remain -----> delayed retry
        `-- retries exhausted --> exhausted queue
                                    |-- confirmed -> ACK original
                                    `-- failed ----> redelivery
```

When retries are exhausted, the received message body and relevant AMQP metadata are published to a durable
`<client_name>:<queue_alias>_exhausted` queue, created on demand and bound to the existing retry exchange. The copy
is persistent and has no per-message TTL. The original delivery is acknowledged only after the broker confirms the
new copy; if publication fails, the original remains available for redelivery. Delivery may be duplicated if the
confirmation succeeds but the original acknowledgement is lost.

Exhausted messages are not replayed automatically and must be reviewed or reprocessed separately.

!!! note "RabbitMQ deployment"
    The listener's RabbitMQ user needs permission to create and bind exhausted queues and publish through the retry
    exchange in its vhost. Ensure broker policies do not expire or discard these messages. Monitoring exhausted queue
    sizes is recommended.
