# Configuration

The `django-streaming` library can be configured via the `STREAMING` dictionary in your Django project's `settings.py`.

## General Settings

*   **`BROKER_URL`** (string, default: `"console://"`):
    The URL for the message broker. This determines which backend is used and its connection parameters. Examples:
    *   `console://`
    *   `debug://`
    *   `amqp://guest:guest@localhost:5672/%2F` (for RabbitMQ)

*   **`RETRY_COUNT`** (int, default: `3`):
    The number of times the backend will attempt to reconnect to the message broker if the connection is lost or fails during establishment.

*   **`RETRY_DELAY`** (int, default: `1`):
    The delay in seconds between retry attempts when connecting to the message broker.

*   **`MESSAGE_TTL`** (int, default: `172800` seconds, i.e. 2 days):
    The maximum time a message can live in a queue before being discarded. The value is in seconds.

*   **`MANAGER_CLASS`** (string, default: `"streaming.manager.ChangeManager"`):
    The Python path to the `ChangeManager` class to be used. You can switch to the threaded manager by setting this to `"streaming.manager.ThreadedChangeManager"`.

*   **`QUEUES`** (dict, default: `{}`):
    A dictionary to configure the queues. The keys are queue aliases, and the values are dictionaries with queue parameters.

    Example:
    ```python
    STREAMING = {
        "QUEUES": {
            "invoices": {
                "name": "invoices_queue",
                "binding_keys": ["invoices.*"],
                "options": {"x-message-ttl": 60000}
            },
            "orders": {
                "name": "orders_queue",
                "binding_keys": ["orders.*"],
            }
        }
    }
    ```

    For each queue, you can specify:
    *   `name`: The actual queue name on the broker. If not provided, the alias is used as the name.
    *   `binding_keys`: A list of routing keys to bind the queue to the exchange.
    *   `options`: A dictionary of arguments to pass to the `queue_declare` method of the backend. This can be used to set queue properties like `x-message-ttl`.

## RabbitMQ Specific Settings

*   **`CLIENT_NAME`** (string, default: `""`):
    A label for the RabbitMQ connection, visible in the RabbitMQ management interface.

## Full Configuration Example

```python

    STREAMING = {
        "BROKER_URL": "rabbit://user:password@localhost:5672/?virtual_host=my_vhost&exchange=my_exchange",
        "QUEUES": {
            "invoices": {
                "name": "invoices_queue",
                "binding_keys": ["invoices.*"],
            },
        },
        "CLIENT_NAME": "my_app_name",
        "RETRY_COUNT": 3,
        "RETRY_DELAY": 1,
        "MESSAGE_TTL": 60 * 60 * 24 * 2,  # 2 days
        "MANAGER_CLASS": "streaming.manager.ChangeManager",
        "LISTEN_CALLBACK": "streaming.callbacks.default_callback",
    }

```
