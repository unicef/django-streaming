# Configuration

The `django-streaming` library can be configured via the `STREAMING` dictionary in your Django project's `settings.py`.

## General Settings

*   **`BROKER_URL`** (string, default: `"redis://localhost:6379/0?queue=hope_changes"`):
    The URL for the message broker. This determines which backend is used and its connection parameters. Examples:
    *   `console://`
    *   `debug://`
    *   `amqp://guest:guest@localhost:5672/%2F` (for RabbitMQ)
    *   `redis://localhost:6379/0` (for Redis)

*   **`RETRY_COUNT`** (int, default: `3`):
    The number of times the backend will attempt to reconnect to the message broker if the connection is lost or fails during establishment.

*   **`RETRY_DELAY`** (int, default: `1`):
    The delay in seconds between retry attempts when connecting to the message broker.

*   **`MANAGER_CLASS`** (string, default: `"streaming.manager.ChangeManager"`):
    The Python path to the `ChangeManager` class to be used. You can switch to the threaded manager by setting this to `"streaming.threaded.ThreadedChangeManager"`.

## RabbitMQ Specific Settings

*   **`CONNECTION_NAME`** (string, default: `"django-streaming-app"`):
    A label for the RabbitMQ connection, visible in the RabbitMQ management interface.
