# Redis Backend

The Redis Backend allows you to publish messages to a Redis instance. This can be used for real-time messaging or as a simple message queue.

## Configuration

To use the Redis Backend, configure your `BROKER_URL` in `settings.py` with the Redis connection details. The format is typically `redis://[password]@host:port/db`.

Example `settings.py` configuration:

```python
STREAMING = {
    "BROKER_URL": "redis://localhost:6379/0"
}
```

-   `localhost:6379`: Default Redis host and port.
-   `0`: The Redis database number to use.

## Connection Reliability

The Redis backend includes a retry mechanism for establishing and re-establishing connections. If the initial connection fails or an existing connection is lost, the backend will attempt to reconnect multiple times with a delay between attempts. These behaviors are controlled by the `RETRY_COUNT` and `RETRY_DELAY` settings in your `STREAMING` configuration.

## Usage

Once configured, you can publish messages using the `manager.publish()` method. These messages will be pushed to a Redis list.

```python
from streaming.manager import manager

manager.publish("Hello from Redis Backend!")
```

## List Name

By default, messages are pushed to a Redis list named `django_model_changes`.
You can customize this list name in your `STREAMING` settings:

```python
STREAMING = {
    "BROKER_URL": "redis://localhost:6379/0?queue=test",
}
```
