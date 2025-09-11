# Usage

`hope-streaming` provides a flexible framework for streaming data from your Django applications to various backends.

## Basic Usage

To use `hope-streaming`, you typically configure a backend in your Django settings and then use the provided `publish` function to send messages.

Example `settings.py` configuration for the console backend:

```python
STREAMING = {
    "BROKER_URL": "console://"
}
```

Then, in your Django application, you can publish messages:

```python
from streaming.manager import manager

manager.publish("Your message here!")
```

## Available Backends

`hope-streaming` supports various backends, including:

-   **Console Backend:** For printing messages to the console (useful for debugging).
-   **RabbitMQ Backend:** For publishing messages to a RabbitMQ message broker.
-   **Redis Backend:** For publishing messages to a Redis instance.

Refer to the specific backend documentation for detailed configuration and usage instructions.
