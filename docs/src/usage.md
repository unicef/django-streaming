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

manager.publish("Your message here!")
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

Refer to the specific backend documentation for detailed configuration and usage instructions.
