# RabbitMQ Backend

The RabbitMQ Backend allows you to publish messages to a RabbitMQ message broker. This is suitable for production environments where reliable message delivery and asynchronous processing are required.

## Configuration

To use the RabbitMQ Backend, configure your `BROKER_URL` in `settings.py` with the RabbitMQ connection details. The format is typically `amqp://user:password@host:port/vhost`.

Example `settings.py` configuration:

```python
STREAMING = {
    "BROKER_URL": "amqp://guest:guest@localhost:5672/%2F"
}
```

-   `guest:guest`: Default RabbitMQ username and password.
-   `localhost:5672`: Default RabbitMQ host and port.
-   `%2F`: URL-encoded virtual host (for the default virtual host `/`).

## Usage

Once configured, you can publish messages using the `manager.publish()` method. These messages will be sent to the configured RabbitMQ queue.

```python
from streaming.manager import manager

manager.publish("Hello from RabbitMQ Backend!")
```

## Queue Name

By default, messages are published to a queue named `django_model_changes`. You can customize this queue name in your `STREAMING` settings:

```python
STREAMING = {
    "BROKER_URL": "amqp://guest:guest@localhost:5672/?queue=my_custom_queue",
}
```
