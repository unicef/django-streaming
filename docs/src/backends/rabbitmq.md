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

## Connection Reliability

The RabbitMQ backend includes a retry mechanism for establishing and re-establishing connections. If the initial connection fails or an existing connection is lost, the backend will attempt to reconnect multiple times with a delay between attempts. These behaviors are controlled by the `RETRY_COUNT` and `RETRY_DELAY` settings in your `STREAMING` configuration.

Additionally, the connection will be gracefully closed when the Python process terminates, ensuring proper resource cleanup.

## Connection Naming

You can assign a custom name to your RabbitMQ connection, which will be visible in the RabbitMQ management interface. This can be useful for monitoring and debugging. Configure the `CONNECTION_NAME` setting in your `STREAMING` dictionary:

```python
STREAMING = {
    "CONNECTION_NAME": "my-django-app-publisher"
}
```

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
