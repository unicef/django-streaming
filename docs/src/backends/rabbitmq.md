# RabbitMQ Backend

The RabbitMQ Backend allows you to publish messages to a RabbitMQ message broker. This is suitable for production environments where reliable message delivery and asynchronous processing are required.

## Configuration

To use the RabbitMQ Backend, configure your `BROKER_URL` in `settings.py` with the RabbitMQ connection details. The format is typically `amqp://user:password@host:port/vhost`.

Example `settings.py` configuration:

```python
STREAMING = {
    "BROKER_URL": "amqp://guest:guest@localhost:5672/connection_name=..&virtual_host=..&exchange=.."
}
```

-   `guest:guest`: Default RabbitMQ username and password.
-   `localhost:5672`: Default RabbitMQ host and port.

Arguments:

-   `connection_name`: Name of the connection to identify this client
-   `exchange`: name of the exchange to use (default: `django-streaming-broadcast`)
-   `timeout`: connection timeout (default: `0.5`)
-   `virtual_host`: RabbitMQ virtual host timeout (default: `/`)

## Connection Reliability

The RabbitMQ backend includes a retry mechanism for establishing and re-establishing connections. If the initial connection fails or an existing connection is lost, the backend will attempt to reconnect multiple times with a delay between attempts. These behaviors are controlled by the `RETRY_COUNT` and `RETRY_DELAY` settings in your `STREAMING` configuration.

Additionally, the connection will be gracefully closed when the Python process terminates, ensuring proper resource cleanup.

## Connection Naming

You can assign a custom name to your RabbitMQ connection, which will be visible in the RabbitMQ management interface. This can be useful for monitoring and debugging. Configure the `CONNECTION_NAME` setting in your `STREAMING` dictionary:

## Queue Configuration

With the RabbitMQ backend, you can define multiple queues with different bindings and properties. This is done in your `settings.py` via the `STREAMING['QUEUES']` dictionary.

Example `settings.py` configuration:

```python
STREAMING = {
    "BROKER_URL": "amqp://guest:guest@localhost:5672/",
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

## Listening for messages

To listen for messages, you can use the `stream listen` command line interface. See the [CLI documentation](../cli.md) for more details.

If you need to listen for messages from your code, you can use the `manager.listen()` method.

```python
from streaming.manager import manager

def my_callback(ch, method, properties, body):
    print(f"Received message: {body}")

# Listen to all configured queues
manager.listen(callback=my_callback)

# Listen to a specific queue (by alias)
manager.listen(callback=my_callback, queues=["invoices"])
```

The `listen` method of the `RabbitMQBackend` now takes a `callback` function and an optional list of queue aliases to listen to. If no queues are specified, it will listen to all queues configured in `STREAMING['QUEUES']`.
