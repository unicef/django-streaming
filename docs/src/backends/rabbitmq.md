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

## Usage

Once configured, you can publish messages using the `manager.publish()` method. These messages will be sent to the configured RabbitMQ queue.

```python
from streaming.manager import manager

manager.publish("Hello from RabbitMQ Backend!")
```

## Queue Name

By default, messages are published to a queue named `django_model_changes`. You can customize this queue name in your `STREAMING` settings by adding the `queue` parameter to the `BROKER_URL`:

```python
STREAMING = {
    "BROKER_URL": "amqp://guest:guest@localhost:5672/?queue=my_custom_queue",
}
```

## Advanced Queue Configuration

When consuming messages, you can configure advanced queue properties like durability and message time-to-live (TTL).

### Durable Queues

By default, queues are declared as durable, meaning they will survive a broker restart. You can control this behavior using the `durable` parameter in the `listen` method.

### Message Time-to-Live (TTL)

You can set a message TTL for a queue, which defines how long a message can live in the queue before it is discarded. This is useful to prevent queues from growing indefinitely with old messages.

To set a TTL, you can use the `queue_arguments` parameter in the `listen` method and pass the `x-message-ttl` argument (in milliseconds).

Example:

```python
from streaming.manager import manager

def my_callback(ch, method, properties, body):
    print(f"Received message: {body}")

manager.listen(
    queue_name="my_queue",
    binding_keys=["my_routing_key"],
    callback=my_callback,
    durable=True,
    queue_arguments={"x-message-ttl": 60000}  # 1 minute TTL
)
```
