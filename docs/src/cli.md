# Command Line Interface (CLI)

`django-streaming` provides a command-line interface (CLI) built with `click` to help you interact with the streaming system, especially for testing and debugging.

## Basic Usage

To see the available commands, run:

```bash
stream --help
```

This will show you the main commands: `listen`, `send`, `purge`, and `check`.

## `listen`

The `listen` command allows you to listen for messages from one or more RabbitMQ queues.

```bash
stream listen --help
```

**Options:**

*   `BINDING_KEYS`: (argument, required) One or more binding keys to listen for.
*   `--queue TEXT`: (option, required, multiple) The name of the queue to listen to. You can specify this option multiple times to listen to multiple queues.
*   `--payload`: (flag) Print the message payload.
*   `--autoreload`: (flag) Enable auto-reloading for development.
*   `--pretty`: (flag) Pretty-print the JSON payload.

**Example:**

```bash
stream listen "fattura.*" --queue my_queue --pretty
```

This command will listen to the `my_queue` for messages with a routing key matching `fattura.*` and pretty-print the payload.

## `send`

The `send` command sends a message to a RabbitMQ exchange with a specific routing key.

```bash
stream send --help
```

**Arguments:**

*   `ROUTING_KEY`: (required) The routing key for the message.

**Options:**

*   `--message TEXT`: The message to send. Can be a plain string or a JSON string. Defaults to "Test Message".

**Example:**

```bash
stream send fattura.emessa --message '''{"amount": 100, "customer": "Acme Corp"}'''
```

## `purge`

The `purge` command purges all messages from one or more specified queues.

```bash
stream purge --help
```

**Arguments:**

*   `QUEUES`: (required) One or more queue names to purge.

**Example:**

```bash
stream purge my_queue another_queue
```

## `check`

The `check` command displays the current `django-streaming` configuration and checks the connection to the message broker.

```bash
stream check
```