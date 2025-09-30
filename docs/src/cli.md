# Command Line Interface (CLI)

`django-streaming` provides a command-line interface (CLI) built with `click` to help you interact with the streaming system, especially for testing and debugging.

## Basic Usage

To see the available commands, run:

```bash
stream --help
```

This will show you the main commands: `listen`, `send`, `purge`, and `check`.

## `listen`

The `listen` command allows you to listen for messages from one or more RabbitMQ queues configured in your `settings.py`.

```bash
stream listen --help
```

**Options:**

*   `--queues TEXT`: (option, multiple) The alias of the queue to listen to. You can specify this option multiple times to listen to multiple queues. If not provided, it will listen to all queues defined in `STREAMING['QUEUES']`.
*   `--payload`: (flag) Print the message payload.
*   `--autoreload`: (flag) Enable auto-reloading for development.
*   `--pretty`: (flag) Pretty-print the JSON payload.

**Example:**

```bash
stream listen --queues invoices --pretty
```

This command will listen to the queue with the alias `invoices` and pretty-print the payload.

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

The `purge` command purges all messages from all queues configured in `STREAMING['QUEUES']`.

```bash
stream purge --help
```

**Example:**

```bash
stream purge
```

## `check`

The `check` command displays the current `django-streaming` configuration and checks the connection to the message broker.

```bash
stream check
```
