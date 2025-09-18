# Command Line Interface (CLI)

`django-streaming` provides a command-line interface (CLI) built with `click` to help you interact with the streaming system, especially for testing and debugging.

## Installation

The CLI is installed automatically when you install `django-streaming`. You can run it using the `stream` command.

## Basic Usage

To see the available commands, run:

```bash
stream --help
```

## RabbitMQ Commands

The `rabbit` group provides commands for interacting with RabbitMQ.

### `stream rabbit send`

Sends a message to a RabbitMQ exchange.

```bash
stream rabbit send --help
```

**Options:**

*   `--message TEXT`: The message to send. Can be a plain string or a JSON string.
*   `--domain TEXT`: An optional domain name to associate with the message.

**Example:**

```bash
stream rabbit send --message "Hello from CLI!" --domain "my-app"
```

### `stream rabbit listen`

Listens for messages from a RabbitMQ queue.

```bash
stream rabbit listen --help
```

**Options:**

*   `--name TEXT`: An optional consumer name. If not provided, a random name will be generated.
*   `--domain TEXT`: An optional domain name to filter messages by.

**Example:**

```bash
stream rabbit listen --name "my-consumer" --domain "my-app"
```
