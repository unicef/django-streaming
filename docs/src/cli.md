# Command Line Interface (CLI)

`django-streaming` provides a command-line interface (CLI) built with `click` to help you interact with the streaming system, especially for testing and debugging.

## Installation

The CLI is installed automatically when you install `django-streaming`. You can run it using the `streaming` command.

## Basic Usage

To see the available commands, run:

```bash
streaming --help
```

## RabbitMQ Commands

The `rabbit` group provides commands for interacting with RabbitMQ.

### `streaming rabbit send`

Sends a message to a RabbitMQ exchange.

```bash
streaming rabbit send --help
```

**Options:**

*   `--message TEXT`: The message to send. Can be a plain string or a JSON string.
*   `--domain TEXT`: An optional domain name to associate with the message.

**Example:**

```bash
streaming rabbit send --message "Hello from CLI!" --domain "my-app"
```

### `streaming rabbit listen`

Listens for messages from a RabbitMQ queue.

```bash
streaming rabbit listen --help
```

**Options:**

*   `--name TEXT`: An optional consumer name. If not provided, a random name will be generated.
*   `--domain TEXT`: An optional domain name to filter messages by.

**Example:**

```bash
streaming rabbit listen --name "my-consumer" --domain "my-app"
```
