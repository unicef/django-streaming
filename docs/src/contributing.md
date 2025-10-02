# Contributing to django-streaming

First off, thank you for considering contributing to django-streaming! It's people like you that make django-streaming such a great tool.

## Where do I go from here?

If you've noticed a bug or have a question, [search the issue tracker](https://github.com/UNICEF/django-streaming/issues) to see if someone else in the community has already created a ticket.
If not, feel free to create a new one!

## Fork & create a branch

If you want to contribute with a new feature or a bugfix, please fork the repository and create a new branch from `main`.

## Get started

To start developing `django-streaming`, you'll need to set up a local development environment.

### 1. Install dependencies

This project uses `uv`:

```bash
uv venv
uv sync --all-extras
```

### 2. Run tests

To run djangp-streaming test suite, you need a running RabbitMQ server instance or you can use
provided Docker compose file with:

```bash
docker compose -f tests/docker-compose.yml up --build --detach
```

Run either tox:

```bash
    tox -e d52-py313
```
or pytest

```bash
    pytest tests
```

Remember to stop your testing containers

.note!!

    If you have your own RabbitMQ server, you can use it just setting relevant end var:

        RABBIT_SERVER="user:password@localhost:5672" tox


```bash
docker compose -f tests/docker-compose.yml down

```

This command runs the tests for Django 5.2 with Python 3.13. You can also run tests for other environments by specifying a different environment, such as `d51-py312`.

### 3. Run linter

To check for code style and linting errors, run:

```bash
tox lint
```

### 4. Build documentation

To build the documentation, use the following command:

```bash
tox docs
```

## Proposing a change

When you're ready to propose your change, please open a pull request.
Please make sure to include a clear and concise title and description of your changes.
If your change is related to an issue, please link it in the description.
