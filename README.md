# Django Streaming

[![Test](https://github.com/unicef/django-streaming/actions/workflows/test.yml/badge.svg)](https://github.com/unicef/django-streaming/actions/workflows/test.yml)
[![Lint](https://github.com/unicef/django-streaming/actions/workflows/lint.yml/badge.svg)](https://github.com/unicef/django-streaming/actions/workflows/lint.yml)
[![codecov](https://codecov.io/github/unicef/django-streaming/branch/develop/graph/badge.svg?token=3ZmxTFfYra)](https://codecov.io/github/unicef/django-streaming)
[![Documentation](https://github.com/unicef/django-streaming/actions/workflows/docs.yml/badge.svg)](https://unicef.github.io/django-streaming/)
[![Pypi](https://badge.fury.io/py/django-streaming.svg)](https://badge.fury.io/py/django-streaming)


`django-streaming` is a robust and flexible Django application designed for efficiently streaming data from your
Django projects to various message brokers.

It provides a pluggable backend architecture supporting popular options like RabbitMQ, alongside console and
debug backends for development. With features like a threaded change manager for non-blocking asynchronous
notifications, configurable connection retry mechanisms, and graceful shutdown handling, django-streaming ensures
reliable data delivery. It seamlessly integrates with Django's ORM via signals to automatically stream model changes
and offers a command-line interface for easy interaction and debugging."
