## 0.8.0

- Publisher confirms are now enabled on the **listener channel** only, so a retry publication is
  confirmed by the broker before the original message is acknowledged. Regular `publish()` calls
  are not slowed down by confirms.
- Retry messages are published with `mandatory=True`, so an unroutable retry fails loudly instead
  of being silently dropped.
- The original message is acknowledged **only after** a retry has been successfully declared,
  published and confirmed. On failure it is nacked with `requeue=True` (or left unacked and
  redelivered by the broker if the channel is closed), preventing message loss.
- Messages that exceed `MAX_RETRIES` are now explicitly dropped with an error log.
- Added regression tests covering the retry acknowledgement behavior.


## 0.7.2
- `ChangeManager.register()` accepts an optional `fields` argument to control which model fields
  are included in the event payload.
- Added `get_serializable_fields()`, which excludes binary, file and image fields by default and
  prevents serialization errors on non-serializable field types.

## 0.7.1
- `make_event()` now accepts `value_type`, `key` and `message_id` as optional keyword arguments;
  `Event.build()` handles the new arguments and the `send` CLI command uses the new signature.

## 0.7
- Refactored the `listen` command around a configurable callback system (`callbacks.py`).
- `UserCallback` now returns a boolean to signal success; added `check_callback()` validation.
- Refined the callback exception hierarchy (`CallbackError`, `CallbackRetry`, `CallbackSkipAck`)
  for finer control over processing and acknowledgements.
- Documentation navigation reworked with `mkdocs-awesome-pages`; added an "About" page and a full
  configuration example.

## 0.6.0
- Introduced the `Event` class (`streaming.event.Event`) replacing the `EventType` TypedDict, with
  robust serialization via `Event.marshall()` / `Event.unmarshal()`.
- Events now carry a unique UUID message ID in the payload; `listen` displays it and `unmarshal`
  correctly restores timestamps.
- Improved RabbitMQ retry logic: a unique delay queue per routing key and back-off period, more
  informative retry logging, and centralized CLI logging configuration.
- `BaseBackend` now defines `configure_exchanges()` and `configure_queue_routing()` as abstract
  methods.
- Renamed `StreamingCallbackError` to `StreamingCallbackRetryError`.
- Documentation aligned with the codebase (project name/URLs, CLI commands, configuration keys).

## 0.5
- Refactored the backend architecture into a pluggable design (RabbitMQ, console and debug
  backends) and enhanced the CLI.
- Added multi-queue configuration and a listener to the RabbitMQ backend.
- Added `ThreadedChangeManager` (non-blocking, asynchronous notifications) and signal-based
  integration with the Django ORM.

## 0.4
- Added support for Django 3.2.

## 0.3
— Updated the RabbitMQ backend to handle manual acknowledgements.

## 0.2
— Documentation updates.

## 0.1
— Initial release with `ThreadedChangeManager` and tests.
