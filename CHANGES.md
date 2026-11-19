## 0.9.0

- Publisher confirms are now enabled on the **listener channel** only, so a retry publication is
  confirmed by the broker before the original message is acknowledged. Regular `publish()` calls
  are not slowed down by confirms.
- Retry messages are published with `mandatory=True`, so an unroutable retry fails loudly instead
  of being silently dropped.
- The original message is acknowledged **only after** a retry has been successfully declared,
  published and confirmed. On failure it is nacked with `requeue=True` (or left unacked and
  redelivered by the broker if the channel is closed), preventing message loss.
- Messages that exhaust their retries are now preserved in a durable `<queue>_exhausted` queue
  instead of being dropped: the original body and AMQP metadata are republished to the retry
  exchange, and the original delivery is acknowledged only after the copy is confirmed. If saving
  fails, the original is nacked with `requeue=True` (or left unacked) for redelivery.
- The maximum number of callback retries is now configurable via the `CALLBACK_RETRY_COUNT` setting
  (default: `5`; `0` sends the first failed attempt straight to the exhausted queue), replacing the
  hard-coded `MAX_RETRIES` constant. Exhausted messages are published without a per-message TTL.
- Documented the callback retry and exhausted-message flow, and added a `docs` tox environment for
  building the documentation.
- Added regression tests covering the retry acknowledgement and exhausted-message behavior.


## 0.8.0

- **New Features:** Allow specifying fields to be serialized in the event payload. It is now
  possible to specify which fields of a model should be included in the event payload, providing
  more flexibility and control over the data that is streamed. `ChangeManager.register()` accepts an
  optional `fields` argument, and `get_serializable_fields()` excludes binary, file and image fields
  by default to prevent serialization errors.
- **Refactor:** Decoupled the routing key from the event. The routing key is no longer tied to the
  event, and the default routing key generation has been improved, making event routing more
  flexible and easier to configure.

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
